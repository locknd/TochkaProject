from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from database import User as UserDB, Instrument as InstrumentDB, Balance as BalanceDB, Order as OrderDB, Transaction as TransactionDB
from schemas import *
import uuid
import time
import random
from datetime import datetime
from typing import List, Dict, Optional, Union
import threading
import asyncio

balance_update_lock = asyncio.Lock()

class TradingEngine:
    """Движок биржевой торговли"""
    
    def __init__(self, db: Session):
        self.db = db
    
    async def create_order(self, user: UserDB, order_data: Union[LimitOrderBody, MarketOrderBody]) -> str:
        """Создать заявку"""
        # Проверяем существование инструмента
        instrument = self.db.query(InstrumentDB).filter(InstrumentDB.ticker == order_data.ticker).first()
        if not instrument:
            raise ValueError("Инструмент не найден")
        # Проверка баланса перед созданием ордера
        if order_data.direction == "BUY":
            # Для покупки нужен RUB
            total_cost = order_data.qty * (getattr(order_data, 'price', 0) or 1)
            rub_balance = self.db.query(BalanceDB).filter(BalanceDB.user_id == user.id, BalanceDB.ticker == "RUB").first()
            if not rub_balance or rub_balance.amount < total_cost:
                raise ValueError("Недостаточно средств для покупки")
        elif order_data.direction == "SELL":
            # Для продажи нужен актив
            asset_balance = self.db.query(BalanceDB).filter(BalanceDB.user_id == user.id, BalanceDB.ticker == order_data.ticker).first()
            if not asset_balance or asset_balance.amount < order_data.qty:
                raise ValueError("Недостаточно актива для продажи")
        order_id = str(uuid.uuid4())
        order_type = "LIMIT" if isinstance(order_data, LimitOrderBody) else "MARKET"
        
        # Создаем заявку
        order = OrderDB(
            id=order_id,
            user_id=user.id,
            ticker=order_data.ticker,
            direction=order_data.direction,
            qty=order_data.qty,
            price=getattr(order_data, 'price', None),
            order_type=order_type,
            status="NEW",
            filled=0
        )
        
        self.db.add(order)
        
        # Для рыночных заявок пытаемся исполнить немедленно
        if order_type == "MARKET":
            await self._execute_market_order(order)
        else:
            await self._try_execute_limit_order(order)
        
        self.db.commit()
        return order_id
    
    async def _execute_market_order(self, order: OrderDB):
        """Исполнить рыночную заявку"""
        # Находим лучшие противоположные заявки
        opposite_direction = "SELL" if order.direction == "BUY" else "BUY"
        opposite_orders = self.db.query(OrderDB).filter(
            OrderDB.ticker == order.ticker,
            OrderDB.direction == opposite_direction,
            OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
            OrderDB.order_type == "LIMIT"
        ).order_by(
            OrderDB.price.asc() if order.direction == "BUY" else OrderDB.price.desc()
        ).all()
        
        remaining_qty = order.qty
        
        for opposite_order in opposite_orders:
            if remaining_qty <= 0:
                break
            
            # Определяем количество для исполнения
            available_qty = opposite_order.qty - opposite_order.filled
            execute_qty = min(remaining_qty, available_qty)
            
            if execute_qty > 0:
                # Создаем транзакцию
                transaction = TransactionDB(
                    ticker=order.ticker,
                    amount=execute_qty,
                    price=opposite_order.price,
                    buyer_id=order.user_id if order.direction == "BUY" else opposite_order.user_id,
                    seller_id=order.user_id if order.direction == "SELL" else opposite_order.user_id
                )
                self.db.add(transaction)
                
                # Обновляем заявки
                order.filled += execute_qty
                opposite_order.filled += execute_qty
                remaining_qty -= execute_qty
                
                # Обновляем статусы
                if opposite_order.filled >= opposite_order.qty:
                    opposite_order.status = "EXECUTED"
                else:
                    opposite_order.status = "PARTIALLY_EXECUTED"
                
                # Обновляем балансы
                await self._update_balances_after_trade(order, opposite_order, execute_qty, opposite_order.price)
        
        # Обновляем статус рыночной заявки
        if order.filled >= order.qty:
            order.status = "EXECUTED"
        elif order.filled > 0:
            order.status = "PARTIALLY_EXECUTED"
        else:
            # Рыночная заявка не исполнена - отменяем
            order.status = "CANCELLED"
    
    async def _try_execute_limit_order(self, order: OrderDB):
        """Попытаться исполнить лимитную заявку"""
        # Аналогично рыночной заявке, но с проверкой цены
        opposite_direction = "SELL" if order.direction == "BUY" else "BUY"
        if order.direction == "BUY":
            # Покупка: ищем заявки на продажу с ценой <= нашей цены
            opposite_orders = self.db.query(OrderDB).filter(
                OrderDB.ticker == order.ticker,
                OrderDB.direction == opposite_direction,
                OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
                OrderDB.price <= order.price
            ).order_by(OrderDB.price.asc()).all()        
        else:
            # Продажа: ищем заявки на покупку с ценой >= нашей цены
            opposite_orders = self.db.query(OrderDB).filter(
                OrderDB.ticker == order.ticker,
                OrderDB.direction == opposite_direction,
                OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
                OrderDB.price >= order.price
            ).order_by(OrderDB.price.desc()).all()
        
        remaining_qty = order.qty
        
        for opposite_order in opposite_orders:
            if remaining_qty <= 0:
                break
            
            available_qty = opposite_order.qty - opposite_order.filled
            execute_qty = min(remaining_qty, available_qty)
            
            if execute_qty > 0:
                # Цена исполнения - цена заявки, которая была в стакане первой
                execution_price = opposite_order.price
                
                transaction = TransactionDB(
                    ticker=order.ticker,
                    amount=execute_qty,
                    price=execution_price,
                    buyer_id=order.user_id if order.direction == "BUY" else opposite_order.user_id,
                    seller_id=order.user_id if order.direction == "SELL" else opposite_order.user_id
                )
                self.db.add(transaction)
                order.filled += execute_qty
                opposite_order.filled += execute_qty
                remaining_qty -= execute_qty
                
                if opposite_order.filled >= opposite_order.qty:
                    opposite_order.status = "EXECUTED"
                else:
                    opposite_order.status = "PARTIALLY_EXECUTED"
                
                await self._update_balances_after_trade(order, opposite_order, execute_qty, execution_price)
        
        # Обновляем статус нашей заявки
        if order.filled >= order.qty:
            order.status = "EXECUTED"
        elif order.filled > 0:
            order.status = "PARTIALLY_EXECUTED"
        # Иначе остается NEW
    
    async def _update_balances_after_trade(self, order1: OrderDB, order2: OrderDB, qty: int, price: int):
        """Обновить балансы после сделки"""
        buyer_id = order1.user_id if order1.direction == "BUY" else order2.user_id
        seller_id = order1.user_id if order1.direction == "SELL" else order2.user_id
        ticker = order1.ticker
        total_cost = qty * price
        
        # Создаем словарь для отслеживания изменений балансов
        balance_changes = {}
        
        # Функция для безопасного изменения баланса
        def update_balance(user_id: str, ticker: str, amount_change: int):
            key = (user_id, ticker)
            if key not in balance_changes:
                balance_changes[key] = 0
            balance_changes[key] += amount_change
        
        # Записываем все изменения
        update_balance(buyer_id, ticker, qty)       # Покупатель получает актив
        update_balance(buyer_id, "RUB", -total_cost) # Покупатель теряет RUB
        update_balance(seller_id, ticker, -qty)     # Продавец теряет актив
        update_balance(seller_id, "RUB", total_cost) # Продавец получает RUB
        
        # Сортируем изменения по ключам для избежания deadlocks
        sorted_changes = sorted(balance_changes.items(), key=lambda x: (str(x[0][0]), x[0][1]))
        
        # Критическая секция: обновление балансов
        async with balance_update_lock:
            # Применяем изменения к базе данных с retry логикой
            for (user_id, ticker), amount_change in sorted_changes:
                if amount_change == 0:
                    continue
                
                await self._upsert_balance_with_retry(user_id, ticker, amount_change)
    
    async def _upsert_balance_with_retry(self, user_id: str, ticker: str, amount_change: int, max_retries: int = 3):
        """Безопасное обновление баланса с поддержкой upsert и retry для deadlocks"""
        for attempt in range(max_retries):
            try:
                # Используем PostgreSQL ON CONFLICT для атомарного upsert
                upsert_sql = text("""
                    INSERT INTO balances (user_id, ticker, amount, updated_at)
                    VALUES (:user_id, :ticker, :amount, :updated_at)
                    ON CONFLICT (user_id, ticker)
                    DO UPDATE SET 
                        amount = balances.amount + :amount,
                        updated_at = :updated_at
                """)
                
                self.db.execute(upsert_sql, {
                    'user_id': user_id,
                    'ticker': ticker,
                    'amount': amount_change,
                    'updated_at': datetime.utcnow()
                })
                break  # Успешно выполнено, выходим из цикла
                
            except OperationalError as e:
                if "deadlock detected" in str(e).lower() and attempt < max_retries - 1:
                    # Deadlock обнаружен, ждем случайное время и повторяем
                    wait_time = random.uniform(0.01, 0.1) * (2 ** attempt)  # Exponential backoff
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    # Последняя попытка или другая ошибка - поднимаем исключение
                    raise
    
    def cancel_order(self, order_id: str, user: UserDB) -> bool:
        """Отменить заявку"""
        order = self.db.query(OrderDB).filter(
            OrderDB.id == order_id,
            OrderDB.user_id == user.id,
            OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"])
        ).first()
        
        if not order:
            return False
        
        order.status = "CANCELLED"
        self.db.commit()
        return True
    
    def get_orderbook(self, ticker: str, limit: int = 10) -> L2OrderBook:
        """Получить стакан заявок"""
        # Заявки на покупку (bids) - сортируем по убыванию цены
        bids = self.db.query(OrderDB).filter(
            OrderDB.ticker == ticker,
            OrderDB.direction == "BUY",
            OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
            OrderDB.order_type == "LIMIT"
        ).order_by(OrderDB.price.desc()).all()
        # Заявки на продажу (asks) - сортируем по возрастанию цены
        asks = self.db.query(OrderDB).filter(
            OrderDB.ticker == ticker,
            OrderDB.direction == "SELL",
            OrderDB.status.in_(["NEW", "PARTIALLY_EXECUTED"]),
            OrderDB.order_type == "LIMIT"
        ).order_by(OrderDB.price.asc()).all()
        # Группируем по ценам только неисполненные остатки
        bid_levels = {}
        for bid in bids:
            qty = bid.qty - bid.filled
            if qty <= 0:
                continue
            price = bid.price
            if price in bid_levels:
                bid_levels[price] += qty
            else:
                bid_levels[price] = qty
        ask_levels = {}
        for ask in asks:
            qty = ask.qty - ask.filled
            if qty <= 0:
                continue
            price = ask.price
            if price in ask_levels:
                ask_levels[price] += qty
            else:
                ask_levels[price] = qty
        # Сортировка уровней стакана
        bid_levels_sorted = sorted(bid_levels.items(), key=lambda x: -x[0])
        ask_levels_sorted = sorted(ask_levels.items(), key=lambda x: x[0])
        return L2OrderBook(
            bid_levels=[Level(price=price, qty=qty) for price, qty in bid_levels_sorted[:limit]],
            ask_levels=[Level(price=price, qty=qty) for price, qty in ask_levels_sorted[:limit]]
        )
