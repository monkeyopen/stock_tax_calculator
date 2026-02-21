from datetime import datetime
from collections import deque
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field
import pandas as pd


@dataclass
class Lot:
    price: float
    qty: int
    fee: float
    updated_at: datetime
    true_price: float = field(init=False)
    
    def __post_init__(self):
        self.true_price = self.price + (self.fee / self.qty if self.qty > 0 else 0)


@dataclass
class TradeRecord:
    trade_type: str
    price: float
    qty: int
    fee: float
    updated_at: datetime
    shares: int = 1
    true_price: float = field(init=False)
    
    def __post_init__(self):
        if self.trade_type == "BUY":
            self.true_price = self.price * self.shares + (self.fee / self.qty if self.qty > 0 else 0)
        else:
            self.true_price = self.price * self.shares - (self.fee / self.qty if self.qty > 0 else 0)


class FIFOCostMethod:
    """
    先进先出法（First-In, First-Out, FIFO）
    
    原理：假设先买入的股票先卖出。在计算成本时，按照买入的先后顺序来确定成本。
    
    特点：
    - 在物价上涨时期，成本较低，利润较高
    - 期末存货成本接近于最近期的价格水平
    - 符合实际交易顺序，便于核算
    """
    
    def __init__(self, symbol: str, currency: str):
        self.symbol = symbol
        self.currency = currency
        self.buy_lots: deque[Lot] = deque()
        self.sell_lots: deque[Lot] = deque()
        self.bonus = 0.0
        self.bonus_by_year: Dict[int, float] = {}
        self.qty = 0
        
    def _add_bonus(self, realized: float, updated_at):
        if realized == 0:
            return
        
        self.bonus += realized
        
        if isinstance(updated_at, (datetime, pd.Timestamp)):
            dt = updated_at
        else:
            try:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")
        
        year = dt.year
        self.bonus_by_year[year] = self.bonus_by_year.get(year, 0.0) + realized
    
    def buy(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        true_price = price * shares + (fee / qty if qty > 0 else 0)
        
        if self.qty < 0:
            cover = min(qty, abs(self.qty))
            remaining = cover
            
            while remaining > 0 and self.sell_lots:
                sell_lot = self.sell_lots[0]
                lot_cover = min(remaining, sell_lot.qty)
                
                realized = lot_cover * (sell_lot.true_price - true_price)
                self._add_bonus(realized, updated_at)
                
                sell_lot.qty -= lot_cover
                remaining -= lot_cover
                
                if sell_lot.qty <= 0:
                    self.sell_lots.popleft()
            
            self.qty += cover
            qty -= cover
        
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.buy_lots.append(lot)
            self.qty += qty
    
    def sell(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        true_price = price * shares - (fee / qty if qty > 0 else 0)
        
        if self.qty > 0:
            close = min(qty, self.qty)
            remaining = close
            
            while remaining > 0 and self.buy_lots:
                lot = self.buy_lots[0]
                lot_close = min(remaining, lot.qty)
                
                realized = lot_close * (true_price - lot.true_price)
                self._add_bonus(realized, updated_at)
                
                lot.qty -= lot_close
                remaining -= lot_close
                
                if lot.qty <= 0:
                    self.buy_lots.popleft()
            
            self.qty -= close
            qty -= close
        
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.sell_lots.append(lot)
            self.qty -= qty
    
    def add_fee(self, fee: float, updated_at):
        if fee <= 0:
            return
        self._add_bonus(-fee, updated_at)
    
    def expire_option(self, expiry_date, updated_at):
        if self.qty == 0:
            return
        
        if self.qty > 0:
            remaining = self.qty
            while remaining > 0 and self.buy_lots:
                lot = self.buy_lots[0]
                unrealized_loss = -lot.true_price * min(remaining, lot.qty)
                self._add_bonus(unrealized_loss, updated_at)
                remaining -= lot.qty
                self.buy_lots.popleft()
        else:
            remaining = abs(self.qty)
            while remaining > 0 and self.sell_lots:
                lot = self.sell_lots[0]
                unrealized_loss = lot.true_price * min(remaining, lot.qty)
                self._add_bonus(unrealized_loss, updated_at)
                remaining -= lot.qty
                self.sell_lots.popleft()
        
        self.buy_lots.clear()
        self.sell_lots.clear()
        self.qty = 0


class LIFOCostMethod:
    """
    后进先出法（Last-In, First-Out, LIFO）
    
    原理：假设后买入的股票先卖出。在计算成本时，后买入的股票先被卖出。
    
    特点：
    - 在物价上涨时期，成本较高，利润较低，可减少税负
    - 更接近当前成本水平
    - 不符合实际交易顺序，核算较复杂
    """
    
    def __init__(self, symbol: str, currency: str):
        self.symbol = symbol
        self.currency = currency
        self.buy_lots: List[Lot] = []
        self.sell_lots: List[Lot] = []
        self.bonus = 0.0
        self.bonus_by_year: Dict[int, float] = {}
        self.qty = 0
    
    def _add_bonus(self, realized: float, updated_at):
        if realized == 0:
            return
        
        self.bonus += realized
        
        if isinstance(updated_at, (datetime, pd.Timestamp)):
            dt = updated_at
        else:
            try:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")
        
        year = dt.year
        self.bonus_by_year[year] = self.bonus_by_year.get(year, 0.0) + realized
    
    def buy(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        true_price = price * shares + (fee / qty if qty > 0 else 0)
        
        if self.qty < 0:
            cover = min(qty, abs(self.qty))
            remaining = cover
            
            while remaining > 0 and self.sell_lots:
                sell_lot = self.sell_lots[-1]
                lot_cover = min(remaining, sell_lot.qty)
                
                realized = lot_cover * (sell_lot.true_price - true_price)
                self._add_bonus(realized, updated_at)
                
                sell_lot.qty -= lot_cover
                remaining -= lot_cover
                
                if sell_lot.qty <= 0:
                    self.sell_lots.pop()
            
            self.qty += cover
            qty -= cover
        
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.buy_lots.append(lot)
            self.qty += qty
    
    def sell(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        true_price = price * shares - (fee / qty if qty > 0 else 0)
        
        if self.qty > 0:
            close = min(qty, self.qty)
            remaining = close
            
            while remaining > 0 and self.buy_lots:
                lot = self.buy_lots[-1]
                lot_close = min(remaining, lot.qty)
                
                realized = lot_close * (true_price - lot.true_price)
                self._add_bonus(realized, updated_at)
                
                lot.qty -= lot_close
                remaining -= lot_close
                
                if lot.qty <= 0:
                    self.buy_lots.pop()
            
            self.qty -= close
            qty -= close
        
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.sell_lots.append(lot)
            self.qty -= qty
    
    def add_fee(self, fee: float, updated_at):
        if fee <= 0:
            return
        self._add_bonus(-fee, updated_at)
    
    def expire_option(self, expiry_date, updated_at):
        if self.qty == 0:
            return
        
        if self.qty > 0:
            remaining = self.qty
            while remaining > 0 and self.buy_lots:
                lot = self.buy_lots[-1]
                unrealized_loss = -lot.true_price * min(remaining, lot.qty)
                self._add_bonus(unrealized_loss, updated_at)
                remaining -= lot.qty
                self.buy_lots.pop()
        else:
            remaining = abs(self.qty)
            while remaining > 0 and self.sell_lots:
                lot = self.sell_lots[-1]
                unrealized_loss = lot.true_price * min(remaining, lot.qty)
                self._add_bonus(unrealized_loss, updated_at)
                remaining -= lot.qty
                self.sell_lots.pop()
        
        self.buy_lots.clear()
        self.sell_lots.clear()
        self.qty = 0


class HIFOCoastMethod:
    """
    最高成本优先法（Highest In, First Out, HIFO）
    
    原理：假设成本最高的股票先卖出。
    
    特点：
    - 在物价上涨时期，可以最大化成本，最小化利润
    - 可用于税务筹划，减少当期税负
    """
    
    def __init__(self, symbol: str, currency: str):
        self.symbol = symbol
        self.currency = currency
        self.buy_lots: List[Lot] = []
        self.sell_lots: List[Lot] = []
        self.bonus = 0.0
        self.bonus_by_year: Dict[int, float] = {}
        self.qty = 0
    
    def _add_bonus(self, realized: float, updated_at):
        if realized == 0:
            return
        
        self.bonus += realized
        
        if isinstance(updated_at, (datetime, pd.Timestamp)):
            dt = updated_at
        else:
            try:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")
        
        year = dt.year
        self.bonus_by_year[year] = self.bonus_by_year.get(year, 0.0) + realized
    
    def _get_highest_cost_lot(self) -> Optional[int]:
        if not self.buy_lots:
            return None
        
        max_idx = 0
        max_price = self.buy_lots[0].true_price
        
        for i, lot in enumerate(self.buy_lots):
            if lot.true_price > max_price:
                max_price = lot.true_price
                max_idx = i
        
        return max_idx
    
    def buy(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        true_price = price * shares + (fee / qty if qty > 0 else 0)
        
        if self.qty < 0:
            cover = min(qty, abs(self.qty))
            remaining = cover
            
            while remaining > 0 and self.sell_lots:
                sell_lot = self.sell_lots[-1]
                lot_cover = min(remaining, sell_lot.qty)
                
                realized = lot_cover * (sell_lot.true_price - true_price)
                self._add_bonus(realized, updated_at)
                
                sell_lot.qty -= lot_cover
                remaining -= lot_cover
                
                if sell_lot.qty <= 0:
                    self.sell_lots.pop()
            
            self.qty += cover
            qty -= cover
        
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.buy_lots.append(lot)
            self.qty += qty
    
    def sell(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        true_price = price * shares - (fee / qty if qty > 0 else 0)
        
        if self.qty > 0:
            close = min(qty, self.qty)
            remaining = close
            
            while remaining > 0 and self.buy_lots:
                idx = self._get_highest_cost_lot()
                if idx is None:
                    break
                
                lot = self.buy_lots[idx]
                lot_close = min(remaining, lot.qty)
                
                realized = lot_close * (true_price - lot.true_price)
                self._add_bonus(realized, updated_at)
                
                lot.qty -= lot_close
                remaining -= lot_close
                
                if lot.qty <= 0:
                    self.buy_lots.pop(idx)
            
            self.qty -= close
            qty -= close
        
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.sell_lots.append(lot)
            self.qty -= qty
    
    def add_fee(self, fee: float, updated_at):
        if fee <= 0:
            return
        self._add_bonus(-fee, updated_at)
    
    def expire_option(self, expiry_date, updated_at):
        if self.qty == 0:
            return
        
        for lot in self.buy_lots:
            unrealized_loss = -lot.true_price * lot.qty
            self._add_bonus(unrealized_loss, updated_at)
        
        self.buy_lots.clear()
        self.sell_lots.clear()
        self.qty = 0


class LOFOCostMethod:
    """
    最低成本优先法（Lowest In, First Out, LOFO）
    
    原理：假设成本最低的股票先卖出。
    
    特点：
    - 在物价上涨时期，成本较低，利润较高
    - 与HIFO相反，可能增加税负
    """
    
    def __init__(self, symbol: str, currency: str):
        self.symbol = symbol
        self.currency = currency
        self.buy_lots: List[Lot] = []
        self.sell_lots: List[Lot] = []
        self.bonus = 0.0
        self.bonus_by_year: Dict[int, float] = {}
        self.qty = 0
    
    def _add_bonus(self, realized: float, updated_at):
        if realized == 0:
            return
        
        self.bonus += realized
        
        if isinstance(updated_at, (datetime, pd.Timestamp)):
            dt = updated_at
        else:
            try:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")
        
        year = dt.year
        self.bonus_by_year[year] = self.bonus_by_year.get(year, 0.0) + realized
    
    def _get_lowest_cost_lot(self) -> Optional[int]:
        if not self.buy_lots:
            return None
        
        min_idx = 0
        min_price = self.buy_lots[0].true_price
        
        for i, lot in enumerate(self.buy_lots):
            if lot.true_price < min_price:
                min_price = lot.true_price
                min_idx = i
        
        return min_idx
    
    def buy(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        true_price = price * shares + (fee / qty if qty > 0 else 0)
        
        if self.qty < 0:
            cover = min(qty, abs(self.qty))
            remaining = cover
            
            while remaining > 0 and self.sell_lots:
                sell_lot = self.sell_lots[-1]
                lot_cover = min(remaining, sell_lot.qty)
                
                realized = lot_cover * (sell_lot.true_price - true_price)
                self._add_bonus(realized, updated_at)
                
                sell_lot.qty -= lot_cover
                remaining -= lot_cover
                
                if sell_lot.qty <= 0:
                    self.sell_lots.pop()
            
            self.qty += cover
            qty -= cover
        
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.buy_lots.append(lot)
            self.qty += qty
    
    def sell(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        true_price = price * shares - (fee / qty if qty > 0 else 0)
        
        if self.qty > 0:
            close = min(qty, self.qty)
            remaining = close
            
            while remaining > 0 and self.buy_lots:
                idx = self._get_lowest_cost_lot()
                if idx is None:
                    break
                
                lot = self.buy_lots[idx]
                lot_close = min(remaining, lot.qty)
                
                realized = lot_close * (true_price - lot.true_price)
                self._add_bonus(realized, updated_at)
                
                lot.qty -= lot_close
                remaining -= lot_close
                
                if lot.qty <= 0:
                    self.buy_lots.pop(idx)
            
            self.qty -= close
            qty -= close
        
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.sell_lots.append(lot)
            self.qty -= qty
    
    def add_fee(self, fee: float, updated_at):
        if fee <= 0:
            return
        self._add_bonus(-fee, updated_at)
    
    def expire_option(self, expiry_date, updated_at):
        if self.qty == 0:
            return
        
        for lot in self.buy_lots:
            unrealized_loss = -lot.true_price * lot.qty
            self._add_bonus(unrealized_loss, updated_at)
        
        self.buy_lots.clear()
        self.sell_lots.clear()
        self.qty = 0


COST_METHODS = {
    "FIFO": FIFOCostMethod,
    "LIFO": LIFOCostMethod,
    "HIFO": HIFOCoastMethod,
    "LOFO": LOFOCostMethod,
}


def create_stock(symbol: str, currency: str, method: str = "FIFO"):
    """
    创建指定成本核算方法的股票对象
    
    Args:
        symbol: 股票代码
        currency: 货币
        method: 成本核算方法，可选值: FIFO, LIFO, HIFO, LOFO
        
    Returns:
        对应方法的股票对象
    """
    if method.upper() not in COST_METHODS:
        raise ValueError(f"不支持的成本核算方法: {method}，可选值: {list(COST_METHODS.keys())}")
    
    return COST_METHODS[method.upper()](symbol, currency)
