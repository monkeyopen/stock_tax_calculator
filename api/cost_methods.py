#!/usr/bin/env python3
"""
股票成本核算方法模块

本模块实现了四种常用的股票成本核算方法：
1. FIFO (First-In, First-Out) - 先进先出法
2. LIFO (Last-In, First-Out) - 后进先出法  
3. HIFO (Highest In, First-Out) - 最高成本优先法
4. LOFO (Lowest In, First-Out) - 最低成本优先法

每种方法都支持：
- 普通股票交易
- 期权交易（通过shares参数设置合约乘数）
- 卖空交易（做空）
- 按年度统计盈亏（用于税务申报）

核心概念：
- Lot: 批次，记录一次买入/卖出的股票信息
- true_price: 真实成本价 = 成交价 + 手续费分摊
- shares: 合约乘数（股票=1，期权=100）
- qty: 交易数量（股数或合约数）
"""

from datetime import datetime
from collections import deque
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field
import pandas as pd


# =============================================================================
# 数据类定义
# =============================================================================

@dataclass
class Lot:
    """
    股票批次类 - 记录一次买入的股票信息
    
    在FIFO/LIFO等方法中，每次买入都会创建一个Lot对象，
    记录买入价格、数量、手续费和时间等信息。
    
    Attributes:
        price: 成交价格（每股/每合约的价格）
        qty: 数量（股数或合约数）
        fee: 手续费（总费用）
        updated_at: 交易时间
        true_price: 真实成本价（自动计算，包含手续费分摊）
    
    Example:
        >>> lot = Lot(price=100.0, qty=100, fee=5.0, updated_at=datetime.now())
        >>> lot.true_price  # 100.0 + 5.0/100 = 100.05
    """
    price: float
    qty: int
    fee: float
    updated_at: datetime
    true_price: float = field(init=False)  # 自动计算，不需要在构造函数中传入
    
    def __post_init__(self):
        """
        初始化后自动计算真实成本价
        
        真实成本价 = 成交价 + 每股手续费
        每股手续费 = 总手续费 / 数量
        
        注意：如果手续费为None或NaN，则视为0
        """
        # 处理可能为None或NaN的手续费
        safe_fee = self.fee if (self.fee is not None and pd.notna(self.fee)) else 0.0
        # 计算真实成本价：成交价 + 手续费分摊到每股
        self.true_price = self.price + (safe_fee / self.qty if self.qty > 0 else 0)


@dataclass
class TradeRecord:
    """
    交易记录类 - 用于记录单次交易的详细信息
    
    与Lot的区别：
    - Lot用于内部持仓管理（记录买入的批次）
    - TradeRecord用于记录完整的交易历史（买入和卖出）
    
    Attributes:
        trade_type: 交易类型，"BUY"或"SELL"
        price: 成交价格
        qty: 交易数量
        fee: 手续费
        updated_at: 交易时间
        shares: 合约乘数（股票=1，期权=100）
        true_price: 真实价格（自动计算）
    """
    trade_type: str
    price: float
    qty: int
    fee: float
    updated_at: datetime
    shares: int = 1  # 默认是股票，乘数为1
    true_price: float = field(init=False)
    
    def __post_init__(self):
        """
        初始化后自动计算真实价格
        
        买入时：真实成本 = 成交价×乘数 + 手续费分摊
        卖出时：真实收入 = 成交价×乘数 - 手续费分摊
        
        为什么买入加手续费，卖出减手续费？
        - 买入：手续费增加了你的成本
        - 卖出：手续费减少了你的收入
        """
        safe_fee = self.fee if (self.fee is not None and pd.notna(self.fee)) else 0.0
        if self.trade_type == "BUY":
            # 买入：成本 = 价格×乘数 + 手续费分摊
            self.true_price = self.price * self.shares + (safe_fee / self.qty if self.qty > 0 else 0)
        else:
            # 卖出：收入 = 价格×乘数 - 手续费分摊
            self.true_price = self.price * self.shares - (safe_fee / self.qty if self.qty > 0 else 0)


# =============================================================================
# FIFO 先进先出法
# =============================================================================

class FIFOCostMethod:
    """
    先进先出法（First-In, First-Out, FIFO）
    
    【核心原理】
    假设先买入的股票先卖出。在计算成本时，按照买入的先后顺序来确定成本。
    
    【特点】
    1. 在物价上涨时期，成本较低，利润较高
    2. 期末存货成本接近于最近期的价格水平
    3. 符合实际交易顺序，便于核算
    4. 大多数国家和地区的税务默认方法
    
    【数据结构】
    使用双端队列(deque)存储买入批次：
    - 新买入的批次添加到队列尾部 (append)
    - 卖出时从队列头部取出 (popleft)
    
    【卖空支持】
    当 qty < 0 时，表示当前处于做空状态：
    - sell_lots 队列记录做空的批次
    - 买入时会优先平掉空头仓位
    
    Example:
        >>> stock = FIFOCostMethod("AAPL", "USD")
        >>> stock.buy(price=100, qty=100, fee=5, updated_at="2024-01-01")
        >>> stock.buy(price=110, qty=100, fee=5, updated_at="2024-02-01")
        >>> stock.sell(price=120, qty=150, fee=7, updated_at="2024-03-01")
        >>> # 先卖100股（成本100），再卖50股（成本110）
        >>> print(stock.bonus)  # 已实现盈亏
    """
    
    def __init__(self, symbol: str, currency: str):
        """
        初始化FIFO成本核算对象
        
        Args:
            symbol: 股票代码，如 "AAPL"、"HK.00700"
            currency: 货币代码，如 "USD"、"HKD"
        """
        self.symbol = symbol          # 股票代码
        self.currency = currency      # 货币
        self.buy_lots: deque[Lot] = deque()   # 买入批次队列（FIFO核心）
        self.sell_lots: deque[Lot] = deque()  # 做空批次队列（处理卖空）
        self.bonus = 0.0              # 累计已实现盈亏
        self.bonus_by_year: Dict[int, float] = {}  # 按年份统计的盈亏 {year: amount}
        self.qty = 0                  # 当前持仓数量（正数=多头，负数=空头，0=空仓）
        
    def _add_bonus(self, realized: float, updated_at):
        """
        记录已实现盈亏，并按年份分类统计
        
        这个方法会被 buy() 和 sell() 方法调用，每当有盈亏实现时记录。
        
        Args:
            realized: 本次实现的盈亏金额（正数=盈利，负数=亏损）
            updated_at: 交易时间，用于确定盈亏所属年份
        
        Note:
            - 如果 realized 为0，则不记录
            - updated_at 支持 datetime、pd.Timestamp 或字符串格式
            - 字符串格式支持："2024-01-01 10:00:00" 或 "2024-01-01 10:00:00.000000"
        """
        if realized == 0:
            return
        
        # 累加到总盈亏
        self.bonus += realized
        
        # 解析时间，获取年份
        if isinstance(updated_at, (datetime, pd.Timestamp)):
            dt = updated_at
        else:
            # 尝试解析字符串格式
            try:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")
        
        year = dt.year
        # 按年份累加盈亏
        self.bonus_by_year[year] = self.bonus_by_year.get(year, 0.0) + realized
    
    def buy(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        """
        买入股票/期权
        
        【处理流程】
        1. 计算真实成本价 = price×shares + 手续费分摊
        2. 如果当前是做空状态(qty < 0)，先平掉部分或全部空头
        3. 剩余的买入量创建新的Lot，添加到buy_lots队列尾部
        
        【空头回补逻辑】
        当 qty < 0 时（做空状态），买入会触发空头回补：
        - 从 sell_lots 队列头部取批次（最早的做空）
        - 计算盈亏：做空价 - 买入价
        - 盈亏 = 数量 × (做空真实成本 - 买入真实成本)
        
        Args:
            price: 成交价格（每股/每合约的价格）
            qty: 买入数量（股数或合约数）
            fee: 手续费（总费用）
            updated_at: 交易时间
            shares: 合约乘数（股票=1，期权=100），默认1
        
        Example:
            >>> stock = FIFOCostMethod("AAPL", "USD")
            >>> # 普通股票买入100股，每股$150，手续费$5
            >>> stock.buy(price=150, qty=100, fee=5, updated_at="2024-01-01")
            >>> 
            >>> # 期权买入1张合约（对应100股），每张$5，手续费$1
            >>> stock.buy(price=5, qty=1, fee=1, updated_at="2024-01-01", shares=100)
        """
        # 计算真实成本价：成交价×乘数 + 手续费分摊
        safe_fee = fee if (fee is not None and pd.notna(fee)) else 0.0
        true_price = price * shares + (safe_fee / qty if qty > 0 else 0)
        
        # ========== 步骤1：处理空头回补 ==========
        # 如果当前是做空状态(qty < 0)，买入会平掉空头仓位
        if self.qty < 0:
            # 计算本次买入能平掉多少空头
            # cover = min(买入量, 空头持仓量)
            cover = min(qty, abs(self.qty))
            remaining = cover  # 还需要平掉的数量
            
            # 从sell_lots队列头部开始平（FIFO原则，最早的做空先平）
            while remaining > 0 and self.sell_lots:
                sell_lot = self.sell_lots[0]  # 取最早的做空批次
                lot_cover = min(remaining, sell_lot.qty)  # 本次平掉的数量
                
                # 计算盈亏：做空时高价卖出，现在低价买回
                # 盈亏 = 数量 × (做空成本 - 买入成本)
                realized = lot_cover * (sell_lot.true_price - true_price)
                self._add_bonus(realized, updated_at)
                
                # 减少该做空批次的数量
                sell_lot.qty -= lot_cover
                remaining -= lot_cover
                
                # 如果该批次完全平仓，从队列中移除
                if sell_lot.qty <= 0:
                    self.sell_lots.popleft()
            
            # 更新持仓状态
            self.qty += cover  # 空头减少
            qty -= cover       # 剩余的买入量
        
        # ========== 步骤2：处理正常买入 ==========
        # 如果还有剩余买入量，创建新的Lot
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price  # 使用已计算的真实成本价
            self.buy_lots.append(lot)    # 添加到队列尾部
            self.qty += qty              # 增加多头持仓
    
    def sell(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        """
        卖出股票/期权 - 【FIFO核心逻辑】
        
        【处理流程】
        1. 计算真实卖出价 = price×shares - 手续费分摊
        2. 如果当前是多头状态(qty > 0)，先平掉部分或全部多头（FIFO原则）
        3. 如果还有剩余卖出量，创建做空仓位
        
        【FIFO核心逻辑】
        当 qty > 0 时（多头状态），卖出会触发平仓：
        - 从 buy_lots 队列头部取批次（最早买入的先卖出）
        - 计算盈亏：卖出价 - 买入价
        - 盈亏 = 数量 × (卖出真实价格 - 买入真实成本)
        
        Args:
            price: 成交价格（每股/每合约的价格）
            qty: 卖出数量（股数或合约数）
            fee: 手续费（总费用）
            updated_at: 交易时间
            shares: 合约乘数（股票=1，期权=100），默认1
        
        Example:
            >>> stock = FIFOCostMethod("AAPL", "USD")
            >>> # 先买入两批
            >>> stock.buy(price=100, qty=100, fee=5, updated_at="2024-01-01")
            >>> stock.buy(price=110, qty=100, fee=5, updated_at="2024-02-01")
            >>> 
            >>> # 卖出150股 - 先卖第一批100股，再卖第二批50股
            >>> stock.sell(price=120, qty=150, fee=7, updated_at="2024-03-01")
            >>> 
            >>> print(stock.bonus)  # 显示已实现盈亏
        """
        # 计算真实卖出价：成交价×乘数 - 手续费分摊
        # 注意：卖出时手续费减少收入，所以是减号
        safe_fee = fee if (fee is not None and pd.notna(fee)) else 0.0
        true_price = price * shares - (safe_fee / qty if qty > 0 else 0)
        
        # ========== 步骤1：处理多头平仓（FIFO核心）==========
        if self.qty > 0:
            # 计算本次卖出能平掉多少多头
            close = min(qty, self.qty)
            remaining = close
            
            # 从buy_lots队列头部开始平（FIFO：最早买入的先卖出）
            while remaining > 0 and self.buy_lots:
                lot = self.buy_lots[0]  # 取最早买入的批次
                lot_close = min(remaining, lot.qty)  # 本次卖出的数量
                
                # 计算盈亏：卖出价 - 买入价
                # 盈亏 = 数量 × (卖出真实价格 - 买入真实成本)
                realized = lot_close * (true_price - lot.true_price)
                self._add_bonus(realized, updated_at)
                
                # 减少该买入批次的数量
                lot.qty -= lot_close
                remaining -= lot_close
                
                # 如果该批次完全卖完，从队列中移除
                if lot.qty <= 0:
                    self.buy_lots.popleft()
            
            # 更新持仓状态
            self.qty -= close  # 多头减少
            qty -= close       # 剩余的卖出量
        
        # ========== 步骤2：处理做空 ==========
        # 如果还有剩余卖出量，创建做空仓位
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.sell_lots.append(lot)  # 添加到做空队列
            self.qty -= qty             # 减少持仓（变为负数或更负）
    
    def add_fee(self, fee: float, updated_at):
        """
        添加额外费用（如股息税、融资利息等）
        
        这些费用会被记录为负收益（亏损）。
        
        Args:
            fee: 费用金额（必须为正数）
            updated_at: 费用发生时间
        """
        if fee <= 0:
            return
        # 费用视为负收益
        self._add_bonus(-fee, updated_at)
    
    def expire_option(self, expiry_date, updated_at):
        """
        处理期权到期失效
        
        当期权到期时，如果还有持仓，视为全部亏损。
        未实现亏损 = -持仓成本 × 数量
        
        Args:
            expiry_date: 期权到期日
            updated_at: 失效记录时间
        """
        if self.qty == 0:
            return
        
        # 多头期权失效：全部成本变为亏损
        if self.qty > 0:
            remaining = self.qty
            while remaining > 0 and self.buy_lots:
                lot = self.buy_lots[0]
                # 未实现亏损 = -成本 × 数量
                unrealized_loss = -lot.true_price * min(remaining, lot.qty)
                self._add_bonus(unrealized_loss, updated_at)
                remaining -= lot.qty
                self.buy_lots.popleft()
        else:
            # 空头期权失效：做空利润实现
            remaining = abs(self.qty)
            while remaining > 0 and self.sell_lots:
                lot = self.sell_lots[0]
                unrealized_loss = lot.true_price * min(remaining, lot.qty)
                self._add_bonus(unrealized_loss, updated_at)
                remaining -= lot.qty
                self.sell_lots.popleft()
        
        # 清空所有持仓
        self.buy_lots.clear()
        self.sell_lots.clear()
        self.qty = 0


# =============================================================================
# LIFO 后进先出法
# =============================================================================

class LIFOCostMethod:
    """
    后进先出法（Last-In, First-Out, LIFO）
    
    【核心原理】
    假设后买入的股票先卖出。在计算成本时，后买入的股票先被卖出。
    
    【特点】
    1. 在物价上涨时期，成本较高，利润较低，可减少税负
    2. 更接近当前成本水平
    3. 不符合实际交易顺序，核算较复杂
    4. 美国税法允许使用（但需要专门申报）
    
    【数据结构】
    使用列表(List)存储买入批次：
    - 新买入的批次添加到列表尾部 (append)
    - 卖出时从列表尾部取出 (pop) - 这就是LIFO的核心
    
    【与FIFO的区别】
    - FIFO: 从队列头部取 (popleft) - 先进先出
    - LIFO: 从列表尾部取 (pop) - 后进先出
    
    Example:
        >>> stock = LIFOCostMethod("AAPL", "USD")
        >>> stock.buy(price=100, qty=100, fee=5, updated_at="2024-01-01")
        >>> stock.buy(price=110, qty=100, fee=5, updated_at="2024-02-01")
        >>> stock.sell(price=120, qty=150, fee=7, updated_at="2024-03-01")
        >>> # 先卖100股（成本110），再卖50股（成本100）
    """
    
    def __init__(self, symbol: str, currency: str):
        """
        初始化LIFO成本核算对象
        
        Args:
            symbol: 股票代码
            currency: 货币代码
        """
        self.symbol = symbol
        self.currency = currency
        self.buy_lots: List[Lot] = []    # 买入批次列表（LIFO用列表）
        self.sell_lots: List[Lot] = []   # 做空批次列表
        self.bonus = 0.0
        self.bonus_by_year: Dict[int, float] = {}
        self.qty = 0
    
    def _add_bonus(self, realized: float, updated_at):
        """
        记录已实现盈亏，并按年份分类统计
        
        逻辑与FIFO完全相同，参见 FIFOCostMethod._add_bonus()
        """
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
        """
        买入股票/期权
        
        【处理流程】
        1. 计算真实成本价
        2. 如果当前是做空状态，先平掉空头（LIFO原则：最近的做空先平）
        3. 剩余的买入量创建新的Lot，添加到列表尾部
        
        【空头回补的LIFO逻辑】
        从 sell_lots 列表尾部取批次（最近的做空先平）
        
        Args:
            price: 成交价格
            qty: 买入数量
            fee: 手续费
            updated_at: 交易时间
            shares: 合约乘数，默认1
        """
        safe_fee = fee if (fee is not None and pd.notna(fee)) else 0.0
        true_price = price * shares + (safe_fee / qty if qty > 0 else 0)
        
        # 处理空头回补（LIFO：最近的做空先平）
        if self.qty < 0:
            cover = min(qty, abs(self.qty))
            remaining = cover
            
            # 从sell_lots列表尾部取（最近的做空）
            while remaining > 0 and self.sell_lots:
                sell_lot = self.sell_lots[-1]  # 取最后一个元素（最近的）
                lot_cover = min(remaining, sell_lot.qty)
                
                realized = lot_cover * (sell_lot.true_price - true_price)
                self._add_bonus(realized, updated_at)
                
                sell_lot.qty -= lot_cover
                remaining -= lot_cover
                
                if sell_lot.qty <= 0:
                    self.sell_lots.pop()  # 从尾部移除
            
            self.qty += cover
            qty -= cover
        
        # 处理正常买入
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.buy_lots.append(lot)  # 添加到尾部
            self.qty += qty
    
    def sell(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        """
        卖出股票/期权 - 【LIFO核心逻辑】
        
        【LIFO核心逻辑】
        当 qty > 0 时（多头状态），卖出会触发平仓：
        - 从 buy_lots 列表尾部取批次（最近买入的先卖出）
        - 计算盈亏：卖出价 - 买入价
        
        【与FIFO的区别】
        - FIFO: buy_lots[0] + popleft() - 取第一个（最早的）
        - LIFO: buy_lots[-1] + pop() - 取最后一个（最近的）
        
        Args:
            price: 成交价格
            qty: 卖出数量
            fee: 手续费
            updated_at: 交易时间
            shares: 合约乘数，默认1
        """
        safe_fee = fee if (fee is not None and pd.notna(fee)) else 0.0
        true_price = price * shares - (safe_fee / qty if qty > 0 else 0)
        
        # 处理多头平仓（LIFO：最近买入的先卖出）
        if self.qty > 0:
            close = min(qty, self.qty)
            remaining = close
            
            # 从buy_lots列表尾部取（最近买入的）
            while remaining > 0 and self.buy_lots:
                lot = self.buy_lots[-1]  # 取最后一个元素（最近买入的）
                lot_close = min(remaining, lot.qty)
                
                realized = lot_close * (true_price - lot.true_price)
                self._add_bonus(realized, updated_at)
                
                lot.qty -= lot_close
                remaining -= lot_close
                
                if lot.qty <= 0:
                    self.buy_lots.pop()  # 从尾部移除
            
            self.qty -= close
            qty -= close
        
        # 处理做空
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.sell_lots.append(lot)
            self.qty -= qty
    
    def add_fee(self, fee: float, updated_at):
        """
        添加额外费用
        
        参见 FIFOCostMethod.add_fee()
        """
        if fee <= 0:
            return
        self._add_bonus(-fee, updated_at)
    
    def expire_option(self, expiry_date, updated_at):
        """
        处理期权到期失效
        
        参见 FIFOCostMethod.expire_option()
        """
        if self.qty == 0:
            return
        
        if self.qty > 0:
            remaining = self.qty
            while remaining > 0 and self.buy_lots:
                lot = self.buy_lots[-1]  # LIFO：从尾部取
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


# =============================================================================
# HIFO 最高成本优先法
# =============================================================================

class HIFOCostMethod:
    """
    最高成本优先法（Highest In, First Out, HIFO）
    
    【核心原理】
    假设成本最高的股票先卖出。每次卖出时，优先选择成本最高的批次。
    
    【特点】
    1. 在物价上涨时期，可以最大化成本，最小化利润
    2. 可用于税务筹划，减少当期税负
    3. 需要遍历所有批次来找到最高成本，计算复杂度较高
    4. 美国税法允许使用（需要专门申报和记录）
    
    【核心算法】
    每次卖出时，遍历所有 buy_lots，找到 true_price 最高的批次卖出。
    
    【数据结构】
    使用列表(List)存储买入批次，因为需要随机访问来查找最高成本。
    
    Example:
        >>> stock = HIFOCostMethod("AAPL", "USD")
        >>> stock.buy(price=100, qty=100, fee=5, updated_at="2024-01-01")  # 成本100.05
        >>> stock.buy(price=120, qty=100, fee=5, updated_at="2024-02-01")  # 成本120.05
        >>> stock.buy(price=110, qty=100, fee=5, updated_at="2024-03-01")  # 成本110.05
        >>> stock.sell(price=130, qty=150, fee=7, updated_at="2024-04-01")
        >>> # 先卖100股（成本120.05），再卖50股（成本110.05）- 优先高成本
    """
    
    def __init__(self, symbol: str, currency: str):
        """
        初始化HIFO成本核算对象
        
        Args:
            symbol: 股票代码
            currency: 货币代码
        """
        self.symbol = symbol
        self.currency = currency
        self.buy_lots: List[Lot] = []   # 买入批次列表
        self.sell_lots: List[Lot] = []  # 做空批次列表
        self.bonus = 0.0
        self.bonus_by_year: Dict[int, float] = {}
        self.qty = 0
    
    def _add_bonus(self, realized: float, updated_at):
        """
        记录已实现盈亏，并按年份分类统计
        
        逻辑与FIFO相同，参见 FIFOCostMethod._add_bonus()
        """
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
        """
        获取成本最高的批次索引
        
        【核心算法】
        遍历所有 buy_lots，比较 true_price，返回最高成本的索引。
        
        Returns:
            最高成本批次的索引，如果没有批次则返回None
        
        Time Complexity: O(n)，n为批次数量
        
        Example:
            >>> # buy_lots: [成本100, 成本120, 成本110]
            >>> idx = self._get_highest_cost_lot()
            >>> print(idx)  # 1（成本120的批次）
        """
        if not self.buy_lots:
            return None
        
        max_idx = 0
        max_price = self.buy_lots[0].true_price
        
        # 遍历所有批次，找到最高成本
        for i, lot in enumerate(self.buy_lots):
            if lot.true_price > max_price:
                max_price = lot.true_price
                max_idx = i
        
        return max_idx
    
    def buy(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        """
        买入股票/期权
        
        【处理流程】
        1. 计算真实成本价
        2. 如果当前是做空状态，先平掉空头（HIFO原则：成本最高的做空先平）
        3. 剩余的买入量创建新的Lot，添加到列表
        
        【空头回补的HIFO逻辑】
        从 sell_lots 中找到成本最高的批次先平。
        
        Args:
            price: 成交价格
            qty: 买入数量
            fee: 手续费
            updated_at: 交易时间
            shares: 合约乘数，默认1
        """
        safe_fee = fee if (fee is not None and pd.notna(fee)) else 0.0
        true_price = price * shares + (safe_fee / qty if qty > 0 else 0)
        
        # 处理空头回补（HIFO：成本最高的做空先平）
        if self.qty < 0:
            cover = min(qty, abs(self.qty))
            remaining = cover
            
            while remaining > 0 and self.sell_lots:
                # 找到成本最高的做空批次
                max_idx = 0
                max_price = self.sell_lots[0].true_price
                for i, lot in enumerate(self.sell_lots):
                    if lot.true_price > max_price:
                        max_price = lot.true_price
                        max_idx = i
                
                sell_lot = self.sell_lots[max_idx]
                lot_cover = min(remaining, sell_lot.qty)
                
                realized = lot_cover * (sell_lot.true_price - true_price)
                self._add_bonus(realized, updated_at)
                
                sell_lot.qty -= lot_cover
                remaining -= lot_cover
                
                if sell_lot.qty <= 0:
                    self.sell_lots.pop(max_idx)
            
            self.qty += cover
            qty -= cover
        
        # 处理正常买入
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.buy_lots.append(lot)
            self.qty += qty
    
    def sell(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        """
        卖出股票/期权 - 【HIFO核心逻辑】
        
        【HIFO核心逻辑】
        当 qty > 0 时（多头状态），卖出会触发平仓：
        1. 调用 _get_highest_cost_lot() 找到成本最高的批次
        2. 卖出该批次（或部分）
        3. 重复直到卖出数量满足
        
        【与FIFO/LIFO的区别】
        - FIFO: 固定顺序（最早买入）
        - LIFO: 固定顺序（最近买入）
        - HIFO: 动态选择（成本最高）
        
        Args:
            price: 成交价格
            qty: 卖出数量
            fee: 手续费
            updated_at: 交易时间
            shares: 合约乘数，默认1
        """
        safe_fee = fee if (fee is not None and pd.notna(fee)) else 0.0
        true_price = price * shares - (safe_fee / qty if qty > 0 else 0)
        
        # 处理多头平仓（HIFO：成本最高的先卖出）
        if self.qty > 0:
            close = min(qty, self.qty)
            remaining = close
            
            while remaining > 0 and self.buy_lots:
                # 找到成本最高的批次
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
        
        # 处理做空
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.sell_lots.append(lot)
            self.qty -= qty
    
    def add_fee(self, fee: float, updated_at):
        """
        添加额外费用
        
        参见 FIFOCostMethod.add_fee()
        """
        if fee <= 0:
            return
        self._add_bonus(-fee, updated_at)
    
    def expire_option(self, expiry_date, updated_at):
        """
        处理期权到期失效
        
        参见 FIFOCostMethod.expire_option()
        """
        if self.qty == 0:
            return
        
        # 所有持仓都视为亏损
        for lot in self.buy_lots:
            unrealized_loss = -lot.true_price * lot.qty
            self._add_bonus(unrealized_loss, updated_at)
        
        self.buy_lots.clear()
        self.sell_lots.clear()
        self.qty = 0


# =============================================================================
# LOFO 最低成本优先法
# =============================================================================

class LOFOCostMethod:
    """
    最低成本优先法（Lowest In, First Out, LOFO）
    
    【核心原理】
    假设成本最低的股票先卖出。每次卖出时，优先选择成本最低的批次。
    
    【特点】
    1. 在物价上涨时期，成本较低，利润较高
    2. 与HIFO相反，可能增加税负
    3. 适用于需要展示高利润的场景（如融资、上市等）
    4. 计算复杂度与HIFO相同
    
    【核心算法】
    每次卖出时，遍历所有 buy_lots，找到 true_price 最低的批次卖出。
    
    【与HIFO的关系】
    LOFO和HIFO是相反的策略：
    - HIFO: 最大化成本，最小化利润，减少税负
    - LOFO: 最小化成本，最大化利润，增加税负
    
    Example:
        >>> stock = LOFOCostMethod("AAPL", "USD")
        >>> stock.buy(price=100, qty=100, fee=5, updated_at="2024-01-01")  # 成本100.05
        >>> stock.buy(price=120, qty=100, fee=5, updated_at="2024-02-01")  # 成本120.05
        >>> stock.buy(price=110, qty=100, fee=5, updated_at="2024-03-01")  # 成本110.05
        >>> stock.sell(price=130, qty=150, fee=7, updated_at="2024-04-01")
        >>> # 先卖100股（成本100.05），再卖50股（成本110.05）- 优先低成本
    """
    
    def __init__(self, symbol: str, currency: str):
        """
        初始化LOFO成本核算对象
        
        Args:
            symbol: 股票代码
            currency: 货币代码
        """
        self.symbol = symbol
        self.currency = currency
        self.buy_lots: List[Lot] = []   # 买入批次列表
        self.sell_lots: List[Lot] = []  # 做空批次列表
        self.bonus = 0.0
        self.bonus_by_year: Dict[int, float] = {}
        self.qty = 0
    
    def _add_bonus(self, realized: float, updated_at):
        """
        记录已实现盈亏，并按年份分类统计
        
        逻辑与FIFO相同，参见 FIFOCostMethod._add_bonus()
        """
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
        """
        获取成本最低的批次索引
        
        【核心算法】
        遍历所有 buy_lots，比较 true_price，返回最低成本的索引。
        
        Returns:
            最低成本批次的索引，如果没有批次则返回None
        
        Time Complexity: O(n)，n为批次数量
        
        Example:
            >>> # buy_lots: [成本100, 成本120, 成本110]
            >>> idx = self._get_lowest_cost_lot()
            >>> print(idx)  # 0（成本100的批次）
        """
        if not self.buy_lots:
            return None
        
        min_idx = 0
        min_price = self.buy_lots[0].true_price
        
        # 遍历所有批次，找到最低成本
        for i, lot in enumerate(self.buy_lots):
            if lot.true_price < min_price:
                min_price = lot.true_price
                min_idx = i
        
        return min_idx
    
    def buy(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        """
        买入股票/期权
        
        【处理流程】
        1. 计算真实成本价
        2. 如果当前是做空状态，先平掉空头（LOFO原则：成本最低的做空先平）
        3. 剩余的买入量创建新的Lot，添加到列表
        
        Args:
            price: 成交价格
            qty: 买入数量
            fee: 手续费
            updated_at: 交易时间
            shares: 合约乘数，默认1
        """
        safe_fee = fee if (fee is not None and pd.notna(fee)) else 0.0
        true_price = price * shares + (safe_fee / qty if qty > 0 else 0)
        
        # 处理空头回补（LOFO：成本最低的做空先平）
        if self.qty < 0:
            cover = min(qty, abs(self.qty))
            remaining = cover
            
            while remaining > 0 and self.sell_lots:
                # 找到成本最低的做空批次
                min_idx = 0
                min_price = self.sell_lots[0].true_price
                for i, lot in enumerate(self.sell_lots):
                    if lot.true_price < min_price:
                        min_price = lot.true_price
                        min_idx = i
                
                sell_lot = self.sell_lots[min_idx]
                lot_cover = min(remaining, sell_lot.qty)
                
                realized = lot_cover * (sell_lot.true_price - true_price)
                self._add_bonus(realized, updated_at)
                
                sell_lot.qty -= lot_cover
                remaining -= lot_cover
                
                if sell_lot.qty <= 0:
                    self.sell_lots.pop(min_idx)
            
            self.qty += cover
            qty -= cover
        
        # 处理正常买入
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.buy_lots.append(lot)
            self.qty += qty
    
    def sell(self, price: float, qty: int, fee: float, updated_at, shares: int = 1):
        """
        卖出股票/期权 - 【LOFO核心逻辑】
        
        【LOFO核心逻辑】
        当 qty > 0 时（多头状态），卖出会触发平仓：
        1. 调用 _get_lowest_cost_lot() 找到成本最低的批次
        2. 卖出该批次（或部分）
        3. 重复直到卖出数量满足
        
        【与HIFO的区别】
        - HIFO: _get_highest_cost_lot() - 找最高成本
        - LOFO: _get_lowest_cost_lot() - 找最低成本
        
        Args:
            price: 成交价格
            qty: 卖出数量
            fee: 手续费
            updated_at: 交易时间
            shares: 合约乘数，默认1
        """
        safe_fee = fee if (fee is not None and pd.notna(fee)) else 0.0
        true_price = price * shares - (safe_fee / qty if qty > 0 else 0)
        
        # 处理多头平仓（LOFO：成本最低的先卖出）
        if self.qty > 0:
            close = min(qty, self.qty)
            remaining = close
            
            while remaining > 0 and self.buy_lots:
                # 找到成本最低的批次
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
        
        # 处理做空
        if qty > 0:
            lot = Lot(price=price, qty=qty, fee=fee, updated_at=updated_at)
            lot.true_price = true_price
            self.sell_lots.append(lot)
            self.qty -= qty
    
    def add_fee(self, fee: float, updated_at):
        """
        添加额外费用
        
        参见 FIFOCostMethod.add_fee()
        """
        if fee <= 0:
            return
        self._add_bonus(-fee, updated_at)
    
    def expire_option(self, expiry_date, updated_at):
        """
        处理期权到期失效
        
        参见 FIFOCostMethod.expire_option()
        """
        if self.qty == 0:
            return
        
        # 所有持仓都视为亏损
        for lot in self.buy_lots:
            unrealized_loss = -lot.true_price * lot.qty
            self._add_bonus(unrealized_loss, updated_at)
        
        self.buy_lots.clear()
        self.sell_lots.clear()
        self.qty = 0


# =============================================================================
# 工厂函数
# =============================================================================

# 成本方法注册表
# 添加新方法时，只需要在这里注册即可
COST_METHODS = {
    "FIFO": FIFOCostMethod,  # 先进先出法（默认）
    "LIFO": LIFOCostMethod,  # 后进先出法
    "HIFO": HIFOCostMethod,  # 最高成本优先法
    "LOFO": LOFOCostMethod,  # 最低成本优先法
}


def create_stock(symbol: str, currency: str, method: str = "FIFO"):
    """
    创建指定成本核算方法的股票对象（工厂函数）
    
    这是创建股票成本核算对象的推荐方式，通过方法名称字符串来创建对应的对象。
    
    Args:
        symbol: 股票代码，如 "AAPL"、"HK.00700"
        currency: 货币代码，如 "USD"、"HKD"
        method: 成本核算方法，可选值: "FIFO", "LIFO", "HIFO", "LOFO"，默认"FIFO"
    
    Returns:
        对应方法的股票成本核算对象
    
    Raises:
        ValueError: 如果指定的方法不存在
    
    Example:
        >>> # 创建FIFO方法的股票对象
        >>> stock = create_stock("AAPL", "USD", "FIFO")
        >>> 
        >>> # 创建HIFO方法的股票对象（税务优化）
        >>> stock = create_stock("TSLA", "USD", "HIFO")
        >>> 
        >>> # 默认使用FIFO
        >>> stock = create_stock("00700", "HKD")
    """
    if method.upper() not in COST_METHODS:
        raise ValueError(f"不支持的成本核算方法: {method}，可选值: {list(COST_METHODS.keys())}")
    
    return COST_METHODS[method.upper()](symbol, currency)
