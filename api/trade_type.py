#!/usr/bin/env python3
"""
股票交易类型模块 - 实现移动平均成本法（Weighted Average Cost）

本模块实现了移动平均成本法（也称为加权平均成本法），用于计算股票持仓的平均成本。

【核心原理】
移动平均成本法是一种成本核算方法，每次买入时都会重新计算平均成本：
- 平均成本 = 总成本 / 总数量
- 每次买入后，平均成本会被更新
- 卖出时，使用当前的平均成本来计算盈亏

【与 FIFO/LIFO 的区别】
- FIFO/LIFO/HIFO/LOFO：跟踪每个批次的成本，卖出时选择特定批次
- 移动平均法：不跟踪批次，只维护一个平均成本值

【特点】
1. 计算简单，只需要维护一个平均成本值
2. 不需要跟踪每个批次的详细信息
3. 每次买入都会平滑成本（上涨时平均成本上升，下跌时平均成本下降）
4. 适用于频繁交易的场景
5. 中国 A 股市场默认使用此方法

【应用场景】
- 中国 A 股市场（默认方法）
- 基金定投
- 需要简化成本核算的场景
"""

from datetime import datetime
import pandas as pd


class Stock:
    """
    股票类 - 实现移动平均成本法
    
    这个类使用移动平均成本法来跟踪股票持仓和计算盈亏。
    与 cost_methods.py 中的 FIFO/LIFO 等方法不同，这个方法不跟踪批次，
    只维护一个平均成本值。
    
    【核心属性】
    - qty: 持仓数量（正数=多头，负数=空头）
    - cost: 总成本 = qty × average_price
    - average_price: 平均成本价（通过@property 动态计算）
    
    【核心方法】
    - buy(): 买入股票（可能平空或开多）
    - sell(): 卖出股票（可能平多或开空）
    - add_fee(): 添加额外费用
    - expire_option(): 处理期权到期
    
    Example:
        >>> stock = Stock("AAPL", "USD")
        >>> # 第一次买入 100 股，每股$100
        >>> stock.buy(price=100, qty=100, free=5, updated_at="2024-01-01")
        >>> print(stock.average_price)  # 100.05（包含手续费）
        >>> 
        >>> # 第二次买入 100 股，每股$110
        >>> stock.buy(price=110, qty=100, free=5, updated_at="2024-02-01")
        >>> print(stock.average_price)  # 105.05（平均成本）
        >>> 
        >>> # 卖出 150 股，每股$120
        >>> stock.sell(price=120, qty=150, free=7, updated_at="2024-03-01")
        >>> print(stock.bonus)  # 已实现盈亏
    """
    
    def __init__(self, symbol, currency) -> None:
        """
        初始化股票对象
        
        Args:
            symbol: 股票代码，如 "AAPL"、"HK.00700"
            currency: 货币代码，如 "USD"、"HKD"
        
        初始化后的状态：
        - qty = 0: 空仓
        - cost = 0.0: 无成本
        - bonus = 0.0: 无盈亏
        """
        self.qty = 0                  # 持仓数量：做空 < 0, 做多 > 0, 空仓 = 0
        self.cost = 0.0               # 总成本 = qty * avg_price（允许为负数）
                                      # 正数表示多头成本，负数表示空头成本
        self.symbol = symbol          # 股票代码
        self.currency = currency      # 货币
        
        self.bonus = 0.0              # 累计已实现盈亏（所有交易的盈亏总和）
        self.bonus_by_year = {}       # 按年份统计的盈亏 {year: amount}，用于税务申报
    
    @property
    def average_price(self):
        """
        计算平均成本价（移动平均核心）
        
        【计算公式】
        average_price = cost / qty
        
        【特殊情况】
        - 当 qty = 0 时，返回 0.0（避免除以零错误）
        - 当 qty < 0 时（做空），返回的是做空的平均成本
        
        【移动平均的体现】
        每次买入时，cost 会更新，从而 average_price 也会更新：
        - 新平均成本 = (旧成本 + 新买入成本) / (旧数量 + 新数量)
        
        Returns:
            平均成本价（每股）
        
        Example:
            >>> stock = Stock("AAPL", "USD")
            >>> stock.buy(price=100, qty=100, free=0, updated_at="2024-01-01")
            >>> stock.buy(price=120, qty=100, free=0, updated_at="2024-02-01")
            >>> print(stock.average_price)  # 110.0（(100*100 + 120*100) / 200）
        """
        if self.qty == 0:
            return 0.0
        return self.cost / self.qty
    
    def _add_bonus(self, realized, updated_at):
        """
        记录已实现盈亏，并按年份分类统计
        
        这个方法会被 buy() 和 sell() 方法调用，每当有盈亏实现时记录。
        
        【盈亏计算时机】
        - 平仓时（卖出多头或买入平空）
        - 期权到期时
        - 添加额外费用时
        
        Args:
            realized: 本次实现的盈亏金额
                     - 正数 = 盈利（赚钱）
                     - 负数 = 亏损（赔钱）
            updated_at: 交易时间，用于确定盈亏所属年份
                       支持格式：
                       - datetime 对象
                       - pd.Timestamp 对象
                       - 字符串："2024-01-01 10:00:00" 或 "2024-01-01 10:00:00.000000"
        
        Note:
            - 如果 realized 为 0，则不记录（避免无意义的操作）
            - 同时更新总盈亏（bonus）和年度盈亏（bonus_by_year）
        
        Example:
            >>> stock = Stock("AAPL", "USD")
            >>> stock._add_bonus(1000.50, "2024-03-15 14:30:00")
            >>> print(stock.bonus)  # 1000.50
            >>> print(stock.bonus_by_year)  # {2024: 1000.50}
        """
        if realized == 0:
            return
        
        # 累加到总盈亏
        self.bonus += realized
        
        # ---- 情况 1：已经是 datetime / Timestamp ----
        if isinstance(updated_at, (datetime, pd.Timestamp)):
            dt = updated_at
        
        # ---- 情况 2：字符串（可能有/没有毫秒）----
        else:
            try:
                # 尝试解析带毫秒的格式
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                # 如果没有毫秒，使用不带毫秒的格式
                dt = datetime.strptime(updated_at, "%Y-%m-%d %H:%M:%S")
        
        year = dt.year
        # 按年份累加盈亏（用于税务申报）
        self.bonus_by_year[year] = self.bonus_by_year.get(year, 0.0) + realized
    
    def buy(self, price, qty, free, updated_at, shares=1):
        """
        买入股票/期权 - 移动平均成本法
        
        【处理流程】
        1. 计算真实买入价 = price×shares + 手续费分摊
        2. 如果当前是做空状态 (qty < 0)，先平掉部分或全部空头
        3. 剩余的买入量增加多头仓位，并更新平均成本
        
        【移动平均核心逻辑】
        当买入开多时：
        - 新成本 = 旧成本 + 本次买入成本
        - 新数量 = 旧数量 + 本次买入数量
        - 新平均成本 = 新成本 / 新数量（自动计算，通过 average_price 属性）
        
        【空头回补逻辑】
        当 qty < 0 时（做空状态），买入会触发空头回补：
        - 平仓盈亏 = 平仓数量 × (做空平均成本 - 买入成本)
        - 如果买入价 < 做空成本，则盈利（低价买回）
        - 如果买入价 > 做空成本，则亏损（高价买回）
        
        Args:
            price: 成交价格（每股/每合约的价格）
            qty: 买入数量（股数或合约数）
            free: 手续费（总费用）
            updated_at: 交易时间
            shares: 合约乘数（股票=1，期权=100），默认 1
        
        Example:
            >>> stock = Stock("AAPL", "USD")
            >>> 
            >>> # 场景 1：普通买入（开多）
            >>> stock.buy(price=150, qty=100, free=5, updated_at="2024-01-01")
            >>> print(stock.qty)  # 100
            >>> print(stock.average_price)  # 150.05
            >>> 
            >>> # 场景 2：做空后回补
            >>> stock.sell(price=160, qty=100, free=5, updated_at="2024-02-01")  # 先做空
            >>> stock.buy(price=150, qty=100, free=5, updated_at="2024-03-01")   # 再回补
            >>> # 盈利 = 100 × (160 - 150) = 1000
        """
        # 计算真实买入价：成交价×乘数 + 手续费分摊
        # 手续费会增加成本，所以是加号
        safe_free = free if (free is not None and pd.notna(free)) else 0.0
        true_price = price * shares + (safe_free / qty if qty > 0 else 0)
        
        # ========== 步骤 1：处理空头回补 ==========
        # 如果当前是做空状态 (qty < 0)，买入会平掉空头仓位
        if self.qty < 0:
            # 计算本次买入能平掉多少空头
            # cover = min(买入量，空头持仓量)
            cover = min(qty, abs(self.qty))
            
            # 计算平仓盈亏
            # 盈亏 = 数量 × (做空成本 - 买入成本)
            # 如果买入价 < 做空成本，则盈利（低价买回高价卖出的仓位）
            realized = cover * (self.average_price - true_price)
            self._add_bonus(realized, updated_at)
            
            # 更新成本和数量
            # 平仓部分按做空成本计算（因为之前做空时已经记录了成本）
            self.cost += self.average_price * cover  # 增加成本（做空成本是负数，所以是减少绝对值）
            self.qty += cover                        # 减少空头持仓
            qty -= cover                             # 剩余的买入量
        
        # ========== 步骤 2：处理正常买入（开多）==========
        # 如果还有剩余买入量，增加多头仓位
        if qty > 0:
            # 移动平均核心：直接累加成本和数量
            # 新平均成本会在下次访问 average_price 时自动计算
            self.cost += true_price * qty  # 增加总成本
            self.qty += qty                # 增加持仓数量
    
    def sell(self, price, qty, free, updated_at, shares=1):
        """
        卖出股票/期权 - 移动平均成本法
        
        【处理流程】
        1. 计算真实卖出价 = price×shares - 手续费分摊
        2. 如果当前是多头状态 (qty > 0)，先平掉部分或全部多头
        3. 如果还有剩余卖出量，创建做空仓位
        
        【移动平均核心逻辑】
        当卖出平仓时：
        - 平仓盈亏 = 平仓数量 × (卖出价 - 平均成本)
        - 如果卖出价 > 平均成本，则盈利
        - 如果卖出价 < 平均成本，则亏损
        - 平仓后，剩余持仓的平均成本不变（因为是按平均成本计算的）
        
        【多头平仓逻辑】
        当 qty > 0 时（多头状态），卖出会触发平仓：
        - 平仓盈亏 = 平仓数量 × (卖出真实价格 - 平均成本)
        - 成本减少 = 平均成本 × 平仓数量
        - 数量减少 = 平仓数量
        
        Args:
            price: 成交价格（每股/每合约的价格）
            qty: 卖出数量（股数或合约数）
            free: 手续费（总费用）
            updated_at: 交易时间
            shares: 合约乘数（股票=1，期权=100），默认 1
        
        Example:
            >>> stock = Stock("AAPL", "USD")
            >>> 
            >>> # 场景 1：普通卖出（平多）
            >>> stock.buy(price=100, qty=100, free=0, updated_at="2024-01-01")
            >>> stock.sell(price=120, qty=100, free=5, updated_at="2024-02-01")
            >>> # 盈利 = 100 × (120 - 100) = 2000
            >>> 
            >>> # 场景 2：部分卖出
            >>> stock.buy(price=100, qty=200, free=0, updated_at="2024-01-01")
            >>> stock.sell(price=120, qty=100, free=0, updated_at="2024-02-01")
            >>> # 盈利 = 100 × (120 - 100) = 2000
            >>> # 剩余 100 股，平均成本仍为 100
        """
        # 计算真实卖出价：成交价×乘数 - 手续费分摊
        # 手续费会减少收入，所以是减号
        safe_free = free if (free is not None and pd.notna(free)) else 0.0
        true_price = price * shares - (safe_free / qty if qty > 0 else 0)
        
        # ========== 步骤 1：处理多头平仓 ==========
        # 如果当前是多头状态 (qty > 0)，卖出会平掉多头仓位
        if self.qty > 0:
            # 计算本次卖出能平掉多少多头
            close = min(qty, self.qty)
            
            # 计算平仓盈亏
            # 盈亏 = 数量 × (卖出价 - 平均成本)
            # 如果卖出价 > 平均成本，则盈利
            realized = close * (true_price - self.average_price)
            self._add_bonus(realized, updated_at)
            
            # 更新成本和数量
            # 平仓部分按平均成本计算（因为是按平均成本买入的）
            self.cost -= self.average_price * close  # 减少成本
            self.qty -= close                        # 减少持仓数量
            qty -= close                             # 剩余的卖出量
        
        # ========== 步骤 2：处理做空 ==========
        # 如果还有剩余卖出量，创建做空仓位
        if qty > 0:
            # 做空：成本减少（因为做空是借股票卖出，获得现金）
            # 做空成本 = 卖出价 × 数量
            self.cost -= true_price * qty  # 减少成本（变为负数）
            self.qty -= qty                # 减少持仓（变为负数）
    
    def add_fee(self, fee, updated_at):
        """
        添加额外费用（如股息税、融资利息等）
        
        这些费用会被记录为负收益（亏损），因为它们减少了总利润。
        
        【常见费用类型】
        - 股息税（Dividend Tax）
        - 融资利息（Margin Interest）
        - 平台使用费
        - 其他交易相关费用
        
        Args:
            fee: 费用金额（必须为正数）
            updated_at: 费用发生时间
        
        Example:
            >>> stock = Stock("AAPL", "USD")
            >>> # 添加股息税
            >>> stock.add_fee(50.0, "2024-06-15")
            >>> # 这 50 元会从 bonus 中扣除
        """
        if fee <= 0:
            return
        
        # 费用视作负收益（亏损）
        self._add_bonus(-fee, updated_at)
    
    def expire_option(self, expiry_date, updated_at):
        """
        处理期权到期失效
        
        当期权到期时，如果还有持仓，视为全部亏损（价值归 0）。
        
        【处理逻辑】
        1. 计算未实现亏损 = -平均成本 × 持仓数量
        2. 将未实现亏损记录为已实现亏损（因为期权真的作废了）
        3. 清空持仓
        
        【为什么是亏损？】
        - 期权到期时，如果不行权，价值归 0
        - 之前买入期权的成本全部损失
        - 亏损金额 = 买入成本 = 平均成本 × 数量
        
        Args:
            expiry_date: 期权到期日（datetime 对象）
            updated_at: 失效记录时间
        
        Example:
            >>> stock = Stock("AAPL240615C00150000", "USD")
            >>> stock.buy(price=5, qty=1, free=1, updated_at="2024-01-01", shares=100)
            >>> # 期权到期，价值归 0
            >>> stock.expire_option(expiry_date=datetime(2024, 6, 15), 
            ...                     updated_at="2024-06-15")
            >>> # 亏损 = 500 + 1 = 501
            >>> print(stock.bonus)  # -501.0
        """
        if self.qty == 0:
            return
        
        # 计算未实现亏损（过期期权价值归 0）
        # 亏损 = -成本 × 数量
        unrealized_loss = -self.average_price * self.qty
        self._add_bonus(unrealized_loss, updated_at)
        
        # 清空持仓
        self.cost = 0.0
        self.qty = 0
        
        # 打印调试信息
        print(f"期权失效处理：{self.symbol}, 到期日：{expiry_date.date()}, "
              f"持仓量：{abs(self.qty)}, 损失：{unrealized_loss:.2f}")
