#!/usr/bin/env python3
"""
从成交历史文件计算盈利和税
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from collections import defaultdict

from api.utils import safe_read_csv
from api.cost_methods import create_stock, COST_METHODS
from api.trade_type import Stock
import re


def parse_option_expiry_from_symbol(symbol: str):
    """
    从股票代码中解析期权到期日期
    
    Args:
        symbol: 股票代码
        
    Returns:
        (expiry_date, is_option): 到期日期(如果是期权)和是否是期权
    """
    # 检查是否是期权格式，例如: HK_SMC230928C24000 或 US_AAPL181130C215000
    # 期权代码通常包含类似 YYMMDD 的日期格式
    pattern = r'(\d{2})(\d{2})(\d{2})[CP]'
    match = re.search(pattern, symbol)
    
    if match:
        try:
            yy = int(match.group(1))
            mm = int(match.group(2))
            dd = int(match.group(3))
            
            # 假设2000年后
            year = 2000 + yy
            
            from datetime import datetime
            expiry_date = datetime(year, mm, dd)
            return (expiry_date, True)
        except Exception:
            pass
    
    return (None, False)


COST_METHOD_OPTIONS = {
    "AVERAGE": "移动加权平均法",
    "FIFO": "先进先出法 (FIFO)",
    "LIFO": "后进先出法 (LIFO)",
    "HIFO": "最高成本优先法 (HIFO)",
    "LOFO": "最低成本优先法 (LOFO)",
}

COST_METHOD_DESCRIPTIONS = {
    "AVERAGE": "将所有买入成本加权平均，计算简便，能平滑成本波动",
    "FIFO": "假设先买入的股票先卖出，符合实际交易顺序",
    "LIFO": "假设后买入的股票先卖出，在物价上涨时可减少税负",
    "HIFO": "假设成本最高的股票先卖出，可最大化成本、最小化利润",
    "LOFO": "假设成本最低的股票先卖出，最小化成本、最大化利润",
}


def format_trade_from_file(data_path, cost_method="AVERAGE", start_date=None, end_date=None):
    """
    从交易数据文件格式化并计算已实现收益
    
    重要：所有交易（包括开始日期之前的）都需要参与计算，以正确计算持仓成本。
    但只统计在指定日期范围内卖出的盈利。
    
    Args:
        data_path: 交易数据文件路径
        cost_method: 成本核算方法
        start_date: 开始日期 (datetime)
        end_date: 结束日期 (datetime)
    
    Returns:
        dict: {symbol: Stock对象} - 只包含在指定日期范围内有卖出盈利的股票
    """
    data = safe_read_csv(data_path)
    
    if "updated_time" not in data.columns and "create_time" in data.columns:
        # 使用apply逐个解析，这样能处理更多日期格式
        data = data.assign(updated_time=lambda df: df["create_time"].apply(lambda x: pd.to_datetime(x, errors="coerce")))
    elif "updated_time" not in data.columns:
        print("错误: 数据文件中没有 'create_time' 或 'updated_time' 列！")
        sys.exit(1)
    
    data = data.sort_values(by="updated_time", ascending=True)
    
    if "qty" in data.columns:
        data = data[data["qty"] > 0]
    
    # 第一步：用完整数据计算（包含所有历史记录）
    full_pool = {}
    
    market2currency = {
        "HK": "HKD",
        "US": "USD",
    }
    
    contract_multiplier = {
        "HK": 500,
        "US": 100,
    }
    
    def create_stock_instance(symbol, currency):
        if cost_method.upper() == "AVERAGE":
            return Stock(symbol, currency)
        else:
            return create_stock(symbol, currency, cost_method)
    
    for _, row in data.iterrows():
        updated_time = row["updated_time"]
        
        if pd.isna(updated_time):
            continue
        
        symbol = row.get("code", row.get("symbol", None))
        if symbol is None:
            print("警告: 记录中没有 code 或 symbol 列，跳过")
            continue
        
        expiry_date, is_option = parse_option_expiry_from_symbol(symbol)
        
        deal_market = row.get("deal_market", "US")
        shares = 1 if not is_option else contract_multiplier.get(deal_market, 100)
        
        if symbol not in full_pool:
            currency = market2currency.get(deal_market, "USD")
            full_pool[symbol] = create_stock_instance(symbol, currency)
        
        trd_side = str(row.get("trd_side", "")).upper()
        
        qty = int(row.get("qty", 0))
        price = float(row.get("price", 0))
        fee = float(row.get("fee_amount", row.get("fee", 0)))
        
        if "SELL" in trd_side:
            full_pool[symbol].sell(price, qty, fee, updated_time, shares)
        else:
            full_pool[symbol].buy(price, qty, fee, updated_time, shares)
    
    # 第二步：如果没有指定日期范围，直接返回完整结果
    if start_date is None and end_date is None:
        return full_pool
    
    # 第三步：重新计算，只保留指定日期范围内的卖出盈利
    # 我们需要重新初始化所有股票，重放所有交易，
    # 但只记录在指定日期范围内卖出时产生的盈利
    
    result_pool = {}
    
    # 再次遍历数据，这次跟踪哪些盈利属于指定日期范围
    for symbol in full_pool.keys():
        # 创建新的股票实例
        currency = full_pool[symbol].currency
        result_pool[symbol] = create_stock_instance(symbol, currency)
    
    # 再次完整遍历所有交易记录
    for _, row in data.iterrows():
        updated_time = row["updated_time"]
        
        if pd.isna(updated_time):
            continue
        
        symbol = row.get("code", row.get("symbol", None))
        if symbol is None or symbol not in result_pool:
            continue
        
        expiry_date, is_option = parse_option_expiry_from_symbol(symbol)
        
        deal_market = row.get("deal_market", "US")
        shares = 1 if not is_option else contract_multiplier.get(deal_market, 100)
        
        trd_side = str(row.get("trd_side", "")).upper()
        
        qty = int(row.get("qty", 0))
        price = float(row.get("price", 0))
        fee = float(row.get("fee_amount", row.get("fee", 0)))
        
        # 在执行交易前，备份当前的bonus_by_year
        stock_obj = result_pool[symbol]
        prev_bonus_by_year = dict(stock_obj.bonus_by_year)
        
        if "SELL" in trd_side:
            stock_obj.sell(price, qty, fee, updated_time, shares)
        else:
            stock_obj.buy(price, qty, fee, updated_time, shares)
        
        # 检查这次交易是否在指定日期范围内
        is_in_range = True
        if start_date and updated_time < start_date:
            is_in_range = False
        if end_date and updated_time > end_date:
            is_in_range = False
        
        # 如果是卖单且不在范围内，需要回滚这次产生的盈利
        if "SELL" in trd_side and not is_in_range:
            # 恢复之前的bonus_by_year
            stock_obj.bonus_by_year = prev_bonus_by_year
            # 重新计算total bonus
            stock_obj.bonus = sum(prev_bonus_by_year.values())
    
    # 最后，过滤掉没有盈利的股票
    final_pool = {}
    for symbol, stock_obj in result_pool.items():
        if stock_obj.bonus_by_year:  # 只要有年度盈利就保留
            final_pool[symbol] = stock_obj
    
    return final_pool


def show_yearly_bonus_by_currency(stocks):
    """
    按货币分组显示年度收益
    """
    if isinstance(stocks, dict):
        stocks = list(stocks.values())
    
    currency_groups = defaultdict(list)
    for s in stocks:
        currency_groups[s.currency].append(s)
    
    for currency, group in currency_groups.items():
        print(f"\n{'='*80}")
        print(f"{currency} — 每年已实现收益")
        print(f"{'='*80}")
        
        all_years = sorted({y for s in group for y in s.bonus_by_year.keys()})
        rows = []
        for s in group:
            row = {"Symbol": s.symbol}
            total = 0.0
            for y in all_years:
                v = s.bonus_by_year.get(y, 0.0)
                row[str(y)] = v
                total += v
            row["Total"] = total
            rows.append(row)
        
        df = pd.DataFrame(rows).set_index("Symbol")
        
        sum_row = {"Symbol": f"{currency} Total"}
        for y in all_years:
            sum_row[str(y)] = df[str(y)].sum()
        sum_row["Total"] = df["Total"].sum()
        df = pd.concat([df, pd.DataFrame([sum_row]).set_index("Symbol")])
        
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        
        print(df.to_string(float_format=lambda x: f"{x:.2f}"))


def main():
    parser = argparse.ArgumentParser(description="从成交历史文件计算盈利")
    parser.add_argument("file_path", nargs="?", help="交易数据文件路径 (如 futu_trade.csv 或 stocks/xxx.csv)")
    parser.add_argument("--method", "-m", default="FIFO", choices=list(COST_METHOD_OPTIONS.keys()),
                        help=f"成本核算方法 (默认: FIFO)。可选值: {', '.join(COST_METHOD_OPTIONS.keys())}")
    parser.add_argument("--start-date", "-s", help="开始日期 (格式: YYYY-MM-DD)")
    parser.add_argument("--end-date", "-e", help="结束日期 (格式: YYYY-MM-DD)")
    parser.add_argument("--list-methods", "-l", action="store_true", help="列出所有可用的成本核算方法")
    
    args = parser.parse_args()
    
    if args.list_methods:
        print("可用的成本核算方法:")
        print("-" * 60)
        for key, name in COST_METHOD_OPTIONS.items():
            print(f"{key:10s} - {name}")
            print(f"           {COST_METHOD_DESCRIPTIONS[key]}")
            print()
        return
    
    if not args.file_path:
        parser.print_help()
        print("\n错误: 请提供数据文件路径或使用 --list-methods 查看可用方法")
        sys.exit(1)
    
    file_path = Path(args.file_path)
    if not file_path.exists():
        print(f"错误: 文件不存在: {file_path}")
        sys.exit(1)
    
    print("="*80)
    print("📊 从文件计算盈利和税")
    print("="*80)
    print(f"\n数据文件: {file_path}")
    print(f"计算方法: {COST_METHOD_OPTIONS[args.method]}")
    
    start_date = None
    end_date = None
    
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            print(f"开始日期: {start_date.strftime('%Y-%m-%d')}")
        except ValueError:
            print(f"错误: 无效的开始日期格式: {args.start_date}，请使用 YYYY-MM-DD")
            sys.exit(1)
    
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            print(f"结束日期: {end_date.strftime('%Y-%m-%d')}")
        except ValueError:
            print(f"错误: 无效的结束日期格式: {args.end_date}，请使用 YYYY-MM-DD")
            sys.exit(1)
    
    print("\n正在计算...")
    
    try:
        stocks = format_trade_from_file(
            str(file_path), 
            cost_method=args.method,
            start_date=start_date,
            end_date=end_date
        )
        
        if not stocks:
            print("\n未找到符合条件的交易记录！")
            return
        
        show_yearly_bonus_by_currency(stocks)
        
        print(f"\n{'='*80}")
        print("计算完成！")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"\n错误: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
