#!/usr/bin/env python3
"""
从成交历史文件计算盈利和税 - Streamlit网页版
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import re

from api.utils import safe_read_csv
from api.cost_methods import create_stock, COST_METHODS
from api.trade_type import Stock


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


def parse_option_expiry_from_symbol(symbol: str):
    """
    从股票代码中解析期权到期日期
    
    Args:
        symbol: 股票代码
        
    Returns:
        (expiry_date, is_option): 到期日期(如果是期权)和是否是期权
    """
    pattern = r'(\d{2})(\d{2})(\d{2})[CP]'
    match = re.search(pattern, symbol)
    
    if match:
        try:
            yy = int(match.group(1))
            mm = int(match.group(2))
            dd = int(match.group(3))
            year = 2000 + yy
            expiry_date = datetime(year, mm, dd)
            return (expiry_date, True)
        except Exception:
            pass
    
    return (None, False)


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
        tuple: (pool_dict, symbol_to_name) - 股票对象字典和股票代码到名称的映射
    """
    data = safe_read_csv(data_path)
    
    # 收集股票代码到名称的映射
    symbol_to_name = {}
    if "stock_name" in data.columns:
        for _, row in data.iterrows():
            symbol = row.get("code", row.get("symbol", None))
            name = row.get("stock_name", "")
            if symbol and name and symbol not in symbol_to_name:
                symbol_to_name[symbol] = name
    
    if "updated_time" not in data.columns and "create_time" in data.columns:
        # 使用apply逐个解析，这样能处理更多日期格式
        data = data.assign(updated_time=lambda df: df["create_time"].apply(lambda x: pd.to_datetime(x, errors="coerce")))
    elif "updated_time" not in data.columns:
        raise ValueError("数据文件中没有 'create_time' 或 'updated_time' 列！")
    
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
        return full_pool, symbol_to_name
    
    # 第三步：重新计算，只保留指定日期范围内的卖出盈利
    # 我们需要重新初始化所有股票，重放所有交易，
    # 但只记录在指定日期范围内卖出时产生的盈利
    
    result_pool = {}
    
    # 再次遍历数据，这次跟踪哪些盈利属于指定日期范围
    for symbol in full_pool.keys():
        # 创建新的股票实例
        currency = full_pool[symbol].currency
        result_pool[symbol] = create_stock_instance(symbol, currency)
        
        # 临时保存每年的盈利，用于后续筛选
        result_pool[symbol]._temp_bonus_by_year = {}
    
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
    
    return final_pool, symbol_to_name


def show_yearly_bonus_by_currency(stocks, title, symbol_to_name=None):
    """
    按货币分组显示年度收益
    """
    if symbol_to_name is None:
        symbol_to_name = {}
    
    if isinstance(stocks, dict):
        stocks = list(stocks.values())
    
    currency_groups = defaultdict(list)
    for s in stocks:
        currency_groups[s.currency].append(s)
    
    for currency, group in currency_groups.items():
        st.subheader(f"{title} — {currency}")
        
        all_years = sorted({y for s in group for y in s.bonus_by_year.keys()})
        rows = []
        for s in group:
            row = {
                "股票代码": s.symbol,
                "股票名称": symbol_to_name.get(s.symbol, "")
            }
            total = 0.0
            for y in all_years:
                v = s.bonus_by_year.get(y, 0.0)
                row[str(y)] = v
                total += v
            row["Total"] = total
            rows.append(row)
        
        df = pd.DataFrame(rows).set_index("股票代码")
        
        sum_row = {
            "股票代码": f"{currency} Total",
            "股票名称": "合计"
        }
        for y in all_years:
            sum_row[str(y)] = df[str(y)].sum()
        sum_row["Total"] = df["Total"].sum()
        df = pd.concat([df, pd.DataFrame([sum_row]).set_index("股票代码")])
        
        numeric_cols = df.select_dtypes(include="number").columns
        max_abs = df[numeric_cols].abs().max().max() or 1
        
        def color_by_value(val):
            if pd.isna(val):
                return ""
            norm = min(abs(val) / max_abs, 1.0)
            if val > 0:
                green = int(50 + 205 * norm)
                return f"color: rgb(0,{green},0)"
            elif val < 0:
                red = int(50 + 205 * norm)
                return f"color: rgb({red},0,0)"
            return "color: gray"
        
        format_dict = {col: "{:.2f}" for col in numeric_cols}
        styled = df.style.format(format_dict).applymap(
            color_by_value, subset=numeric_cols)
        st.dataframe(styled, use_container_width=True)


def main():
    st.set_page_config(
        page_title="📊 股票盈利计算工具",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 从文件计算股票盈利和税")
    
    st.sidebar.header("⚙️ 设置")
    
    # 成本核算方法选择
    st.sidebar.subheader("📐 成本核算方法")
    cost_method_keys = list(COST_METHOD_OPTIONS.keys())
    default_index = cost_method_keys.index("FIFO")
    
    selected_cost_method = st.sidebar.selectbox(
        "选择计算方式",
        options=cost_method_keys,
        format_func=lambda x: COST_METHOD_OPTIONS[x],
        index=default_index,
        help="选择不同的成本核算方法会影响已实现收益的计算结果"
    )
    
    st.sidebar.info(f"**{COST_METHOD_OPTIONS[selected_cost_method]}**\n\n{COST_METHOD_DESCRIPTIONS[selected_cost_method]}")
    
    if selected_cost_method != "FIFO" and selected_cost_method != "AVERAGE":
        st.sidebar.warning("⚠️ 您选择了非默认的成本核算方法，计算结果可能与报税要求不同。")
    
    # 文件选择
    st.sidebar.subheader("📁 数据文件")
    
    # 查找可用的CSV文件
    cache_dir = Path(".cache_data")
    available_files = []
    
    if cache_dir.exists():
        # 主目录下的CSV文件
        for csv_file in cache_dir.glob("*.csv"):
            available_files.append(str(csv_file))
        
        # stocks目录下的CSV文件
        stocks_dir = cache_dir / "stocks"
        if stocks_dir.exists():
            for csv_file in stocks_dir.glob("*.csv"):
                available_files.append(str(csv_file))
    
    available_files = sorted(available_files)
    
    if available_files:
        selected_file = st.sidebar.selectbox(
            "选择数据文件",
            options=available_files,
            index=0 if ".cache_data/futu_trade.csv" in available_files else 0
        )
        
        # 文件上传选项
        uploaded_file = st.sidebar.file_uploader(
            "或上传您自己的CSV文件",
            type=["csv"]
        )
        
        if uploaded_file is not None:
            # 保存上传的文件到临时位置
            temp_file = Path(".cache_data") / "temp_uploaded.csv"
            temp_file.parent.mkdir(exist_ok=True)
            with open(temp_file, "wb") as f:
                f.write(uploaded_file.getbuffer())
            selected_file = str(temp_file)
            st.sidebar.success("文件已上传！")
    else:
        st.sidebar.info("未找到预存的数据文件，请上传CSV文件")
        uploaded_file = st.sidebar.file_uploader(
            "上传CSV文件",
            type=["csv"]
        )
        if uploaded_file is not None:
            temp_file = Path(".cache_data") / "temp_uploaded.csv"
            temp_file.parent.mkdir(exist_ok=True)
            with open(temp_file, "wb") as f:
                f.write(uploaded_file.getbuffer())
            selected_file = str(temp_file)
            st.sidebar.success("文件已上传！")
        else:
            selected_file = None
    
    # 日期范围选择
    st.sidebar.subheader("📅 日期范围")
    today = datetime.today().date()
    
    # 尝试从文件中获取日期范围
    min_date = None
    max_date = None
    if selected_file and Path(selected_file).exists():
        try:
            data = safe_read_csv(selected_file)
            if "create_time" in data.columns:
                # 使用apply逐个解析，这样能处理更多日期格式
                dates = data["create_time"].apply(lambda x: pd.to_datetime(x, errors="coerce")).dropna()
                if len(dates) > 0:
                    min_date = dates.min().date()
                    max_date = dates.max().date()
        except Exception:
            pass
    
    if min_date is None:
        min_date = today.replace(year=today.year - 5)
    if max_date is None:
        max_date = today
    
    start_date = st.sidebar.date_input(
        "开始日期",
        value=min_date,
        min_value=min_date,
        max_value=max_date
    )
    
    end_date = st.sidebar.date_input(
        "结束日期",
        value=max_date,
        min_value=min_date,
        max_value=max_date
    )
    
    compute_btn = st.sidebar.button("🚀 开始计算", type="primary")
    
    # 主界面
    if selected_file:
        st.info(f"📂 已选择文件: `{Path(selected_file).name}`")
        
        # 显示文件预览
        with st.expander("📋 文件预览"):
            try:
                data = safe_read_csv(selected_file)
                st.write(f"共 {len(data)} 条记录")
                st.dataframe(data.head(10), use_container_width=True)
            except Exception as e:
                st.error(f"无法预览文件: {str(e)}")
    
    if compute_btn and selected_file:
        try:
            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.max.time())
            
            with st.spinner("正在计算..."):
                stocks, symbol_to_name = format_trade_from_file(
                    selected_file,
                    cost_method=selected_cost_method,
                    start_date=start_datetime,
                    end_date=end_datetime
                )
            
            if stocks:
                st.success(f"✅ 计算完成！共处理 {len(stocks)} 只股票")
                show_yearly_bonus_by_currency(stocks, "每年已实现收益", symbol_to_name)
            else:
                st.warning("未找到符合条件的交易记录！")
                
        except Exception as e:
            st.error(f"计算过程中发生错误: {str(e)}")
            st.exception(e)


if __name__ == "__main__":
    main()
