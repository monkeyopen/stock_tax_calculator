import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import yaml

from api.utils import run_with_output, safe_read_csv

with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

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


def show_yearly_bonus_by_currency(stocks, title):
    if isinstance(stocks, dict):
        stocks = list(stocks.values())

    from collections import defaultdict

    currency_groups = defaultdict(list)
    for stock in stocks:
        currency_groups[stock.currency].append(stock)

    for currency, group in currency_groups.items():
        st.subheader(f"{title} — {currency}")

        all_years = sorted({year for stock in group for year in stock.bonus_by_year.keys()})
        rows = []
        for stock in group:
            row = {"Symbol": stock.symbol}
            total = 0.0
            for year in all_years:
                value = stock.bonus_by_year.get(year, 0.0)
                row[str(year)] = value
                total += value
            row["Total"] = total
            rows.append(row)

        df = pd.DataFrame(rows).set_index("Symbol")

        sum_row = {"Symbol": f"{currency} Total"}
        for year in all_years:
            sum_row[str(year)] = df[str(year)].sum()
        sum_row["Total"] = df["Total"].sum()
        df = pd.concat([df, pd.DataFrame([sum_row]).set_index("Symbol")])

        numeric_cols = df.select_dtypes(include="number").columns
        max_abs = df[numeric_cols].abs().max().max() or 1

        def color_by_value(value):
            if pd.isna(value):
                return ""
            norm = min(abs(value) / max_abs, 1.0)
            if value > 0:
                green = int(50 + 205 * norm)
                return f"color: rgb(0,{green},0)"
            if value < 0:
                red = int(50 + 205 * norm)
                return f"color: rgb({red},0,0)"
            return "color: gray"

        styled = df.style.format("{:.2f}").applymap(color_by_value, subset=numeric_cols)
        st.dataframe(styled, use_container_width=True)


def file_has_data(file_path):
    path = Path(file_path)
    return path.exists() and len(safe_read_csv(path)) > 0


st.title("📊 富途股票年度已实现收益分析工具")
st.caption("当前版本仅保留 Futu 数据下载与收益计算功能。")

st.sidebar.header("操作区")
st.sidebar.subheader("📐 成本核算方法")

cost_method_keys = list(COST_METHOD_OPTIONS.keys())
default_index = cost_method_keys.index("AVERAGE")

selected_cost_method = st.sidebar.selectbox(
    "选择计算方式",
    options=cost_method_keys,
    format_func=lambda key: COST_METHOD_OPTIONS[key],
    index=default_index,
    help="选择不同的成本核算方法会影响已实现收益的计算结果",
)

st.sidebar.info(
    f"**{COST_METHOD_OPTIONS[selected_cost_method]}**\n\n{COST_METHOD_DESCRIPTIONS[selected_cost_method]}"
)

if selected_cost_method != "AVERAGE":
    st.sidebar.warning("⚠️ 您选择了非默认的成本核算方法，计算结果可能与报税要求不同。")

today = datetime.today().date()

st.sidebar.subheader("富途查询时间")
futu_start = st.sidebar.date_input("开始日期", value=today, max_value=today)
futu_start = datetime.combine(futu_start, datetime.min.time())
futu_end = st.sidebar.date_input("结束日期", value=today, max_value=today)
futu_end = datetime.combine(futu_end, datetime.min.time())

download_btn_futu = st.sidebar.button("⬇️ 开始下载富途数据")
compute_btn = st.sidebar.button("🚀 开始计算", type="primary")

if download_btn_futu:
    from api import user_futu

    st.info("正在下载富途交易流水")
    run_with_output(user_futu.get_trade_flow, config["futu"]["trade_file"], futu_start, futu_end)

    # st.info("正在下载富途现金流水")
    # run_with_output(user_futu.get_cash_flow, config["futu"]["cash_file"], futu_start, futu_end)

    st.success("富途数据下载完成 ✅")

if compute_btn:
    from api import user_futu

    futu_trade_file = config["futu"]["trade_file"]
    futu_cash_file = config["futu"]["cash_file"]

    st.info(f"正在使用 **{COST_METHOD_OPTIONS[selected_cost_method]}** 计算富途已实现收益...")

    try:
        if not file_has_data(futu_trade_file):
            st.info("未检测到可用的富途交易流水，请先下载或导入数据。")
        else:
            if not file_has_data(futu_cash_file):
                futu_cash_file = None
                st.warning("富途现金流水不存在，计算结果可能不准确。")

            futu_data = user_futu.format_trade(
                futu_trade_file,
                futu_cash_file,
                cost_method=selected_cost_method,
            )
            show_yearly_bonus_by_currency(futu_data, "富途每年已实现收益")
    except Exception as error:
        st.error(f"计算过程中发生错误: {error}")
        st.exception(error)
