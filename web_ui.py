import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv, set_key
import os
from api.utils import run_with_output, safe_read_csv
from api.cost_methods import COST_METHODS
import yaml

CONFIG_FILE = Path(".env")
if CONFIG_FILE.exists():
    load_dotenv(CONFIG_FILE)

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
    for s in stocks:
        currency_groups[s.currency].append(s)

    for currency, group in currency_groups.items():
        st.subheader(f"{title} — {currency}")

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

        styled = df.style.format("{:.2f}").applymap(
            color_by_value, subset=numeric_cols)
        st.dataframe(styled, use_container_width=True)


st.title("📊 股票年度已实现收益分析工具")

saved_app_key = os.getenv("LONGPORT_APP_KEY", "")
saved_app_secret = os.getenv("LONGPORT_APP_SECRET", "")
saved_token = os.getenv("LONGPORT_ACCESS_TOKEN", "")
saved_region = os.getenv("LONGPORT_REGION", "cn")

with st.expander("🔐 长桥 API 凭证", expanded=True):
    with st.form("api_form"):
        app_key = st.text_input(
            "App Key", value=saved_app_key, type="password")
        app_secret = st.text_input(
            "App Secret", value=saved_app_secret, type="password")
        access_token = st.text_input(
            "Access Token", value=saved_token, type="password")
        region = st.selectbox(
            "Region", ["cn", "hk"], index=0 if saved_region == "cn" else 1)

        if st.form_submit_button("💾 保存"):
            CONFIG_FILE.touch(exist_ok=True)
            set_key(CONFIG_FILE, "LONGPORT_APP_KEY", app_key)
            set_key(CONFIG_FILE, "LONGPORT_APP_SECRET", app_secret)
            set_key(CONFIG_FILE, "LONGPORT_ACCESS_TOKEN", access_token)
            set_key(CONFIG_FILE, "LONGPORT_REGION", region)
            st.success("凭证已保存！")

st.sidebar.header("操作区")

st.sidebar.subheader("📐 成本核算方法")
cost_method_keys = list(COST_METHOD_OPTIONS.keys())
cost_method_labels = list(COST_METHOD_OPTIONS.values())
default_index = cost_method_keys.index("AVERAGE")

selected_cost_method = st.sidebar.selectbox(
    "选择计算方式",
    options=cost_method_keys,
    format_func=lambda x: COST_METHOD_OPTIONS[x],
    index=default_index,
    help="选择不同的成本核算方法会影响已实现收益的计算结果"
)

st.sidebar.info(f"**{COST_METHOD_OPTIONS[selected_cost_method]}**\n\n{COST_METHOD_DESCRIPTIONS[selected_cost_method]}")

if selected_cost_method != "AVERAGE":
    st.sidebar.warning("⚠️ 您选择了非默认的成本核算方法，计算结果可能与报税要求不同。")

today = datetime.today().date()

st.sidebar.subheader("长桥查询时间")
longport_start = st.sidebar.date_input("开始日期", value=today, max_value=today)
longport_start = datetime.combine(longport_start, datetime.min.time())
longport_end = st.sidebar.date_input("结束日期", value=today, max_value=today)
longport_end = datetime.combine(longport_end, datetime.min.time())
download_btn_longport = st.sidebar.button("⬇️ 开始下载长桥数据")

st.sidebar.subheader("富途查询时间")
futu_start = st.sidebar.date_input(
    "开始日期", value=today, max_value=today, key="futu_start")
futu_start = datetime.combine(futu_start, datetime.min.time())
futu_end = st.sidebar.date_input(
    "结束日期", value=today, max_value=today, key="futu_end")
futu_end = datetime.combine(futu_end, datetime.min.time())
download_btn_futu = st.sidebar.button("⬇️ 开始下载富途数据")
compute_btn = st.sidebar.button("🚀 开始计算", type="primary")

if download_btn_longport:
    from api import user_longport
    st.info("正在下载长桥交易流水")
    run_with_output(user_longport.get_trade_flow, config["longport"]["trade_file"],
                    user_longport.get_ctx(), longport_start, longport_end)
    st.info("正在下载长桥现金流水")
    run_with_output(user_longport.get_cash_flow, config["longport"]["cash_file"],
                    user_longport.get_ctx(), longport_start, longport_end)
    st.info("下载完成 ✅")
if download_btn_futu:
    from api import user_futu
    st.info("正在下载富途交易流水")
    run_with_output(user_futu.get_trade_flow,
                    config["futu"]["trade_file"], futu_start, futu_end)
    st.info("正在下载富途现金流水")
    run_with_output(user_futu.get_cash_flow,
                    config["futu"]["cash_file"], futu_start, futu_end)
    st.info("下载完成 ✅")

if compute_btn:
    from api import user_futu, user_longport
    longport_trade_file = config["longport"]["trade_file"]
    longport_cash_file = config["longport"]["cash_file"]
    futu_trade_file = config["futu"]["trade_file"]
    futu_cash_file = config["futu"]["cash_file"]

    tabs = []

    def file_has_data(file_path):
        file_path = Path(file_path)
        return file_path.exists() and len(safe_read_csv(file_path)) > 0

    st.info(f"正在使用 **{COST_METHOD_OPTIONS[selected_cost_method]}** 计算已实现收益...")

    try:
        if file_has_data(longport_trade_file) and file_has_data(longport_cash_file):
            longport_data = user_longport.format_longport_trade(
                longport_trade_file,
                longport_cash_file,
                cost_method=selected_cost_method
            )
            tabs.append(("长桥", longport_data, "每年已实现收益"))

        if file_has_data(futu_trade_file):
            if not file_has_data(futu_cash_file):
                futu_cash_file = None
                st.warning("富途现金流水不存在，计算结果可能不准确")
            futu_data = user_futu.format_trade(
                futu_trade_file, 
                futu_cash_file,
                cost_method=selected_cost_method
            )
            tabs.append(("富途", futu_data, "每年已实现收益"))

        if len(tabs) == 2:
            tabs.append(("合计", None, "每年已实现收益"))

        if tabs:
            tab_objs = st.tabs([t[0] for t in tabs])

            for tab, (name, data, title) in zip(tab_objs, tabs):
                with tab:
                    if name == "合计":
                        combined = list(tabs[0][1].values()) + \
                            list(tabs[1][1].values())
                        show_yearly_bonus_by_currency(combined, title)
                    else:
                        show_yearly_bonus_by_currency(data, title)
        else:
            st.info("未检测到可用的数据文件或文件为空，请先导入。")
            
    except Exception as e:
        st.error(f"计算过程中发生错误: {str(e)}")
        st.exception(e)
