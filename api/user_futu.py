from .utils import RateLimiter, parse_option_expiry_from_symbol
from futu import *
import pandas as pd
import os
from datetime import datetime, timedelta
import time
from futu import *
from .trade_type import Stock
from .utils import safe_read_csv
from .cost_methods import create_stock, COST_METHODS

MAX_REQUESTS = 20
TIME_WINDOW = 35


def remove_repeated_fee(df):
    df.sort_values(by=['order_id', 'create_time'],
                   inplace=True, ignore_index=True)
    mask = df.groupby('order_id').cumcount() > 0
    df.loc[mask, 'fee_amount'] = 0
    return df


def get_cash_flow(output_path, start_date, end_date):
    trd_ctx = OpenSecTradeContext(filter_trdmarket=TrdMarket.NONE, host='127.0.0.1',
                                  port=11111, security_firm=SecurityFirm.FUTUSECURITIES)

    all_cash_flow = []
    rate_limiter = RateLimiter(MAX_REQUESTS, TIME_WINDOW)
    try:
        ret, acc_list_df = trd_ctx.get_acc_list()
        if ret != RET_OK or not isinstance(acc_list_df, pd.DataFrame):
            print(f'获取账户列表失败: {acc_list_df}')
            exit(1)

        date_list = []
        d = start_date
        while d <= end_date:
            date_list.append(d.strftime('%Y-%m-%d'))
            d += timedelta(days=1)

        for _, acc_row in acc_list_df.iterrows():
            acc_id = acc_row.get('acc_id')
            if acc_row.get('trd_env') == TrdEnv.SIMULATE:
                continue
            if acc_id is None:
                continue
            try:
                acc_id = int(acc_id)
            except Exception:
                print(f"无效的账户ID: {acc_id}")
                continue
            print(f"处理账户: {acc_id}")
            for clearing_date in date_list:
                print(f"查询日期: {clearing_date}")
                rate_limiter.wait_if_needed()
                ret, data = trd_ctx.get_acc_cash_flow(
                    clearing_date=clearing_date,
                    trd_env=TrdEnv.REAL,
                    acc_id=acc_id,
                    cashflow_direction=CashFlowDirection.NONE
                )
                if ret == RET_OK:
                    data['acc_id'] = acc_id
                    all_cash_flow.append(data)
                else:
                    print(f"获取现金流水失败: {data}")

        if not all_cash_flow:
            print("所有账户都未获取到现金流水")
        else:
            final_df = pd.concat(all_cash_flow, ignore_index=True)
            if len(final_df) > 0:
                final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
                print(f"已导出到 {output_path}")
    finally:
        trd_ctx.close()


def extract_other_fees(path):
    df = safe_read_csv(path)
    df["cashflow_remark"] = df["cashflow_remark"].fillna("").str.upper()

    fee_keywords = [
        "FEE", "ADR", "INTEREST", "LOAN", "STAMP", "WITHHOLDING TAX", "TAX",
        "IRO", "REGISTRATION", "DIVIDENDS"
    ]

    mask_fee = df["cashflow_remark"].str.contains(
        "|".join(fee_keywords), na=False)

    fees = df[mask_fee]

    return fees


def get_trade_flow(output_path, start_date, end_date):
    quote_ctx = OpenQuoteContext(host='127.0.0.1', port=11111)
    trade_ctx = OpenSecTradeContext(
        host='127.0.0.1', port=11111, filter_trdmarket=TrdMarket.NONE)

    rate_limiter = RateLimiter(max_requests=10, time_window=50)
    all_accounts_orders = []
    markets_to_query = [TrdMarket.NONE]

    try:
        ret, acc_list_df = trade_ctx.get_acc_list()
        if ret != RET_OK or not isinstance(acc_list_df, pd.DataFrame):
            print(f'获取账户列表失败: {acc_list_df}')
            return

        for _, acc_row in acc_list_df.iterrows():
            acc_id = acc_row.get('acc_id')
            if acc_row.get("trd_env") == TrdEnv.SIMULATE:
                continue
            if acc_id is None:
                continue

            try:
                acc_id = int(acc_id)
            except (ValueError, TypeError):
                print(f"无效的账户ID: {acc_id}")
                continue

            print(f"\n开始处理账户: {acc_id}")

            for market in markets_to_query:
                current_start = start_date

                while current_start < end_date:
                    current_end = min(
                        current_start + timedelta(days=90), end_date)

                    print(
                        f"正在获取 {current_start.strftime('%Y-%m-%d')} 到 {current_end.strftime('%Y-%m-%d')} 的订单数据...")

                    rate_limiter.wait_if_needed()

                    ret, data = trade_ctx.history_deal_list_query(
                        acc_id=acc_id,
                        deal_market=market,
                        start=current_start.strftime('%Y-%m-%d %H:%M:%S'),
                        end=current_end.strftime('%Y-%m-%d %H:%M:%S'),
                    )

                    if ret != RET_OK:
                        print(f'获取历史订单失败: {data}')

                    if isinstance(data, pd.DataFrame) and not data.empty:
                        data['acc_id'] = acc_id
                        all_accounts_orders.append(data)
                        print(f"成功获取 {len(data)} 条订单记录")
                    elif data is not None:
                        try:
                            data_df = pd.DataFrame(data)
                            if not data_df.empty:
                                data_df['acc_id'] = acc_id
                                all_accounts_orders.append(data_df)
                                print(
                                    f"成功获取 {len(data_df)} 条订单记录 (非DataFrame原始类型)")
                        except Exception as e:
                            print(f"数据无法转为DataFrame: {e}")

                    current_start = current_end

        if not all_accounts_orders:
            print("所有账户和市场都未找到任何订单记录")
            return

        final_df = pd.concat(all_accounts_orders, ignore_index=True)

        if 'create_time' in final_df.columns:
            final_df = final_df.sort_values(
                by='create_time', ascending=False, kind='stable')

        if 'order_id' in final_df.columns and 'acc_id' in final_df.columns:
            fee_list = []
            batch_size = 400
            for acc_id_val, group in final_df.groupby('acc_id'):
                if not isinstance(acc_id_val, (int, str)):
                    print(f'不支持的acc_id类型: {type(acc_id_val)}, 跳过该分组')
                    continue
                try:
                    acc_id_int = int(str(acc_id_val))
                except Exception:
                    print(f'无法转换acc_id: {acc_id_val}，跳过该分组')
                    continue
                order_ids = group['order_id'].tolist()
                for i in range(0, len(order_ids), batch_size):
                    batch_ids = order_ids[i:i+batch_size]
                    ret, fee_df = trade_ctx.order_fee_query(
                        order_id_list=batch_ids, acc_id=acc_id_int, trd_env=TrdEnv.REAL)
                    if ret == RET_OK and isinstance(fee_df, pd.DataFrame):
                        fee_list.append(fee_df[['order_id', 'fee_amount']])
                    else:
                        print(f'acc_id={acc_id_int} 获取订单费用失败:', fee_df)
            if fee_list:
                all_fee_df = pd.concat(fee_list, ignore_index=True)
            else:
                all_fee_df = pd.DataFrame(columns=['order_id', 'fee_amount'])
            final_df = final_df.merge(all_fee_df, on='order_id', how='left')
        else:
            final_df['fee_amount'] = 0

        print(final_df)
        final_df = remove_repeated_fee(final_df)

        if len(final_df) > 0:
            final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            print(f"\n所有账户数据已合并保存到 {output_path}")

    finally:
        quote_ctx.close()
        trade_ctx.close()


def format_trade(data_path, cash_path=None, check_expiry=True, check_date=None, cost_method="AVERAGE"):
    """
    格式化交易数据并计算已实现收益
    
    Args:
        data_path: 交易数据文件路径
        cash_path: 现金流水文件路径
        check_expiry: 是否检查期权过期
        check_date: 检查日期
        cost_method: 成本核算方法，可选值:
            - AVERAGE: 移动加权平均法（默认）
            - FIFO: 先进先出法
            - LIFO: 后进先出法
            - SPECIFIC: 个别计价法
            - HIFO: 最高成本优先法
            - LOFO: 最低成本优先法
    
    Returns:
        dict: {symbol: Stock对象}
    """
    data = (
        safe_read_csv(data_path)
        .assign(updated_time=lambda df: pd.to_datetime(df["create_time"], errors="coerce"))
        .sort_values(by="updated_time", ascending=True)
    )
    data = data[data["qty"] > 0]

    pool = {}

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
        symbol = row["code"]
        expiry_date, is_option = parse_option_expiry_from_symbol(symbol)
        shares = 1 if not is_option else contract_multiplier[row["deal_market"]]
        if symbol not in pool:
            pool[symbol] = create_stock_instance(symbol, market2currency[row["deal_market"]])
        if "SELL" in row["trd_side"]:
            pool[symbol].sell(row["price"], row["qty"],
                              row["fee_amount"], row["updated_time"], shares)
        else:
            pool[symbol].buy(row["price"], row["qty"],
                             row["fee_amount"], row["updated_time"], shares)

    if check_expiry:
        if check_date is None:
            check_date = datetime.now()

        expired_count = 0
        for symbol, stock_obj in pool.items():
            expiry_date, is_option = parse_option_expiry_from_symbol(symbol)
            print(expiry_date, check_date)

            if is_option and expiry_date and stock_obj.qty != 0:
                if check_date >= expiry_date:
                    stock_obj.expire_option(expiry_date, expiry_date)
                    expired_count += 1

        if expired_count > 0:
            print(f"已处理 {expired_count} 个过期期权")

    if cash_path is not None:
        fees = extract_other_fees(cash_path)

        fees = fees.assign(
            ts=lambda df: pd.to_datetime(df["clearing_date"], errors="coerce")
        ).sort_values("ts")

        for _, r in fees.iterrows():
            currency = r.get("currency", "").upper()
            if not currency:
                continue

            fee_symbol = f"FEE-{currency}"

            if fee_symbol not in pool:
                pool[fee_symbol] = create_stock_instance(fee_symbol, currency)

            amount = float(r["cashflow_amount"])

            pool[fee_symbol].add_fee(-amount, r["ts"])

    return pool
