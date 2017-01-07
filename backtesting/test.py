# -*- coding: utf-8 -*-
import sys, os

try:
    from backtesting.recorder import SingleStockTestRecorder, MultiStockTestRecorder
    from dao.Const import Const
    from dao import fromDB
except Exception:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from backtesting.recorder import SingleStockTestRecorder, MultiStockTestRecorder
    from dao.Const import Const
    from dao import fromDB

import numpy as np
from matplotlib import pyplot as plt


def single_stock_maxsize(df, buyer_class, seller_class,
                         base_period='d', initial_fund=Const.default_init_fund, **kwargs):
    has_buy = False
    buy_price = 0.0
    buy_size = 0

    buyer = buyer_class(df, base_period, **kwargs)
    seller = seller_class(df, base_period)
    recorder = SingleStockTestRecorder(df, base_period, initial_fund)
    relevant_df = df[[col_name for col_name in df.columns if col_name[-1] == base_period]].drop_duplicates()
    df_index_date = relevant_df.set_index('Date_' + base_period, drop=False)

    for cur_date, row in df_index_date.iterrows():
        if not has_buy:
            should_buy = buyer.buy_or_not(cur_date)
            if should_buy:
                has_buy = True
                buy_price = row['Close_' + base_period]
                # buy_size = int(initial_fund / buy_price)
                buy_size = 5
                recorder.fund -= buy_price * buy_size
                recorder.buy_date.append(row['Date_' + base_period])

        if has_buy:
            should_sell = seller.sell_or_not(cur_date)
            if should_sell or (cur_date == df_index_date.ix[-1, 'Date_' + base_period]):
                has_buy = False
                sell_price = row['Close_' + base_period]
                sell_size = buy_size
                buy_size = 0
                recorder.fund += sell_price * sell_size
                profit = sell_size * (sell_price - buy_price)
                buy_price = 0
                recorder.sell_date.append(row['Date_' + base_period])
                recorder.profits.append(profit)
                recorder.profits_date.append(row['Date_' + base_period])
                if profit > 0:
                    recorder.n_gain += 1
                else:
                    recorder.n_loss += 1

    return recorder


def test_all(buyer_class, seller_class,
             fq='qfq', base_period='d', test_ratio=1.0,
             initial_fund=Const.default_init_fund, **kwargs):
    multi_records = MultiStockTestRecorder()
    cnt = 0
    stock_info = fromDB.get_all_stock_info()
    codes = stock_info.code.values
    codes = np.random.permutation(codes)
    tot_cnt = len(codes)
    test_cnt = int(tot_cnt * test_ratio)
    for code in codes[:test_cnt]:
        cnt += 1
        if cnt % 10 == 0:
            print 'Finished {0:.2f}%'.format(float(cnt) / test_cnt * 100)
        try:
            df = fromDB.get_stock(code, fq)
        except Exception:
            print '!!database corrupted!! check {}_{}'.format(fq, code)
            continue
        single_record = single_stock_maxsize(df, buyer_class, seller_class, base_period, initial_fund, **kwargs)
        record = dict()
        record['code'] = code
        record['record'] = single_record
        multi_records.records.append(record)

    return multi_records


def Dad_strategy_1_plot(record_code, record):
    fig, ax_close, ax_profit = record.get_plot()
    base_period = record.base_period
    relevant_df = record.get_relevant_df()
    ax_close.plot_date(relevant_df.index.values, relevant_df['MA_2_' + base_period].values, fmt='b--', lw=2,
                       label='MA_2')
    ax_close.plot_date(relevant_df.index.values, relevant_df['MA_5_' + base_period].values, fmt='m:', lw=2,
                       label='MA_5')
    ax_close.legend(loc='upper left')
    ax_close_pos = ax_close.get_position()
    ax_macd_pos = [ax_close_pos.x0, ax_close_pos.y0, ax_close_pos.width, ax_close_pos.height / 10.0]
    ax_macd = fig.add_axes(ax_macd_pos, frameon=True, sharex=ax_close)
    macd_df = (record.df[[col_name for col_name in record.df.columns
                          if col_name[-1] == 'm']]
               .drop_duplicates()
               .set_index('Date_m'))
    ax_macd.bar(macd_df[macd_df.MACD_m > 0].index.values,
                macd_df[macd_df.MACD_m > 0].MACD_m.values,
                width=15, color='salmon')
    ax_macd.bar(macd_df[macd_df.MACD_m < 0].index.values,
                macd_df[macd_df.MACD_m < 0].MACD_m.values,
                width=15, color='steelblue')
    ax_macd.grid(True)
    plt.title('股票：{0}， 总盈利： {1:.2f}'.format(record_code.decode('utf8'), record.get_profit()).decode('utf8'))
    plt.ioff()
    plt.show()


def main():
    from strategies.Buyer import DadBuyer_1
    from strategies.Seller import DadSeller_1

    # worst_code = '002411'
    # df = fromDB.get_stock(worst_code, 'qfq')
    # worst_record = single_stock_maxsize(df, DadBuyer_1, DadSeller_1, base_period='w')

    records = test_all(DadBuyer_1, DadSeller_1, base_period='d', test_ratio=0.01, MA5_p=0.99)
    print records.get_profit_stats()

    worst_code, worst_record = records.get_worst_case()

    Dad_strategy_1_plot(worst_code, worst_record)


if __name__ == '__main__':
    main()
