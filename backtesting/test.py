from dao.Const import Const
from dao import getdata as gd
import numpy as np
import pandas as pd


def single_stock_maxsize(df, buyer_class, seller_class, base_period='d', initial_fund=Const.default_init_fund, **kwargs):
    has_buy = False
    buy_price = 0.0
    buy_size = 0
    fund = initial_fund
    n_loss = 0
    n_gain = 0
    buy_date = []
    sell_date = []
    profits = []
    profits_date = []

    buyer = buyer_class(df, base_period, **kwargs)
    seller = seller_class(df, base_period)
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
                fund -= buy_price * buy_size
                buy_date.append(row['Date_' + base_period])
        else:
            should_sell = seller.sell_or_not(cur_date)
            if should_sell or (cur_date == df_index_date.ix[-1, 'Date_' + base_period]):
                has_buy = False
                sell_price = row['Close_' + base_period]
                sell_size = buy_size
                buy_size = 0
                fund += sell_price * sell_size
                profit = sell_size * (sell_price - buy_price)
                buy_price = 0
                sell_date.append(row['Date_' + base_period])
                profits.append(profit)
                profits_date.append(row['Date_' + base_period])
                if profit > 0:
                    n_gain += 1
                else:
                    n_loss += 1

    return fund


def test_all(buyer_class, seller_class, base_period='d', test_ratio=1, initial_fund=Const.default_init_fund, **kwargs):
    profits = []
    cnt = 0
    ncodes = gd.get_symbols()
    ncodes = np.random.permutation(ncodes)
    tot_cnt = len(ncodes)
    test_cnt = int(tot_cnt * test_ratio)
    for ncode in ncodes[:test_cnt]:
        cnt += 1
        if cnt % 10 == 0:
            print 'Finished {0:.2f}%'.format(float(cnt) / test_cnt * 100)
        try:
            df = gd.hist_by_num(ncode)
        except Exception:
            print '!!database corrupted!! check {}'.format(ncode)
            continue
        final_fund = single_stock_maxsize(df, buyer_class, seller_class, base_period, initial_fund, **kwargs)
        profit = dict()
        profit['ncode'] = ncode
        profit['profit'] = final_fund - initial_fund
        profits.append(profit)

    return pd.DataFrame(profits)


def main():
    from strategies.Buyer import DadBuyer_1
    from strategies.Seller import DadSeller_1
    # df = gd.hist_by_num('600839.SS')
    # print single_stock_maxsize(df, DadBuyer_1, DadSeller_1, base_period='w')
    profits = test_all(DadBuyer_1, DadSeller_1, base_period='w', test_ratio=0.1, MA5_p=0.99)
    print profits.describe()


if __name__ == '__main__':
    main()
