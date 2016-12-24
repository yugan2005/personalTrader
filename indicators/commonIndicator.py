import pandas as pd


def MACD(df, slow_period=26, fast_period=12, dif_period=9):
    slow_EMA = df.ewm(span=slow_period, min_periods=0, adjust=False).mean()
    fast_EMA = df.ewm(span=fast_period, min_periods=0, adjust=False).mean()
    dif = fast_EMA - slow_EMA
    dea = dif.ewm(span=dif_period, min_periods=0, adjust=False).mean()
    macd = (dif - dea) * 2
    return macd.to_frame(name='MACD')


def KDJ(df, N=9, M1=3, M2=3):
    RSV = ((df.Close - df.Low.rolling(N, min_periods=0).min())
           / (df.High.rolling(N, min_periods=0).max() - df.Low.rolling(N, min_periods=0).min()) * 100)
    RSV_s = pd.Series(RSV, index=df.index)
    K = RSV_s.ewm(alpha=1 / float(M1), min_periods=0, adjust=False).mean()
    D = K.ewm(alpha=1 / float(M2), min_periods=0, adjust=False).mean()
    J = 3 * K - 2 * D
    return K, D, J


def MA(df, window_size):
    return (df
            .rolling(window_size, min_periods=0)
            .mean()
            .to_frame(name='MA_' + str(window_size)))


def get_dwm_ochlmk(tdx_df):
    daily = tdx_df.copy()
    days = daily.reset_index()[['Date']].set_index('Date', drop=False)
    month_last = days.to_period('M').groupby(level=0).last()
    week_last = days.to_period('W').groupby(level=0).last()
    monthly = daily.resample('M').agg(
        {'Open': 'first', 'Close': 'last', 'High': 'max', 'Low': 'min', 'Volume': 'sum', 'Amount': 'sum'}).dropna()
    monthly.index = month_last['Date']
    weekly = daily.resample('W').agg(
        {'Open': 'first', 'Close': 'last', 'High': 'max', 'Low': 'min', 'Volume': 'sum', 'Amount': 'sum'}).dropna()
    weekly.index = week_last['Date']

    monthly.loc[:, 'MACD'] = MACD(monthly.Close)['MACD']
    monthly.loc[:, 'MA_5'] = MA(monthly.Close, 5)['MA_5']
    monthly.loc[:, 'MA_2'] = MA(monthly.Close, 2)['MA_2']
    weekly.loc[:, 'MACD'] = MACD(weekly.Close)['MACD']
    weekly.loc[:, 'MA_5'] = MA(weekly.Close, 5)['MA_5']
    weekly.loc[:, 'MA_2'] = MA(weekly.Close, 2)['MA_2']
    daily.loc[:, 'MACD'] = MACD(daily.Close)['MACD']
    daily.loc[:, 'MA_5'] = MA(daily.Close, 5)['MA_5']
    daily.loc[:, 'MA_2'] = MA(daily.Close, 2)['MA_2']

    monthly.loc[:, 'K'], monthly.loc[:, 'D'], monthly.loc[:, 'J'] = KDJ(monthly)
    weekly.loc[:, 'K'], weekly.loc[:, 'D'], weekly.loc[:, 'J'] = KDJ(weekly)
    daily.loc[:, 'K'], daily.loc[:, 'D'], daily.loc[:, 'J'] = KDJ(daily)

    daily.loc[:, 'Date'] = daily.index
    weekly.loc[:, 'Date'] = weekly.index
    monthly.loc[:, 'Date'] = monthly.index
    month_to_daily = monthly.reindex(daily.index, method='ffill')
    week_to_daily = weekly.reindex(daily.index, method='ffill')

    dwm_ochl_macd_kdj = daily.join(week_to_daily, lsuffix='_d', rsuffix='_w')
    month_to_daily.columns = month_to_daily.columns.map(lambda name: name+'_m')
    dwm_ochl_macd_kdj = dwm_ochl_macd_kdj.join(month_to_daily)
    dwm_ochl_macd_kdj = dwm_ochl_macd_kdj.dropna()

    dwm_ochl_macd_kdj.loc[:, 'grp_d'] = (dwm_ochl_macd_kdj.MACD_d * dwm_ochl_macd_kdj.MACD_d.shift() < 0).cumsum()
    dwm_ochl_macd_kdj.loc[:, 'm_neg_d'] = (dwm_ochl_macd_kdj.MACD_d < 0)
    dwm_ochl_macd_kdj.loc[:, 'grp_w'] = (dwm_ochl_macd_kdj.MACD_w * dwm_ochl_macd_kdj.MACD_w.shift() < 0).cumsum()
    dwm_ochl_macd_kdj.loc[:, 'm_neg_w'] = (dwm_ochl_macd_kdj.MACD_w < 0)
    dwm_ochl_macd_kdj.loc[:, 'grp_m'] = (dwm_ochl_macd_kdj.MACD_m * dwm_ochl_macd_kdj.MACD_m.shift() < 0).cumsum()
    dwm_ochl_macd_kdj.loc[:, 'm_neg_m'] = (dwm_ochl_macd_kdj.MACD_m < 0)
    return dwm_ochl_macd_kdj



def main():
    from dao import fromTDX
    stock_info = fromTDX.get_stock_info()
    tdx_df = fromTDX.get_stock(stock_info.loc['600033', 'TDXname'], 'bfq')
    df = get_dwm_ochlmk(tdx_df)
    print df.head()


if __name__ == '__main__':
    main()
