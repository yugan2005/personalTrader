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
    K = RSV_s.ewm(alpha=1 / float(3), min_periods=0, adjust=False).mean()
    D = K.ewm(alpha=1 / float(3), min_periods=0, adjust=False).mean()
    J = 3 * K - 2 * D
    return K, D, J


def get_dwm_ochlmk(cleaned_daily_df):
    days = cleaned_daily_df.reset_index()[['Date']].set_index('Date', drop=False)
    month_last = days.to_period('M').groupby(level=0).last()
    week_last = days.to_period('W').groupby(level=0).last()
    monthly = cleaned_daily_df.resample('M').agg(
        {'Open': 'first', 'Close': 'last', 'High': 'max', 'Low': 'min'}).dropna()
    monthly.index = month_last['Date']
    weekly = cleaned_daily_df.resample('W').agg(
        {'Open': 'first', 'Close': 'last', 'High': 'max', 'Low': 'min'}).dropna()
    weekly.index = week_last['Date']

    monthly.loc[:, 'MACD'] = MACD(monthly.Close)['MACD']
    weekly.loc[:, 'MACD'] = MACD(weekly.Close)['MACD']
    cleaned_daily_df.loc[:, 'MACD'] = MACD(cleaned_daily_df.Close)['MACD']

    monthly.loc[:, 'K'], monthly.loc[:, 'D'], monthly.loc[:, 'J'] = KDJ(monthly)
    weekly.loc[:, 'K'], weekly.loc[:, 'D'], weekly.loc[:, 'J'] = KDJ(weekly)
    cleaned_daily_df.loc[:, 'K'], cleaned_daily_df.loc[:, 'D'], cleaned_daily_df.loc[:, 'J'] = KDJ(cleaned_daily_df)

    cleaned_daily_df.loc[:, 'Date'] = cleaned_daily_df.index
    weekly.loc[:, 'Date'] = weekly.index
    monthly.loc[:, 'Date'] = monthly.index
    month_to_daily = monthly.reindex(cleaned_daily_df.index, method='ffill')
    week_to_daily = weekly.reindex(cleaned_daily_df.index, method='ffill')

    dwm_ochlmk = (cleaned_daily_df[['Date', 'High', 'Close', 'Open', 'Low', 'MACD', 'K', 'D', 'J']]
                  .join(week_to_daily, lsuffix='_d', rsuffix='_w'))
    dwm_ochlmk = (dwm_ochlmk.join(month_to_daily)
                  .rename(columns=
                          {'High': 'High_m', 'Close': 'Close_m', 'Open': 'Open_m',
                           'Low': 'Low_m', 'MACD': 'MACD_m', 'Date': 'Date_m',
                           'K': 'K_m', 'D': 'D_m', 'J': 'J_m'}))

    dwm_ochlmk = dwm_ochlmk.dropna()

    dwm_ochlmk.loc[:, 'grp_d'] = (dwm_ochlmk.MACD_d * dwm_ochlmk.MACD_d.shift() < 0).cumsum()
    dwm_ochlmk.loc[:, 'm_neg_d'] = (dwm_ochlmk.MACD_d < 0)
    dwm_ochlmk.loc[:, 'grp_w'] = (dwm_ochlmk.MACD_w * dwm_ochlmk.MACD_w.shift() < 0).cumsum()
    dwm_ochlmk.loc[:, 'm_neg_w'] = (dwm_ochlmk.MACD_w < 0)
    dwm_ochlmk.loc[:, 'grp_m'] = (dwm_ochlmk.MACD_m * dwm_ochlmk.MACD_m.shift() < 0).cumsum()
    dwm_ochlmk.loc[:, 'm_neg_m'] = (dwm_ochlmk.MACD_m < 0)
    return dwm_ochlmk


def main():
    import pandas_datareader.data as web
    from dao.datacleaner import clean_yahoo
    timeline = web.DataReader('601099.SS', 'yahoo', start='1990-01-01')
    timeline = clean_yahoo(timeline)
    timeline = timeline.drop('Adj Close', axis=1)
    dwm_ochlmk = get_dwm_ochlmk(timeline)
    print dwm_ochlmk


if __name__ == '__main__':
    main()
