from abc import abstractmethod, ABCMeta
import pandas as pd


class BuyerBase:
    __metaclass__ = ABCMeta

    def __init__(self, df, base_period):
        self.df = df
        self.base_period = base_period

    @abstractmethod
    def buy_or_not(self, cur_date):
        pass


class IdealBuyer(BuyerBase):
    def __init__(self, df, base_period):
        relevant_df = df[[col_name for col_name in df.columns if col_name[-1] == base_period]].drop_duplicates()
        super(IdealBuyer, self).__init__(relevant_df, base_period)
        self.buy_dates = set(self.df.loc[self.df['Close_' + base_period] < self.df['Close_' + base_period].shift(-1),
                                         'Date_' + base_period])

    def buy_or_not(self, cur_date):
        return cur_date in self.buy_dates


class DadBuyer_1(BuyerBase):
    def __init__(self, df, base_period, MA5_p=1):
        df = df[~df.m_neg_m]
        relevant_df = df[
            [col_name for col_name in df.columns if col_name[-1] == base_period] + ['grp_m']].drop_duplicates()
        super(DadBuyer_1, self).__init__(relevant_df, base_period)
        self.MA5_p = MA5_p
        buy_dates = self.df.groupby('grp_m').apply(self.get_buy_in_grp)
        if len(buy_dates.index) > 0:
            self.buy_dates = set(buy_dates['Date_' + self.base_period])
        else:
            self.buy_dates = set()

    def get_buy_in_grp(self, grp):
        cond_1_m0 = (grp['MA_2_' + self.base_period] > grp['MA_5_' + self.base_period])
        cond_1 = cond_1_m0 & (~cond_1_m0.shift().astype(bool))
        cond_2 = (grp['MA_5_' + self.base_period] >= self.MA5_p * grp['MA_5_' + self.base_period].shift())
        cond = cond_1 & cond_2

        if ~(cond.any()):
            return pd.DataFrame() # return None may cause issues for the groupby().apply() action
        return grp[cond]

    def buy_or_not(self, cur_date):
        return cur_date in self.buy_dates
