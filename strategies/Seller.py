from abc import abstractmethod, ABCMeta


class SellerBase:
    __metaclass__ = ABCMeta

    def __init__(self, df, base_period):
        self.df = df
        self.base_period = base_period

    @abstractmethod
    def sell_or_not(self, cur_date):
        pass


class IdealSeller(SellerBase):
    def __init__(self, df, base_period):
        relevant_df = df[[col_name for col_name in df.columns if col_name[-1] == base_period]].drop_duplicates()
        super(IdealSeller, self).__init__(relevant_df, base_period)
        self.sell_dates = set(self.df.loc[self.df['Close_' + base_period] > self.df['Close_' + base_period].shift(-1),
                                          'Date_' + base_period])

    def sell_or_not(self, cur_date):
        return cur_date in self.sell_dates


class DadSeller_1(SellerBase):
    def __init__(self, df, base_period):
        relevant_df = df[[col_name for col_name in df.columns if col_name[-1] == base_period]].drop_duplicates()
        super(DadSeller_1, self).__init__(relevant_df, base_period)
        self.sell_dates = set(self.df.loc[self.df['MA_2_' + self.base_period] < self.df['MA_5_' + self.base_period],
                                          'Date_' + base_period])

    def sell_or_not(self, cur_date):
        return cur_date in self.sell_dates
