# -*- coding: utf-8 -*-

from dao.constant import DaoConstant
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.gridspec import GridSpec
import numpy as np


class SingleStockTestRecorder(object):
    def __init__(self, df, base_period='d', initial_fund=DaoConstant.default_init_fund):
        self.n_loss = 0
        self.n_gain = 0
        self.buy_date = []
        self.sell_date = []
        self.profits = []
        self.profits_date = []
        self.df = df
        self.base_period = base_period
        self.init_fund = initial_fund
        self.fund = initial_fund

    def get_profit(self):
        return self.fund - self.init_fund

    def get_profit_ratio(self):
        return (self.fund - self.init_fund) / float(self.init_fund)

    def get_plot(self):
        gs = GridSpec(2, 1, height_ratios=[0.5, 5], left=0.05, right=0.95, bottom=0.15, top=0.975, hspace=0)
        base_period = self.base_period
        relevant_df = self.get_relevant_df()
        fig = plt.figure(figsize=(12, 10))
        ax_close = fig.add_subplot(gs[1, 0])
        ax_profit = fig.add_subplot(gs[0, 0], sharex=ax_close)
        plt.setp(ax_profit.get_xticklabels(), visible=False)

        ax_close.plot_date(relevant_df.index.values, relevant_df['Close_' + base_period].values, fmt='r-',
                           label='Close')
        ax_close.set_ylim([min(0, ax_close.get_ylim()[0]), ax_close.get_ylim()[1]])
        profits_dates = np.array(self.profits_date)
        profits = np.array(self.profits)
        ax_profit.plot_date(profits_dates[profits >= 0], profits[profits >= 0], color='r', ms=8)
        ax_profit.plot_date(profits_dates[profits < 0], profits[profits < 0], color='g', ms=8)
        ax_close.plot_date(self.buy_date, relevant_df['Close_' + base_period].ix[self.buy_date].values,
                           '^', ms=12, label='BUY')
        ax_close.plot_date(self.sell_date, relevant_df['Close_' + base_period].ix[self.sell_date].values,
                           'v', ms=12, mfc='salmon', label='SELL')
        ax_profit.plot_date([relevant_df.index.values[0], relevant_df.index.values[-1]], [0, 0], 'k--')
        for profit_date in profits_dates:
            ax_close.plot_date([profit_date, profit_date], ax_close.get_ylim(), ls='--', color='k', lw=1)
            ax_profit.plot_date([profit_date, profit_date], ax_profit.get_ylim(), ls='--', color='k', lw=1)
        ax_close.legend(loc='upper left')

        return fig, ax_close, ax_profit

    def get_relevant_df(self, base_period=None):

        if not base_period:
            base_period = self.base_period

        return (self.df[[col_name for col_name in self.df.columns if col_name[-1] == base_period]]
                .drop_duplicates().set_index('Date_' + base_period))


class MultiStockTestRecorder(object):
    def __init__(self):
        self.records = []

    def get_profit_stats(self):
        profits = []
        for record in self.records:
            profits.append(record['record'].fund - record['record'].init_fund)
        profit_series = pd.Series(data=profits)
        return profit_series.describe()

    def get_worst_case(self):
        for record in self.records:
            single_record = record['record']
            code = record['code']
            if abs(single_record.get_profit() - self.get_profit_stats().min()) < DaoConstant.float_tolerance:
                return code, single_record
