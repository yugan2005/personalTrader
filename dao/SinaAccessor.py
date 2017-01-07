# -*- coding: utf-8 -*-
import json
import requests
import pandas as pd
import numpy as np
import re
import sys, os

from StringIO import StringIO
from dateutil import parser as date_parser
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

try:
    from Dao import Accessor
    from Const import Const
    from DatabaseAccessor import DatabaseAccessor
    import dao_util
except Exception:
    sys.path.append(os.path.join(os.path.dirname(__file__)))
    from Dao import Accessor
    from Const import Const
    from DatabaseAccessor import DatabaseAccessor
    import dao_util


class SinaAccessor(Accessor):
    def __init__(self):
        self.hq_full_baseURL = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/{}.phtml?year={}&jidu={}'
        self.hq_fast_baseURL = 'http://biz.finance.sina.com.cn/stock/flash_hq/kline_data.php?symbol={}&begin_date={:%Y%m%d}&end_date={:%Y%m%d}'
        self.hq_table_xpath = '//*[@id="FundHoldSharesTable"]'
        self.hq_year_list_xpath = '//*[@id="con02-4"]/table/tbody/tr/td/form/select[1]'
        self.data_source = 'sina'
        self.fhps_baseURL = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vISSUE_ShareBonus/stockid/{}.phtml'
        self.fhps_table_xpath = '//*[@id="sharebonus_1"]'

    def get_fhps(self, code, start_date=None, end_date=None, retry_times=3):
        i_start, i_end, e_start, e_end = dao_util.get_dates(code, self.data_source, start_date, end_date)
        e_df = None
        i_df = None
        url = self.fhps_baseURL.format(code)
        retry = 0

        if e_start:
            while retry < retry_times:
                with dao_util.open_phantomJS_driver() as driver:
                    try:
                        driver.get(url)
                        d_table = WebDriverWait(driver, 30).until(
                            EC.presence_of_element_located((By.XPATH, self.fhps_table_xpath)))
                        e_df = pd.read_html(d_table.get_attribute('outerHTML'))
                        retry = retry_times + 1
                    except Exception:
                        retry += 1
            e_df = e_df[0].iloc[:, [1, 2, 3, 5]]
            e_df.columns = ['Sg', 'Zg', 'Fh', 'Date']
            e_df[['Sg', 'Zg', 'Fh']] = e_df[['Sg', 'Zg', 'Fh']].astype(float).fillna(0) / 10
            e_df['Date'] = pd.to_datetime(e_df['Date'], errors='coerce')
            e_df.loc[:, 'Ps'] = e_df['Sg'] + e_df['Zg']
            e_df = e_df[Const.fhps_col_names].set_index('Date', drop=False).sort_index()
            e_df = e_df[e_start:e_end]

        if i_end:
            db_accessor = DatabaseAccessor()
            i_df = db_accessor.get_fhps(code, i_start, i_end)

        df = pd.concat([i_df, e_df]).sort_index()
        return df

    def get_hq_full(self, code, start_date=None, end_date=None, retry_times=3):
        i_start, i_end, e_start, e_end = dao_util.get_dates(code, self.data_source, start_date, end_date)
        e_df = None
        i_df = None
        retry = 0
        available_year_list = []

        if e_start:
            while retry < retry_times:
                with dao_util.open_phantomJS_driver() as driver:
                    try:
                        e_df_list = []
                        year_start = e_start.year
                        year_end = e_end.year
                        jidu_start = (e_start.month - 1) / 3 + 1
                        jidu_end = (e_end.month - 1) / 3 + 1
                        year = year_end + 1
                        while year > year_start:
                            year -= 1
                            for jidu in range(4, 0, -1):
                                if year > year_end or (year == year_end and jidu > jidu_end):
                                    continue
                                if year < year_start or (year == year_start and jidu < jidu_start):
                                    continue
                                url = self.hq_full_baseURL.format(code, year, jidu)
                                driver.get(url)
                                if not available_year_list:
                                    # need find the available years first
                                    year_list_option = WebDriverWait(driver, 30).until(
                                        EC.presence_of_element_located((By.XPATH, self.hq_year_list_xpath)))
                                    soup = BeautifulSoup(year_list_option.get_attribute('innerHTML'))
                                    for available_year in soup.find_all(name='option'):
                                        available_year_list.append(int(available_year.text))
                                    if year_start < min(available_year_list):
                                        year_start = min(available_year_list)
                                        jidu_start = 1
                                    if year_end > max(available_year_list):
                                        year_end = max(available_year_list)
                                        jidu_end = 4
                                        continue
                                d_table = WebDriverWait(driver, 30).until(
                                    EC.presence_of_element_located((By.XPATH, self.hq_table_xpath)))
                                cur_e_df = pd.read_html(d_table.get_attribute('outerHTML'), header=1)[0]
                                cur_e_df.columns = Const.hq_col_names
                                cur_e_df['Date'] = pd.to_datetime(cur_e_df['Date'], errors='coerce')
                                for i in range(1, len(cur_e_df.columns)):
                                    cur_e_df.iloc[:, i] = cur_e_df.iloc[:, i].astype(Const.hq_datatypes[i])
                                cur_e_df = cur_e_df.set_index('Date', drop=False).sort_index()
                                e_df_list.append(cur_e_df)
                        retry = retry_times + 1
                    except Exception:
                        retry += 1
            e_df = pd.concat(e_df_list).sort_index()
            e_df = dao_util.clean_hq_df(e_df)
            e_df = e_df[e_start:e_end]

        if i_end:
            db_accessor = DatabaseAccessor()
            i_df = db_accessor.get_fhps(code, i_start, i_end)

        df = pd.concat([i_df, e_df]).sort_index()
        return df

    def get_hq(self, code, start_date=None, end_date=None, retry_times=3):
        i_start, i_end, e_start, e_end = dao_util.get_dates(code, self.data_source, start_date, end_date)
        e_df = None
        i_df = None
        retry = 0
        code = Const.code_map.get(code[0])+code

        if e_start:
            while retry < retry_times:
                url = self.hq_fast_baseURL.format(code, e_start, e_end)
                try:
                    r = requests.get(url)
                    if r.status_code == 200:
                        page = r.content
                        soup = BeautifulSoup(page)
                        record_list = []
                        for el in soup.find_all(name='content'):
                            hq_cur = dict()
                            hq_cur['Close'] = float(el.attrs['c'])
                            hq_cur['Open'] = float(el.attrs['o'])
                            hq_cur['Volume'] = int(el.attrs['v'])
                            hq_cur['High'] = float(el.attrs['h'])
                            hq_cur['Low'] = float(el.attrs['l'])
                            hq_cur['Date'] = date_parser.parse(el.attrs['d'])
                            record_list.append(hq_cur)
                        e_df = pd.DataFrame(record_list)
                        if len(e_df) != 0:
                            retry = retry_times + 1
                        e_df['Amount'] = np.NaN
                        e_df = e_df[Const.hq_col_names]
                        e_df = e_df.set_index('Date', drop=False).sort_index()
                        e_df = dao_util.clean_hq_df(e_df)
                        e_df = e_df[e_start:e_end]
                except Exception:
                    retry += 1

        if i_end:
            db_accessor = DatabaseAccessor()
            i_df = db_accessor.get_hq(code, i_start, i_end)

        df = pd.concat([i_df, e_df]).sort_index()
        return df


def main():
    sinaAccessor = SinaAccessor()
    code = '600033'
    start_date = '2014-01-12'
    end_date = '2016-01-12'
    print sinaAccessor.get_hq(code, start_date, end_date)
    print '---'
    print sinaAccessor.get_fhps(code, start_date, end_date)


if __name__ == '__main__':
    main()
