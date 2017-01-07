# -*- coding: utf-8 -*-

import urllib2
import pandas as pd
import requests
from dateutil import parser as date_parser
import sys, os, io

try:
    from dao.Const import Const
    from dao import fromDB
except Exception:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from Const import Const
    from dao import fromDB

from pandas.io import html as pd_html
from contextlib import contextmanager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver


@contextmanager
def wait_for_page_load(driver, timeout=10):
    # I used chrome to get this xpath
    check_ele = driver.find_element_by_xpath('//*[@id="center"]/div[2]/div[2]/div[2]/div')
    check_text = check_ele.text
    if check_text == u'暂无数据':
        old_td = None
    else:
        old_td = driver.find_element_by_xpath(
            '//*[@id="center"]/div[2]/div[2]/div[2]/div[1]/table/tbody/tr[1]/td[2]')
    yield
    # yield nothing, just want keep the current state of old_td
    # when exit the with wait_for_page_load block, the next line
    # make sure that the old_td will be changed, or 'no next page' element showing up
    if old_td:
        WebDriverWait(driver, timeout=timeout).until(EC.staleness_of(old_td))


@contextmanager
def open_driver():
    driver = webdriver.Chrome()  # ChromeDriver need be installed by Homebrew
    driver.implicitly_wait(10)  # This line will cause it to wait upto 10 seconds if an element is not there
    yield driver
    driver.quit()


def get_fhps(code):
    """
    This will return DataFrame in form of ['Date', 'Fh', 'Ps'], sorted by 'Date', but no index
    It can also return None if there is any exception connecting xueqiu.com
    :param code: string without suffix
    :return: DataFrame in form of ['Date', 'Fh', 'Ps'], sorted by 'Date', but no index / None
    """
    xueqiu_name = fromDB.from_code_get_xueqiu_name(code)
    url = Const.xueqiu_fhps_url.format(xueqiu_name)

    try:
        with open_driver() as driver:
            driver.get(url)
            dividend_dfs = []
            while True:
                with wait_for_page_load(driver):
                    check_ele = driver.find_element_by_xpath('//*[@id="center"]/div[2]/div[2]/div[2]/div')
                    check_text = check_ele.text
                    if check_text == u'暂无数据':
                        break
                    table = driver.find_element_by_xpath('//table[@class="dataTable table table-bordered"]')
                    table_html = table.get_attribute('outerHTML')
                    df = pd_html.read_html(table_html, na_values='-')

                    # below is just some normal dataframe munging
                    processed_df = df[0].T
                    processed_df.columns = processed_df.iloc[0, :]
                    processed_df = processed_df.drop(0, axis=0)
                    processed_df = processed_df.reset_index(drop=True)
                    dividend_dfs.append(processed_df)

                    # get the link and click on the link
                    link = driver.find_element_by_link_text(u'下一页')
                    link.click()

        dividend_df = pd.concat(dividend_dfs).drop_duplicates().reset_index(drop=True)
        cleaned_df = dividend_df.iloc[:, [3, 4, 6, 7]].copy()
        cleaned_df.columns = ['Sg', 'Zg', 'Date', 'Fh']
        cleaned_df.loc[:, 'Date'] = pd.to_datetime(cleaned_df.loc[:,'Date'], errors='coerce', infer_datetime_format=True)
        cleaned_df.loc[:, ['Sg', 'Zg', 'Fh']] = cleaned_df.loc[:, ['Sg', 'Zg', 'Fh']].astype(float).fillna(0) / 10.
        cleaned_df.loc[:, 'Ps'] = cleaned_df['Sg'] + cleaned_df['Zg']
        cleaned_df = cleaned_df.sort_values('Date')
        return cleaned_df[['Date', 'Fh', 'Ps']]
    except Exception:
        return None


def get_stock(code, start_date=None, end_date=None):
    """
    xueqiue only provide 'bfq' data
    :param code string without suffix
    :return: dataframe with index of 'Date' and columns of ['Open', 'High', 'Low', 'Close', 'Volume'], or None
    """

    with requests.Session() as s:
        s.headers.update(Const.xueqiu_hdr)
        xueqiu_name = fromDB.from_code_get_xueqiu_name(code)
        r = s.get(Const.xueqiu_stock_url.format(xueqiu_name))
        if r.status_code == 200:
            csv = r.content.decode('utf8')  # the decode is a must
            df = pd.read_csv(io.StringIO(csv), parse_dates=['date'], infer_datetime_format=True)
            df = df.drop('symbol', axis=1)
            df.columns = [name.capitalize() for name in df.columns]
            df = df.set_index('Date').sort_index()
            start_date_xueqiu = df.index[0]
            end_date_xueqiu = df.index[-1]
            if start_date:
                if isinstance(start_date, str):
                    start_date = date_parser.parse(start_date)
                start_date_xueqiu = max(start_date, start_date_xueqiu)
            if end_date:
                if isinstance(end_date, str):
                    end_date = date_parser.parse(end_date)
                end_date_xueqiu = min(end_date, end_date_xueqiu)
            return df[start_date_xueqiu:end_date_xueqiu]
        else:
            return None


def main():
    from dao import fromDB
    stock_info = fromDB.get_all_stock_info().set_index('code')
    info = stock_info.loc['600589']
    print info
    print '---'
    print get_stock(info.get('TDXname'))
    print '---'
    print get_fhps(info.get('TDXname'))


if __name__ == '__main__':
    main()
