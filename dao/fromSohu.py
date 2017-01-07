# -*- coding: utf-8 -*-

import pandas as pd
import sys, os
from dateutil import parser as date_parser

try:
    from dao.Const import Const
    from dao import fromDB
except Exception:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from Const import Const
    from dao import fromDB

from contextlib import contextmanager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.common.by import By


@contextmanager
def open_fast_firefox_driver():
    """
    This uses Firefox WebDriver and block the loading of pictures, It is faster
    Note, no implicit waiting for this driver
    :return:
    """
    firefox_p = webdriver.FirefoxProfile()
    firefox_p.set_preference('permissions.default.image', 2)
    firefox_p.set_preference('network.http.connection-timeout', 5)
    driver = webdriver.Firefox(firefox_profile=firefox_p)
    yield driver
    driver.quit()


@contextmanager
def open_phantomJS_driver():
    """
    This uses phantomJS WebDriver and block the loading of pictures, It is the best
    The selenium server need be started first
    :return:
    """
    capabilities = webdriver.DesiredCapabilities.PHANTOMJS.copy()
    capabilities['phantomjs.page.settings.loadImages'] = False
    capabilities['phantomjs.page.settings.webSecurityEnabled'] = False
    capabilities['phantomjs.page.settings.javascriptCanOpenWindows'] = False
    capabilities['phantomjs.page.settings.javascriptCanCloseWindows'] = False
    capabilities['phantomjs.page.settings.userAgent'] = \
        'Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/538.1 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/538.1'

    driver = webdriver.Remote("http://localhost:4444/wd/hub", capabilities)
    yield driver
    driver.quit()


def get_sohu_name(TDXname):
    sohu_name = TDXname.split('.')[0][3:]
    return sohu_name


def get_fhps(TDXname):
    """
    This will return DataFrame in form of ['Date', 'Fh', 'Ps'], sorted by 'Date', but no index
    It can also return None if there is any exception connecting xueqiu.com
    :param TDXname:
    :return: DataFrame in form of ['Date', 'Fh', 'Ps'], sorted by 'Date', but no index / None
    """
    sohu_name = get_sohu_name(TDXname)
    url = Const.sohu_fhps_url.format(sohu_name)
    table = None
    try:
        with open_phantomJS_driver() as driver:
            import time
            driver.get(url)
            d_table = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.XPATH, Const.sohu_fhps_table_xpath)))
            table = d_table.text
    except Exception:
        return None

    if table:
        line_iter = iter(table.split('\n'))
        records = []
        while True:
            record = dict()
            date_m = None
            try:
                line = line_iter.next()
                tokens = line.strip().split()
                if len(tokens) == 1:
                    continue
                if tokens[0] != u'除权除息日':
                    continue
                if len(tokens) == 2:
                    next_line = line_iter.next()
                    date_m = Const.sohu_date_re.search(next_line)
                if not date_m:
                    date_m = Const.sohu_date_re.search(line)
                zg_m = Const.sohu_zg_re.search(line)
                sg_m = Const.sohu_sg_re.search(line)
                fh_m = Const.sohu_fh_re.search(line)
                record['Date'] = date_parser.parse(date_m.group(1))
                if zg_m:
                    record['Zg'] = float(zg_m.group(1)) / 10.
                if sg_m:
                    record['Sg'] = float(sg_m.group(1)) / 10.
                if fh_m:
                    record['Fh'] = float(fh_m.group(1)) / 10.
                records.append(record)
            except StopIteration:
                break
        fhps_df = pd.DataFrame(records)
        for field in ['Fh', 'Zg', 'Sg']:
            if field not in fhps_df:
                fhps_df.loc[:, field] = 0.0
        fhps_df.loc[:, ['Fh', 'Zg', 'Sg']] = fhps_df.loc[:, ['Fh', 'Zg', 'Sg']].fillna(0.0)
        fhps_df.loc[:, 'Ps'] = fhps_df['Zg'] + fhps_df['Sg']
        fhps_df = fhps_df.sort_values('Date')
        return fhps_df[['Date', 'Fh', 'Ps']]
    else:
        return None


def get_stock(TDXname):
    raise NotImplementedError('Not implemented for sohu')


def main():
    from dao import fromDB
    from dao import seleniumServer

    stock_info = fromDB.get_all_stock_info().set_index('code')
    info = stock_info.loc['600033']
    print info
    print '---'
    with seleniumServer.open_selenium_server():
        print get_fhps(info.get('TDXname'))


if __name__ == '__main__':
    main()
