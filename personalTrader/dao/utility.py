# -*- coding: utf-8 -*-
# pinyin to chinese code is from http://wangwei007.blog.51cto.com/68019/983289


import subprocess
import time

from contextlib import contextmanager
from selenium import webdriver
from dateutil import parser as date_parser
from datetime import date
from datetime import timedelta

from DatabaseAccessor import DatabaseAccessor
import constant


def multi_get_first(str_input):
    if isinstance(str_input, unicode):
        unicode_str = str_input
    else:
        try:
            unicode_str = str_input.decode('utf8')
        except:
            try:
                unicode_str = str_input.decode('gbk')
            except:
                print 'unknown coding'
                return
    return_list = []
    for one_unicode in unicode_str:
        return_list.append(single_get_first(one_unicode))
    return ''.join(return_list)


def single_get_first(unicode1):
    str1 = unicode1.encode('gbk')
    try:
        ord(str1)
        return str1
    except:
        asc = ord(str1[0]) * 256 + ord(str1[1]) - 65536
        if asc >= -20319 and asc <= -20284:
            return 'a'
        if asc >= -20283 and asc <= -19776:
            return 'b'
        if asc >= -19775 and asc <= -19219:
            return 'c'
        if asc >= -19218 and asc <= -18711:
            return 'd'
        if asc >= -18710 and asc <= -18527:
            return 'e'
        if asc >= -18526 and asc <= -18240:
            return 'f'
        if asc >= -18239 and asc <= -17923:
            return 'g'
        if asc >= -17922 and asc <= -17418:
            return 'h'
        if asc >= -17417 and asc <= -16475:
            return 'j'
        if asc >= -16474 and asc <= -16213:
            return 'k'
        if asc >= -16212 and asc <= -15641:
            return 'l'
        if asc >= -15640 and asc <= -15166:
            return 'm'
        if asc >= -15165 and asc <= -14923:
            return 'n'
        if asc >= -14922 and asc <= -14915:
            return 'o'
        if asc >= -14914 and asc <= -14631:
            return 'p'
        if asc >= -14630 and asc <= -14150:
            return 'q'
        if asc >= -14149 and asc <= -14091:
            return 'r'
        if asc >= -14090 and asc <= -13119:
            return 's'
        if asc >= -13118 and asc <= -12839:
            return 't'
        if asc >= -12838 and asc <= -12557:
            return 'w'
        if asc >= -12556 and asc <= -11848:
            return 'x'
        if asc >= -11847 and asc <= -11056:
            return 'y'
        if asc >= -11055 and asc <= -10247:
            return 'z'
        return ''


def clean_hq_df(df, inplace=False):
    """
    This method is used to clean the hq dataframe
    :param df: dataframe indexed by 'Date' (sorted), with columns ['Date', 'Open', 'High', 'Close', 'Low', 'Volume', 'Amount'].
    :return: cleaned dataframe
    """
    if inplace:
        cleaned_df = df
    else:
        cleaned_df = df.copy()

    cleaned_df = cleaned_df[cleaned_df.Volume != 0]
    cleaned_df = cleaned_df[cleaned_df.Volume != cleaned_df.Volume.shift()]
    return cleaned_df


@contextmanager
def open_phantomJS_driver():
    start_selenium_server_if_not()
    capabilities = webdriver.DesiredCapabilities.PHANTOMJS.copy()
    capabilities['phantomjs.page.settings.loadImages'] = False
    capabilities['phantomjs.page.settings.webSecurityEnabled'] = False
    capabilities['phantomjs.page.settings.javascriptCanOpenWindows'] = False
    capabilities['phantomjs.page.settings.javascriptCanCloseWindows'] = False
    capabilities[
        'phantomjs.page.settings.userAgent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/538.1 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/538.1'
    capabilities['phantomjs.page.settings.resourceTimeout'] = 500
    driver = webdriver.Remote("http://localhost:4444/wd/hub", capabilities)
    yield driver
    driver.quit()


def start_selenium_server_if_not():
    running = subprocess.check_output('ps -ef | grep selenium | grep -v grep | wc -l', shell=True).strip()
    if running == '0':
        null_io = open('/dev/null')
        server_proc = subprocess.Popen(['selenium-server', '-port', '4444', '-log', 'selenium_server_log.log'],
                                       stdout=null_io, stderr=null_io)
        time.sleep(1)


def get_dates(code, accessor_name, start_date, end_date):
    """
    this method split the date range into two part, internal_start, internal_end, external_start, external_end,
    in order to reduce the dates need be requested from external data source.
    :param code: string number code without pre/suffix, e.g. '600033'
    :param accessor_name: string for the external data source, e.g. 'sohu', 'sina' etc
    :param start_date: string '%Y-%m-%d' e.g. '2017-01-04'
    :param end_date: string '%Y-%m-%d' e.g. '2017-01-04'
    :return: (i_start, i_end, e_start, e_end) data type are datetime or None
             if e_start != None, means need read from external, e_end also != None,
             if i_end != None, means need read from internal database. But i_start can be None
    """
    db_accessor = DatabaseAccessor()
    db_checkpoint = db_accessor.get_checkpoints(code, accessor_name)
    i_start = None
    i_end = None
    e_start = None
    e_end = None

    if start_date:
        start_date = date_parser.parse(start_date).date()
    else:
        start_date = constant.default_start_date

    if end_date:
        end_date = date_parser.parse(end_date).date()
    else:
        end_date = date.today()

    if db_checkpoint == constant.default_start_date or start_date > db_checkpoint:
        i_start = None
        i_end = None
        e_start = start_date
        e_end = end_date
    elif end_date <= db_checkpoint:
        i_start = None
        i_end = end_date
        e_start = None
        e_end = None
    else:
        i_start = start_date
        i_end = db_checkpoint
        e_start = db_checkpoint + timedelta(days=1)
        e_end = end_date

    return i_start, i_end, e_start, e_end


def main():
    str_input = '欢迎你'
    a = multi_get_first(str_input)
    print a


if __name__ == "__main__":
    main()
