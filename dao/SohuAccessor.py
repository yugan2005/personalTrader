# -*- coding: utf-8 -*-
import json
import requests
import pandas as pd
import re
import sys, os

from StringIO import StringIO
from dateutil import parser as date_parser
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


class SohuAccessor(Accessor):
    def __init__(self):
        self.hq_baseURL = 'http://q.stock.sohu.com/hisHq?&code=cn_{}&start={:%Y%m%d}&end={:%Y%m%d}&t=d&rt=json'
        self.data_source = 'sohu'
        self.fhps_baseURL = 'http://q.stock.sohu.com/cn/{}/fhsp.shtml'
        self.fhps_table_xpath = '/html/body/div[4]/div[2]/div[2]/div[2]/div/div[2]/table'
        self.zg_re = re.compile(ur'转增(\d+)股')
        self.sg_re = re.compile(ur'送(\d+)股')
        self.fh_re = re.compile(ur'派息(.+)元')
        self.date_re = re.compile(ur'(\d{4}-\d{2}-\d{2})')

    def get_fhps(self, code, start_date=None, end_date=None, retry_times=3):
        i_start, i_end, e_start, e_end = dao_util.get_dates(code, self.data_source, start_date, end_date)
        e_df = None
        i_df = None
        url = self.fhps_baseURL.format(code)
        table = None
        retry = 0

        if e_start:
            while retry < retry_times:
                with dao_util.open_phantomJS_driver() as driver:
                    try:
                        driver.get(url)
                        d_table = WebDriverWait(driver, 30).until(
                            EC.presence_of_element_located((By.XPATH, self.fhps_table_xpath)))
                        table = d_table.text
                        if not table:
                            retry += 1
                        else:
                            retry = retry_times + 1
                    except Exception:
                        retry += 1

            if retry != retry_times:
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
                            date_m = self.date_re.search(next_line)
                        if not date_m:
                            date_m = self.date_re.search(line)
                        zg_m = self.zg_re.search(line)
                        sg_m = self.sg_re.search(line)
                        fh_m = self.fh_re.search(line)
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

                if records:
                    e_df = pd.DataFrame(records)
                    for field in ['Fh', 'Zg', 'Sg']:
                        if field not in e_df:
                            e_df.loc[:, field] = 0.0
                    e_df.loc[:, ['Fh', 'Zg', 'Sg']] = e_df.loc[:, ['Fh', 'Zg', 'Sg']].fillna(0.0)
                    e_df.loc[:, 'Ps'] = e_df['Zg'] + e_df['Sg']
                    e_df = e_df[Const.fhps_col_names].set_index('Date', drop=False).sort_index()
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

        if e_start:
            hq_url = self.hq_baseURL.format(code, e_start, e_end)
            retry = 0
            while retry < retry_times:
                r = requests.get(hq_url)
                if r.status_code != 200:
                    retry += 1
                else:
                    retry = retry_times + 1  # this means read successfully
            if retry != retry_times:
                page = r.content[1:-2]
                page_io = StringIO(page)
                data = json.load(page_io)
                e_df = pd.DataFrame(data['hq']).iloc[:, [0, 1, 6, 2, 5, 7, 8]]
                e_df.iloc[:, 0] = pd.to_datetime(e_df.iloc[:, 0], errors='coerce')
                for i in range(1, len(e_df.columns)):
                    e_df.iloc[:, i] = e_df.iloc[:, i].astype(Const.hq_datatypes[i])
                e_df.columns = Const.hq_col_names
                e_df = e_df.set_index('Date', drop=False).sort_index()
                e_df = dao_util.clean_hq_df(e_df)
                e_df = e_df[e_start:e_end]

        if i_end:
            db_accessor = DatabaseAccessor()
            i_df = db_accessor.get_hq(code, i_start, i_end)

        df = pd.concat([i_df, e_df]).sort_index()
        return df


def main():
    sohuAccessor = SohuAccessor()
    code = '600033'
    start_date = '2015-01-12'
    end_date = '2016-01-12'
    print sohuAccessor.get_hq(code, start_date, end_date)
    print '---'
    print sohuAccessor.get_fhps(code, start_date, end_date)
    print '---'
    print sohuAccessor.get_fhps(code)


if __name__ == '__main__':
    main()
