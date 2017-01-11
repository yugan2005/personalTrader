# -*- coding: utf-8 -*-

from unittest import TestCase
from mock import patch
import datetime
import pandas as pd
from pandas_datareader import data as web
from dao import constant
from dao import utility
from dao.SohuAccessor import SohuAccessor


class TestSohuAccessor(TestCase):
    def setUp(self):
        self.accessor = SohuAccessor()
        self.accessor_name = 'sohu'
        self.code = '600033'
        self.yahoo_code = '{}.{}'.format(self.code, constant.yahoo_code_map.get(self.code[0]))
        # Note: This is checked from Sohu, (the same for Sina) but actually not complete
        # The http://stock.jrj.com.cn/share,600033,fhsp.shtml has 2006-07-14: fh: 0.25
        self.fhps_list = [{'Date': datetime.date(2002, 6, 27), 'Fh': 0.12, 'Ps': 0.0},
                          {'Date': datetime.date(2003, 4, 30), 'Fh': 0.05, 'Ps': 0.2},
                          {'Date': datetime.date(2004, 6, 3), 'Fh': 0.15, 'Ps': 0.2},
                          {'Date': datetime.date(2005, 6, 16), 'Fh': 0.35, 'Ps': 0.0},
                          {'Date': datetime.date(2006, 6, 1), 'Fh': 0.35, 'Ps': 0.0},
                          {'Date': datetime.date(2006, 9, 19), 'Fh': 0.0, 'Ps': 0.5},
                          {'Date': datetime.date(2007, 7, 5), 'Fh': 0.25, 'Ps': 0.0},
                          {'Date': datetime.date(2008, 4, 17), 'Fh': 0.25, 'Ps': 0.0},
                          {'Date': datetime.date(2009, 5, 21), 'Fh': 0.15, 'Ps': 0.0},
                          {'Date': datetime.date(2010, 6, 17), 'Fh': 0.1, 'Ps': 0.5},
                          {'Date': datetime.date(2011, 8, 8), 'Fh': 0.1, 'Ps': 0.0},
                          {'Date': datetime.date(2012, 6, 11), 'Fh': 0.1, 'Ps': 0.0},
                          {'Date': datetime.date(2013, 6, 14), 'Fh': 0.1, 'Ps': 0.0},
                          {'Date': datetime.date(2014, 6, 20), 'Fh': 0.1, 'Ps': 0.0},
                          {'Date': datetime.date(2015, 7, 17), 'Fh': 0.1, 'Ps': 0.0},
                          {'Date': datetime.date(2016, 6, 8), 'Fh': 0.1, 'Ps': 0.0}]
        self.fhps = pd.DataFrame(self.fhps_list).set_index('Date', drop=False)
        # 2014-12-16, 2015-01-05, 2015-08-04 have non-matches between yahoo and sohu. Take dates after that
        self.check_point = datetime.date(2015, 8, 4)

        # This is taken from yahoo.
        self.hq_yahoo = web.DataReader(self.yahoo_code, 'yahoo', constant.default_start_date, datetime.date.today())
        self.hq_yahoo.index = [x.date() for x in self.hq_yahoo.index.to_pydatetime()]
        self.hq_yahoo.loc[:, 'Date'] = self.hq_yahoo.index
        self.hq_yahoo.drop('Adj Close', axis=1, inplace=True)
        self.hq_yahoo = utility.clean_hq_df(self.hq_yahoo)
        self.hq_yahoo.loc[:, 'Volume'] = self.hq_yahoo.loc[:, 'Volume'] / 100  # note that Yahoo's volume need /100
        self.hq_yahoo.Volume = self.hq_yahoo.Volume.astype(int)
        # hard to match volume
        self.hq_yahoo = self.hq_yahoo[['Date', 'Open', 'High', 'Close', 'Low']]

    @patch('dao.DatabaseAccessor.DatabaseAccessor.get_checkpoints')
    def test_get_fhps(self, mock_get_checkpoints):
        mock_get_checkpoints.return_value = self.check_point
        fhps_accessor = self.accessor.get_fhps(self.code)
        fhps_true = self.fhps[(self.check_point + datetime.timedelta(days=1)): datetime.date.today()]
        self.assertTrue(fhps_accessor.equals(fhps_true))

    @patch('dao.DatabaseAccessor.DatabaseAccessor.get_checkpoints')
    def test_get_hq(self, mock_get_checkpoints):
        mock_get_checkpoints.return_value = self.check_point
        hq_accessor = self.accessor.get_hq(self.code)
        hq_accessor.drop(['Volume', 'Amount'], axis=1, inplace=True)  # Yahoo only has not `Amount`
        # minus one day just for timezone concerns
        hq_from_yahoo = self.hq_yahoo[(self.check_point + datetime.timedelta(days=1)): (
        datetime.date.today() - datetime.timedelta(days=1))]
        hq_accessor = hq_accessor[:(datetime.date.today() - datetime.timedelta(days=1))]
        self.assertTrue(hq_accessor.equals(hq_from_yahoo))
