# -*- coding: utf-8 -*-
import pymongo
import pandas as pd
from datetime import date

import constant
import utility
from DatabaseAccessor import DatabaseAccessor


class DatabaseBuilder():
    def __init__(self, host=constant.host, port=constant.port):
        self.c = pymongo.MongoClient(host=host, port=port)
        self.db_accessor = DatabaseAccessor()

    def update_stock_hq(self, code, update_accessor_name='all', start_date=None, end_date=None, retry_times=3):

        if not end_date:
            end_date = date.today()

        accessor_map = constant.external_accessor_map
        checking_df = {}
        earliest_check_point = date.today()

        for accessor_name, accessor in accessor_map.items():
            if update_accessor_name == 'all' or update_accessor_name == accessor_name:
                i_start, i_end, e_start, e_end = utility.get_dates(code, accessor_name, start_date, end_date)
                check_point = self.db_accessor.get_checkpoints(code=code, accessor_name=accessor_name)
                if e_start:
                    df = accessor.get_hq(code, start_date=e_start, end_date=e_end, retry_times=retry_times)
                    checking_df[accessor_name] = df
                    earliest_check_point = min(earliest_check_point, check_point)

        if not checking_df or earliest_check_point == date.today():
            return

        new_checkpoints = dict()
        for cur_date in pd.date_range(earliest_check_point, end_date):
