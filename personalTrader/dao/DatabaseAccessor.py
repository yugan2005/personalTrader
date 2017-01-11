# -*- coding: utf-8 -*-

from abstractAccessor import InternalAccessor
import constant


class DatabaseAccessor(InternalAccessor):
    def __init__(self):
        pass

    def get_fhps(self, code, start_date=None, end_date=None, retry_times=3):
        pass

    def get_hq(self, code, start_date=None, end_date=None, retry_times=3):
        pass

    def get_checkpoints(self, code, accessor_name):
        return constant.default_start_date
