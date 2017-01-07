# -*- coding: utf-8 -*-
import sys, os

try:
    from Dao import InternalAccessor
    from Const import Const
except Exception:
    sys.path.append(os.path.join(os.path.dirname(__file__)))
    from Dao import InternalAccessor
    from Const import Const





class DatabaseAccessor(InternalAccessor):

    def __init__(self):
        pass

    def get_fhps(self, code, start_date=None, end_date=None, retry_times=3):
        pass

    def get_hq(self, code, start_date=None, end_date=None, retry_times=3):
        pass

    def get_checkpoints(self, code, data_source):
        return Const.default_start_date
