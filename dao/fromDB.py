# -*- coding: utf-8 -*-
import pymongo
import sys, os
import pandas as pd
from dateutil import parser as date_parser

from Const import Const


def get_stock(code, fq, start_date=None, end_date=None):
    """
    :param code: numcode (no suffix)
    :param fq: string: ['qfq', 'bfq']
    :param start_date: optional
    :param end_date: optional
    :return: a pandas dataframe dwm_ochlmk
    """

    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    collection = db[fq + '_' + code]
    if (not start_date) and (not end_date):
        return (pd.DataFrame(list(collection.find()))
                .drop('_id', axis=1))
    symbol = db[Const.sym_coll_name]
    cur_symbol = symbol.find_one({'code': code})
    start_date_db = cur_symbol['data_start']
    end_date_db = cur_symbol['data_end']
    if start_date:
        start_date = date_parser.parse(start_date)
        start_date_db = max(start_date, start_date_db)
    if end_date:
        end_date = date_parser.parse(end_date)
        end_date_db = min(end_date, end_date_db)
    return (pd.DataFrame(list(collection.find({'Date_d': {'$lte': end_date_db, '$gte': start_date_db}})))
            .drop('_id', axis=1))


def get_stock_ori(code, fq, start_date=None, end_date=None):
    """
    :param code: numcode (no suffix)
    :param fq: string: ['qfq', 'bfq']
    :param start_date: optional
    :param end_date: optional
    :return: a pandas dataframe index by 'Date' (sorted), with columns ['Open', 'High', 'Close', 'Low', 'Volume', 'Amount'].
    """

    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    collection = db[Const.tdx_daily_coll_name + fq]
    ori_df = (pd.DataFrame(list(collection.find({'code': code}, {'code': 0, '_id': 0})))
              .set_index('Date')
              .sort_index())[['Open', 'High', 'Close', 'Low', 'Volume', 'Amount']]
    if (not start_date) and (not end_date):
        return ori_df
    start_date_db = ori_df.index[0]
    end_date_db = ori_df.index[-1]
    if start_date:
        start_date = date_parser.parse(start_date)
        start_date_db = max(start_date, start_date_db)
    if end_date:
        end_date = date_parser.parse(end_date)
        end_date_db = min(end_date, end_date_db)
    return ori_df[start_date_db:end_date_db]


def get_all_stock_info():
    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    collection = db[Const.sym_coll_name]
    stock_info = pd.DataFrame(list(collection.find())).drop('_id', axis=1)
    return stock_info


def get_stock_info_pretty():
    stock_info = get_all_stock_info()
    stock_info.columns = stock_info.columns.map(lambda name: name + ':' + Const.sym_coll_schema[name].decode('utf8'))
    return stock_info


def get_stock_open_dates(code, start_date=None, end_date=None):
    """
    using TDX original bfq data, taken from DataBase
    :param code: string without prefix or suffix, like '600033'
    :param start_date: optional
    :param end_date: optional
    :return: Dataframe with sorted index of dates, and Column 'Date'
    """
    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    coll = db[Const.tdx_daily_coll_name + 'bfq']
    dates = pd.DataFrame(list(coll.find({'code': code}, {'Date': 1, '_id': 0})))
    dates = dates.set_index('Date', drop=False).sort_index()
    if start_date:
        dates = dates[start_date:]
    if end_date:
        dates = dates[:end_date]
    return dates


def from_code_get_yahoo_name(code):
    """
    :param code: string number code without suffix like '600033'
    :return: string yahoo code name
    """
    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    collection = db[Const.sym_coll_name]
    ncode = collection.find_one({'code':code}, {'ncode':1, '_id':0})['ncode']
    return str(ncode)

def from_code_get_xueqiu_name(code):
    """
    :param code: string number code without suffix like '600033'
    :return: string xueqiue name
    """
    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    collection = db[Const.sym_coll_name]
    TDXname = collection.find_one({'code':code}, {'TDXname':1, '_id':0})['TDXname']
    xueqiu_name = ''.join(TDXname.split('.')[0].split('#'))
    return str(xueqiu_name)

def main():
    code = '600012'
    # dwm_ochlmk = get_stock(code, 'qfq')
    # print dwm_ochlmk
    # print '---'
    # print get_all_stock_info()

    print get_stock_open_dates(code=code, start_date='2005-05-01', end_date='2017-12-01')


if __name__ == '__main__':
    main()
