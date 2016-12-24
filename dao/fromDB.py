# -*- coding: utf-8 -*-
import pymongo
import pandas as pd

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
    collection = db[fq+'_'+code]
    if (not start_date) and (not end_date):
        return (pd.DataFrame(list(collection.find()))
                .drop('_id', axis=1))
    symbol = db[Const.sym_coll_name]
    cur_symbol = symbol.find_one({'code': code})
    start_date_db = cur_symbol['data_start']
    end_date_db = cur_symbol['data_end']
    if start_date:
        start_date_db = max(start_date, start_date_db)
    if end_date:
        end_date_db = min(end_date, end_date_db)
    return (pd.DataFrame(list(collection.find({'Date_d': {'$lte': end_date_db, '$gte': start_date_db}})))
            .drop('_id', axis=1))

def get_stock_info():
    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    collection = db[Const.sym_coll_name]
    stock_info = pd.DataFrame(list(collection.find())).drop('_id', axis=1)
    return stock_info

def get_stock_info_pretty():
    stock_info = get_stock_info()
    stock_info.columns = stock_info.columns.map(lambda name: name + ':' + Const.sym_coll_schema[name].decode('utf8'))
    return stock_info



def main():
    code = '600012'
    dwm_ochlmk = get_stock(code, 'qfq')
    print dwm_ochlmk
    # print '---'
    # print get_stock_info()


if __name__ == '__main__':
    main()
