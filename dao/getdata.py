# -*- coding: utf-8 -*-
import pymongo
import pandas as pd

from Const import Const


def hist_by_num(ncode, start_date=None, end_date=None):
    """
    :param ncode: numcode with suffix
    :param start_date: optional
    :param end_date: optional
    :return: a pandas dataframe dwm_ochlmk
    """

    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    collection = db[ncode]
    if (not start_date) and (not end_date):
        return (pd.DataFrame(list(collection.find()))
                .drop('_id', axis=1))
    symbol = db[Const.symbol_collection_name]
    cur_symbol = symbol.find_one({'ncode': ncode})
    start_date_db = cur_symbol['from']
    end_date_db = cur_symbol['to']
    if start_date:
        start_date_db = max(start_date, start_date_db)
    if end_date:
        end_date_db = min(end_date, end_date_db)
    return (pd.DataFrame(list(collection.find({'Date_d': {'$lte': end_date_db, '$gte': start_date_db}})))
            .drop('_id', axis=1))

def get_symbols():
    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    collection = db[Const.symbol_collection_name]
    symbol_ncodes = pd.DataFrame(list(collection.find({}, {'_id':0, 'ncode':1})))
    return symbol_ncodes['ncode'].values





def main():
    # ncode = '600012.SS'
    # dwm_ochlmk = hist_by_num(ncode)
    # print dwm_ochlmk
    print '---'
    print get_symbols()


if __name__ == '__main__':
    main()
