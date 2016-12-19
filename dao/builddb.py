# -*- coding: utf-8 -*-

import pymongo
import pandas_datareader.data as web
import sys

sys.path.append('/Users/yugan/Dropbox/personalTrader')

from dao.datacleaner import clean_yahoo
from indicators.commonIndicator import get_dwm_ochlmk
from dao.Const import Const


def get_symbols(fpath):
    suffix = None
    record_ls = []
    with open(fpath, 'r') as f:
        for line in f:
            if '(' in line:
                record = dict()
                tokens = line.strip().split('(')
                record['scode'] = tokens[0].strip()
                record['ncode'] = tokens[1][:-1] + '.' + suffix
                record_ls.append(record)
            else:
                if line.strip() == '深圳股票':
                    suffix = 'SZ'
                else:
                    suffix = 'SS'
    return record_ls


def init_collection(db, collection_name, record_list, idx_name, unique=True):
    if collection_name in db.collection_names():
        db.drop_collection(collection_name)
    db[collection_name].insert_many(record_list)
    db[collection_name].create_index([(idx_name, pymongo.ASCENDING)], unique=unique, name=idx_name + '_idx')


def main():
    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    symbol_ls = get_symbols(Const.symbol_file_path)

    # Only need run this code once to build the symbol collection the 1st time
    init_collection(db, Const.symbol_collection_name, symbol_ls, 'ncode', True)

    # Only need run this code once to build timeline collections the 1st time
    invalid_ncode = []
    progress = 0
    invalid_cnt = 0
    for symbol in symbol_ls:
        ncode = symbol['ncode']
        try:
            timeline = web.DataReader(ncode, 'yahoo', start='1990-01-01')
            timeline = clean_yahoo(timeline)
            timeline = timeline.drop('Adj Close', axis=1)
            dwm_ochlmk = get_dwm_ochlmk(timeline).reset_index(drop=True)
            init_collection(db, ncode, dwm_ochlmk.to_dict('records'), 'Date_d', True)
            first_date = db[ncode].find_one(sort=[('Date_d', pymongo.ASCENDING)])['Date_d']
            last_date = db[ncode].find_one(sort=[('Date_d', pymongo.DESCENDING)])['Date_d']
            db[Const.symbol_collection_name].update({'ncode': ncode}, {'$set': {'from': first_date,
                                                                                'to': last_date}})
            progress += 1
            if (progress % 10 == 0):
                print 'Finished {0:.2f}%'.format(float(progress) / len(symbol_ls) * 100)
        except Exception:
            invalid_cnt += 1
            print 'Invalid symbol: {0}. Total invalid symbol so far: {1}'.format(ncode, invalid_cnt)
            invalid_ncode.append(ncode)

    if invalid_ncode:
        delete_res = db[Const.symbol_collection_name].delete_many({'ncode': {'$in': invalid_ncode}})
        print '# of invalid symbol: %d' % len(invalid_ncode)
        print '# of document removed: %d' % delete_res.deleted_count


if __name__ == '__main__':
    main()
