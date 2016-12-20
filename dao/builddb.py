# -*- coding: utf-8 -*-

import pymongo
import pandas_datareader.data as web
import sys
import io

sys.path.append('/Users/yugan/Dropbox/personalTrader')

from dao.datacleaner import clean_yahoo
from indicators.commonIndicator import get_dwm_ochlmk, get_cleaned_daily_df
from dao.Const import Const
from dao import getdata as gd


def get_symbols_from_downloadfile(fpath):
    suffix = None
    record_ls = []
    with io.open(fpath, 'r', encoding='utf8') as f:
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



def update_symbol_file():
    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    with io.open(Const.symbol_file_path, 'w', encoding='utf8') as f:
        for symbol in db[Const.symbol_collection_name].find():
            ncode = symbol['ncode']
            scode = symbol['scode']
            f.write(u'{0}:{1}\n'.format(ncode, scode))


def get_symbols(fpath):
    record_ls = []
    with io.open(fpath, 'r', encoding='utf8') as f:
        for line in f:
            record = dict()
            tokens = line.strip().split(':')
            record['ncode'] = tokens[0].strip()
            record['scode'] = tokens[1].strip()
            record_ls.append(record)
    return record_ls


def init_collection(db, collection_name, record_list, idx_name, unique=True):
    if collection_name in db.collection_names():
        db.drop_collection(collection_name)
    db[collection_name].insert_many(record_list)
    db[collection_name].create_index([(idx_name, pymongo.ASCENDING)], unique=unique, name=idx_name + '_idx')


def build_db_frist_run():
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


def update_db_new_indicators():
    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    total_cnt = db[Const.symbol_collection_name].count()
    processed_cnt = 0
    for symbol in db[Const.symbol_collection_name].find({}, {'ncode': 1}):
        ncode = symbol['ncode']
        old_df = gd.hist_by_num(ncode)
        cleaned_df = get_cleaned_daily_df(old_df)
        new_df = get_dwm_ochlmk(cleaned_df)
        record_list = new_df.to_dict('records')
        for record in record_list:
            db[ncode].update({'Date_d': record['Date_d']}, {'$set': record})
        processed_cnt += 1
        if (processed_cnt % 50 == 0):
            print 'Finished {0:.2f}%'.format(float(processed_cnt) / total_cnt * 100)

def update_one_ncode(ncode):
    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    db.drop_collection(ncode)
    timeline = web.DataReader(ncode, 'yahoo', start='1990-01-01')
    timeline = clean_yahoo(timeline)
    timeline = timeline.drop('Adj Close', axis=1)
    dwm_ochlmk = get_dwm_ochlmk(timeline).reset_index(drop=True)
    init_collection(db, ncode, dwm_ochlmk.to_dict('records'), 'Date_d', True)
    first_date = db[ncode].find_one(sort=[('Date_d', pymongo.ASCENDING)])['Date_d']
    last_date = db[ncode].find_one(sort=[('Date_d', pymongo.DESCENDING)])['Date_d']
    db[Const.symbol_collection_name].update({'ncode': ncode}, {'$set': {'from': first_date,
                                                                        'to': last_date}})

def main():
    # update_symbol_file()
    # from datetime import datetime
    # start_time = datetime.now()
    # build_db_frist_run()
    # build_end_time = datetime.now()
    # print 'build took time: {0:.2f} minutes'.format((build_end_time - start_time).total_seconds() / float(60))
    # update_db_new_indicators()
    # update_end_time = datetime.now()
    # print 'update took time: {0:.2f} minutes'.format((update_end_time - start_time).total_seconds() / float(60))

    update_one_ncode('002514.SZ')


if __name__ == '__main__':
    main()
