# -*- coding: utf-8 -*-
import sys, os
import pymongo

try:
    import fromTDX
    from indicators import commonIndicator
    from Const import Const
    import fromDB
except Exception:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    import fromTDX
    from indicators import commonIndicator
    from Const import Const
    import fromDB


def init_collection(db, collection_name, record_list, idx_name=None, unique=True):
    if collection_name in db.collection_names():
        db.drop_collection(collection_name)
    db[collection_name].insert_many(record_list)
    if idx_name:
        db[collection_name].create_index([(idx_name, pymongo.ASCENDING)], unique=unique, name=idx_name + '_idx')


def build_db_first_run():
    client = pymongo.MongoClient(host=Const.host, port=Const.port)
    db = client[Const.db_name]
    stock_info = fromTDX.get_all_stock_info()
    tot_cnt = len(stock_info)
    cur_cnt = 0
    print '... building db'
    for stock in stock_info.iterrows():
        for fq in ['qfq', 'bfq']:
            df = fromTDX.get_stock(stock[1]['TDXname'], fq)
            coll_name = '{}_{}'.format(fq, stock[0])
            processed_df = commonIndicator.get_dwm_ochlmk(df)
            init_collection(db, coll_name, processed_df.to_dict('records'), idx_name='Date_d')
            db[coll_name].create_index([('Date_w', pymongo.ASCENDING)], unique=False, name='Date_w_idx')
            db[coll_name].create_index([('Date_m', pymongo.ASCENDING)], unique=False, name='Date_m_idx')
            tdx_coll_name = Const.tdx_daily_coll_name + fq
            tdx_df = df.reset_index()
            tdx_df.loc[:, 'code'] = stock[0]
            db[tdx_coll_name].insert_many(tdx_df.to_dict('records'))
            if fq == 'bfq':
                symbol_record = stock[1].to_dict()
                symbol_record['code'] = stock[0]
                symbol_record['data_start'] = tdx_df.Date.min()
                symbol_record['data_end'] = tdx_df.Date.max()
                db[Const.sym_coll_name].insert_one(symbol_record)

        cur_cnt += 1
        if cur_cnt % 10 == 0:
            print 'db building... finished {:.2f}%'.format(float(cur_cnt * 100) / tot_cnt)

    for fq in ['qfq', 'bfq']:
        tdx_coll_name = Const.tdx_daily_coll_name + fq
        db[tdx_coll_name].create_index([('Date', pymongo.ASCENDING)], unique=False, name='Date_idx')
        db[tdx_coll_name].create_index([('code', pymongo.ASCENDING)], unique=False, name='code_idx')





def main():
    build_db_first_run()


if __name__ == '__main__':
    main()
