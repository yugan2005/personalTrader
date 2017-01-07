# -*- coding: utf-8 -*-

import pandas as pd
import tushare as ts
from datetime import datetime
from Const import Const
import os

package_directory = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(package_directory, '../resources/tk.key'), 'r') as f:
    token = f.readline()
    ts.set_token(token)


def get_all_stock_info():
    file_path = os.path.join(Const.tdx_base_path1, 'qfq')
    ncodes = []
    for filename in os.listdir(file_path):
        if filename[0] != '.' and os.path.getsize(os.path.join(file_path, filename)) != Const.tdx_empty_file_size:
            record = dict()
            tdx_prefix = filename.strip().split('#')[0].strip()
            code = filename.strip().split('#')[1].split('.')[0].strip()
            yahoo_suffix = Const.y_t_map.get(tdx_prefix, tdx_prefix)
            ncode = code + '.' + yahoo_suffix
            record['code'] = code.encode('utf8')
            record['ncode'] = ncode.encode('utf8')
            record['TDXname'] = filename
            ncodes.append(record)
    ncode_df = pd.DataFrame(ncodes).set_index('code')
    ts_symbols = ts.get_stock_basics()
    result = ts_symbols.join(ncode_df, how='inner')
    result = result[result.timeToMarket != 0]
    result.timeToMarket = result.timeToMarket.map(lambda n: datetime.strptime(str(n), '%Y%m%d'))

    return result


def get_stock(TDXname, fq):
    """
    need specify whether it is 'qfq' or 'bfq'
    :param TDXname:
    :param fq: ['qfq'|'bfq']
    :return: dataframe with index of 'Date' and columns of ['Open', 'High', 'Low', 'Close', 'Volume', 'Amount'], or None
    """
    file_path = os.path.join(Const.tdx_base_path1, fq, TDXname)
    file_path2 = os.path.join(Const.tdx_base_path2, fq, TDXname)
    df = (pd.read_csv(file_path, names=Const.tdx_csv_schema, header=None, skipfooter=1, parse_dates=[0],
                      infer_datetime_format=True, engine='python')
          .set_index('Date'))
    if df.index.duplicated().sum() != 0:
        df = (pd.read_csv(file_path2, names=Const.tdx_csv_schema, header=None, skipfooter=1, parse_dates=[0],
                          infer_datetime_format=True, engine='python')
              .set_index('Date'))
    # There are corrupted data with Volume == 0, which should be removed
    df = df[df.Volume != 0]
    return df


def check_data():
    """
       Found that the 'qfq', 'bfq' data has duplicated dates, The version from my VM has issues for ['SZ#000559.txt',
       'SZ#000560.txt', 'SZ#000561.txt', 'SZ#000563.txt', 'SZ#000564.txt', 'SZ#000565.txt', 'SZ#000566.txt', ...]
       The version from Dad's machine has issue for SH 2014-02-10. Combine this two version seems okay now.
       :return: bad_files
    """
    base_path = Const.tdx_base_path1
    base_path2 = Const.tdx_base_path2
    bad_files = []
    tot_cnt = len(os.listdir(os.path.join(base_path, 'qfq'))) * 2
    cnt = 0

    for fq in ['qfq', 'bfq']:
        file_folder = os.path.join(base_path, fq)
        for filename in os.listdir(file_folder):
            if filename[0] != '.' and os.path.getsize(os.path.join(file_folder, filename)) != Const.tdx_empty_file_size:
                file_path = os.path.join(base_path, fq, filename)
                df = (pd.read_csv(file_path, names=Const.tdx_csv_schema, header=None, skipfooter=1, parse_dates=[0],
                                  infer_datetime_format=True, engine='python')
                      .set_index('Date'))
                if df.index.duplicated().sum() != 0:
                    file_path2 = os.path.join(base_path2, fq, filename)
                    df2 = (pd.read_csv(file_path2, names=Const.tdx_csv_schema, header=None,
                                       skipfooter=1, parse_dates=[0],
                                       infer_datetime_format=True, engine='python').set_index('Date'))
                    if df2.index.duplicated().sum() != 0:
                        bad_file = dict()
                        bad_file['filename'] = os.path.join(fq, filename)
                        bad_file['date'] = df.index[df.index.duplicated()][0]
                        bad_files.append(bad_file)
            cnt += 1
            if cnt % 100 == 0:
                print 'Finished {0:.2f}%, bad files found: {1}'.format(float(cnt * 100) / tot_cnt, len(bad_files))
    if len(bad_files) != 0:
        return bad_files

    return 0


def main():
    # df = get_all_stock_info()
    # print df
    # df2 = get_stock(df.loc['600033', 'TDXname'], 'qfq')
    # print df2
    # print clean_data(df2)
    print check_data()


if __name__ == '__main__':
    main()
