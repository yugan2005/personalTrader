import pandas as pd
import tushare as ts
from datetime import datetime
from Const import Const
import os

package_directory = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(package_directory, '../resources/tk.key'), 'r') as f:
    token = f.readline()
    ts.set_token(token)


def get_stock_info():
    file_path = os.path.join(Const.tdx_base_path, 'qfq')
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
    file_path = os.path.join(Const.tdx_base_path, fq, TDXname)
    df = (pd.read_csv(file_path, names=Const.tdx_csv_schema, header=None, skipfooter=1, parse_dates=[0],
                      infer_datetime_format=True, engine='python')
          .set_index('Date'))
    df = clean_data(df)
    return df


def clean_data(df):
    '''
    Found that the 'qfq', 'bfq' data has duplicated dates, the first one seems correct for 'qfq' and the same for
    'bfq'. need dedupe! Also these duplicated records are not together.
    some example files 'qfq' ['SZ#000559.txt', 'SZ#000560.txt', 'SZ#000561.txt', 'SZ#000563.txt',
    'SZ#000564.txt', 'SZ#000565.txt', 'SZ#000566.txt', 'SZ#000567.txt', 'SZ#000568.txt', 'SZ#000570.txt',
    'SZ#000571.txt', 'SZ#000572.txt', 'SZ#000573.txt', 'SZ#000576.txt'...] Total length is 50
    :return: list of bad_file names
    '''
    return df[~df.index.duplicated()]


def main():
    # df = get_stock_info()
    # print df
    # df2 = get_stock(df.loc['600033', 'TDXname'], 'qfq')
    # print df2
    print clean_data()


if __name__ == '__main__':
    main()
