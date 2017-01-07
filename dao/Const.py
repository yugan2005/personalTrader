# -*- coding: utf-8 -*-
import re
from datetime import datetime


class Const:
    host = 'localhost'
    port = 27017
    db_name = 'personalTrader'
    sym_coll_name = 'symbol'
    tdx_daily_coll_name = 'tdx_ori'

    default_init_fund = 100.00

    float_tolerance = 0.00001

    tdx_base_path1 = '/Users/yugan/VMSharedFolder/stock'
    tdx_base_path2 = '/Users/yugan/VMSharedFolder/stock2'
    tdx_empty_file_size = 17
    tdx_csv_schema = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount']

    y_t_map = {'SH': 'SS', 'SS': 'SH'}
    sym_coll_schema = {'code': '代码',
                       'ncode': 'Yahoo用代码',
                       'TDXname': '通达信文件名',
                       'reserved': '公积金',
                       'outstanding': '流通股本(亿)',
                       'timeToMarket': '上市日期',
                       'totalAssets': '总资产(万)',
                       'area': '地区',
                       'profit': '利润同比(%)',
                       'rev': '收入同比(%)',
                       'totals': '总股本(亿)',
                       'pb': '市净率',
                       'fixedAssets': '固定资产',
                       'pe': '市盈率',
                       'gpr': '毛利率(%)',
                       'undp': '未分利润',
                       'perundp': '每股未分配',
                       'npr': '净利润率(%)',
                       'name': '名称',
                       'reservedPerShare': '每股公积金',
                       'industry': '所属行业',
                       'esp': '每股收益',
                       'liquidAssets': '流动资产',
                       'bvps': '每股净资',
                       'holders': '股东人数',
                       'data_start': '数据起始日',
                       'data_end': '数据结束日'}

    xueqiu_stock_url = 'http://xueqiu.com/S/{}/historical.csv'
    xueqiu_fhps_url = 'https://xueqiu.com/S/{}/FHPS'
    xueqiu_hdr = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'none',
        'Accept-Language': 'en-US,en;q=0.8',
        'Connection': 'keep-alive'}

    sohu_fhps_url = 'http://q.stock.sohu.com/cn/{}/fhsp.shtml'
    sohu_fhps_table_xpath = '/html/body/div[4]/div[2]/div[2]/div[2]/div/div[2]/table'
    sohu_zg_re = re.compile(ur'转增(\d+)股')
    sohu_sg_re = re.compile(ur'送(\d+)股')
    sohu_fh_re = re.compile(ur'派息(.+)元')
    sohu_date_re = re.compile(ur'(\d{4}-\d{2}-\d{2})')

    firefox_KillSpinners_ext_path = '/Users/yugan/Library/Application Support/Firefox/Profiles/1qvac6rq.default/extensions/killspinners@byo.co.il.xpi'

    hq_col_names = ['Date', 'Open', 'High', 'Close', 'Low', 'Volume', 'Amount']
    hq_datatypes = [datetime, float, float, float, float, int, float]

    fhps_col_names = ['Date', 'Fh', 'Ps']
    fhps_datatypes = [datetime, float, float]


    default_start_date = datetime(1980, 1, 1)
    code_map = {'0': 'SZ', '1': 'SZ', '3': 'SZ', '5': 'SH', '6': 'SH'}
