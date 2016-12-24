# -*- coding: utf-8 -*-

class Const:
    host = 'localhost'
    port = 27017
    db_name = 'personalTrader'
    sym_coll_name = 'symbol'

    default_init_fund = 100.00

    float_tolerance = 0.00001

    tdx_base_path = '/Users/yugan/VMSharedFolder/stock'
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
