import pandas as pd
import numpy as np
import os

import pyanomaly as pa
from pyanomaly.globals import *
from pyanomaly.wrdsdata import WRDS

# DIR = '../etfg/'
DIR = 'E:/etfg/'
DIR_CONSTITUENTS = DIR + 'constituents_us/'

def gather_constituent_files():
    columns = [
        'etfg_date',  # missing in old files
        'as_of_date',  # 0, 1
        'Composite_Ticker',  # 1, 2
        'Constituent_Ticker',  # 2, 3
        'Constituent_Name',  # 3, 4
        'Weight',  # 4, 5
        'Market_Value',  # 5, 6
        'Cusip',  # 6, 7
        'Isin',  # 7, 8
        'Figi',  # 8, 9
        'Sedol',  # 9, 10
        'Country_of_Exchange',  # 10, 11
        'Exchange',  # 11, 12
        'Shares_Held',  # 12, 13
        'Asset_Class',  # 13, 14
        'Security_Type',  # 14, 15
        'Currency_Traded'  # missing in old files
    ]
    use_columns = [
        'date',
        # 'etf_ticker',
        'ticker',
        'name',
        # 'weight',
        # 'market_value',
        'cusip',
        # 'isin',
        # 'figi',
        # 'sedol',
        # 'shares',
        'asset_class',
    ]
    # dtypes = {
    #     'as_of_date': np.datetime64,
    #     'Composite_Ticker': str,
    #     'Constituent_Ticker': str,
    #     'Constituent_Name': str,
    #     'Weight': float,
    #     'Market_Value': float,
    #     'Cusip': str,
    #     'Isin': str,
    #     'Figi': str,
    #     'Sedol': str,
    #     'Shares_Held': int,
    #     'Asset_Class': str,
    # }

    name_list = []

    dirs = os.listdir(DIR_CONSTITUENTS)
    for dir in dirs:
        if not os.path.isdir(DIR_CONSTITUENTS + dir):
            continue

        print(dir)
        df_list1 = []
        df_list2 = []
        for root, dirs, files in os.walk(DIR_CONSTITUENTS + dir):
            for file in files:
                print(file)
                try:
                    df = pd.read_csv(root + '/' + file, header=None, dtype=object)
                except Exception as e:
                    print(e)
                    continue

                if file[-10:] == 'export.csv':
                    df = df[[0, 2, 3, 6, 13]]
                elif file[-6:] == 'v2.csv':
                    df = df[[1, 3, 4, 7, 14]]
                else:
                    continue

                df.columns = use_columns
                df['date'] = df['date'].astype(np.datetime64)
                df['name'] = df['name'].str.lower()
                # df['weight'] = df['weight'].astype(float)
                # df['market_value'] = df['market_value'].astype(float)
                # df['shares'] = df['shares'].astype(float)

                name_list += list(df['name'].values)

    return name_list
        #         if file[-10:] == 'export.csv':
        #             df_list1.append(df)
        #         else:
        #             df_list2.append(df)
        #
        # if df_list1:
        #     df = pd.concat(df_list1)
        #     print(f'shape before: {df.shape}')
        #     df = df.drop_duplicates()
        #     print(f'shape after: {df.shape}')
        #     df.to_pickle(f'data/constituents_{dir}_export.pickle')
        #
        # if df_list2:
        #     df = pd.concat(df_list2)
        #     print(f'shape before: {df.shape}')
        #     df = df.drop_duplicates()
        #     print(f'shape after: {df.shape}')
        #     df.to_pickle(f'data/constituents_{dir}_v2.pickle')

import difflib

def link_etfg_permno():
    wrds = WRDS('fehouse')
    stocknames = wrds.read_data('stocknames')
    # holdings = pd.read_pickle('./data/constituents_2018_v2.pickle')
    with open("etfg_names.json", 'r') as f:
        etfg_names = json.load(f)

    # print(holdings.shape)
    # cusip = stocknames[['permno', 'cusip']].drop_duplicates()
    # ncusip = stocknames[['permno', 'ncusip']].drop_duplicates().rename({'ncusip': 'cusip'})
    # cusip = pd.concat([cusip, ncusip]).drop_duplicates().dropna()
    #
    # holdings = holdings.merge(cusip, on='cusip', how='left')
    # holdings_mapped = holdings[~holdings['permno'].isna()]
    #
    # holdings = holdings[holdings['permno'].isna()]
    # ticker = stocknames[['permno', 'ticker', 'namedt', 'nameenddt']].drop_duplicates()
    # holdings = holdings.merge(ticker, on='ticker', how='left')
    # holdings = holdings[(holdings['date'] >= holdings['namedt']) & (holdings['date'] <= holdings['nameenddt'])]
    #
    # holdings_mapped = pd.concat([holdings_mapped, holdings[~holdings['permno'].isna()]])
    #
    # holdings = holdings[holdings['permno'].isna()]
    crsp_names = stocknames['comnam'].str.lower().unique()
    names1 = pd.Series(etfg_names, name='name').sort_values()
    names2 = pd.Series(crsp_names, name='name').sort_values()
    names = pd.merge(names1, names2, on='name', how='inner')
    names = list(np.squeeze(names.values))
    etfg_names = [name for name in etfg_names if name not in names]

    name_mapping = {name: name for name in names}
    for i, name in enumerate(etfg_names):
        try:
            ret = difflib.get_close_matches(name, crsp_names, n=1, cutoff=0.7)
            if ret:
                name_mapping[name] = ret[0]
                log(f'{i}/{len(etfg_names)}: {name} - {ret[0]}')
        except Exception as e:
            log(e)

    return name_mapping


import json


if __name__ == '__main__':
    # name_list = gather_constituent_files()
    # name_list = list(set(name_list))
    name_mapping = link_etfg_permno()

    # wrds = WRDS('fehouse')
    # # wrds.download_table('crsp', 'stocknames')
    #

    with open("name_mapping.json", 'w') as f:
        json.dump(name_mapping, f)

