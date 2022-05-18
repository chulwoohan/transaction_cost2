import pandas as pd
import numpy as np
import os
import json
import difflib
from difflib import SequenceMatcher

import pyanomaly as pa
from pyanomaly.globals import *
from pyanomaly.wrdsdata import WRDS

# DIR = './etfg/'
DIR = 'E:/etfg/'
DIR_CONSTITUENTS = DIR + 'constituents_us/'
DIR_INDUSTRY = DIR + 'industry_us/'
DIR_FUNDFLOW = DIR + 'fundflow_us/'
DIR_V2_HEADER = DIR + 'v2_header_files/'
DIR_LEGACY_HEADER = DIR + 'legacy_header_files/'

legacy_header_industry_map = {
    'etn': 'is_etn',
    'fund_aum': 'aum',
    'average_daily_trading_volume': 'avg_daily_trading_volume',
    'levered': 'is_levered',
    'leverage_factor': 'levered_amount',
    'active': 'is_active',
    'Number_Of_Holdings': 'num_holdings'
}

v2_header_industry_map = {
    'as_of_date': 'date',
    'composite_ticker': 'ticker',
}

legacy_header_fundflow_map = {
    'shares_outstanding': 'shrout',
    'net_flow': 'fundflow',
}

v2_header_fundflow_map = {
    'as_of_date': 'date',
    'composite_ticker': 'ticker',
    'shares_outstanding': 'shrout',
}

legacy_header_constituents_map = {
    'etp_ticker': 'composite_ticker',
    'constitutent_ticker': 'ticker',
    'constituent_name': 'name',
    'constituent_weight': 'weight',
    'constituent_market_value': 'market_value',
    'constituent_cusip': 'cusip',
    'constituent_isin': 'isin',
    'constituent_figi': 'figi',
    'constituent_sedol': 'sedol',
    'constituent_exchange': 'exchange',
    'total_shares_held': 'shares_held',
    'constituent_iso_country': 'country',
}

v2_header_constituents_map = {
    'as_of_date': 'date',
    'constituent_ticker': 'ticker',
    'constituent_name': 'name',
    'country_of_exchange': 'country',
    'currency_traded': 'currency'
}

def read_profile_file(date):
    if date < '2017-04-03':
        type = 'export.csv'
        header_path = DIR_LEGACY_HEADER + 'Legacy_Industry_Header.xlsx'
        header_map = legacy_header_industry_map
    else:
        type = 'v2.csv'
        header_path = DIR_V2_HEADER + 'industry_v2_header.xlsx'
        header_map = v2_header_industry_map

    df = pd.read_csv(DIR_INDUSTRY + date[:4] + f"/{date.replace('-', '')}_industries_{type}", header=None)
    header = pd.read_excel(header_path)
    columns = [col.lower() for col in header.columns]
    df.columns = columns
    df = df.rename(columns=header_map)

    return df

def read_fundflow_file(date):
    if date < '2017-04-03':
        type = 'export.csv'
        header_path = DIR_LEGACY_HEADER + 'Legacy_Fund_Flow_Header.xlsx'
        header_map = legacy_header_fundflow_map
    else:
        type = 'v2.csv'
        header_path = DIR_V2_HEADER + 'fundflow_v2_header.xlsx'
        header_map = v2_header_fundflow_map

    df = pd.read_csv(DIR_FUNDFLOW + date[:4] + f"/{date.replace('-', '')}_fundflow_{type}", header=None)
    header = pd.read_excel(header_path)
    columns = [col.lower() for col in header.columns]
    df.columns = columns
    df = df.rename(columns=header_map)

    return df

def read_constituents_file(date):
    if date < '2017-04-03':
        type = 'export.csv'
        header_path = DIR_LEGACY_HEADER + 'Legacy_Constituents_Header.xlsx'
        header_map = legacy_header_constituents_map
    else:
        type = 'v2.csv'
        header_path = DIR_V2_HEADER + 'constituent_v2_header.xlsx'
        header_map = v2_header_constituents_map

    df = pd.read_csv(DIR_CONSTITUENTS + date[:4] + f"/{date.replace('-', '')}_constituents_{type}", header=None)
    header = pd.read_excel(header_path)
    columns = [col.lower() for col in header.columns]
    df.columns = columns
    df = df.rename(columns=header_map)

    return df

def get_avaiable_dates(sdate=None, edate=None):
    sdate = sdate or '2000-01-01'
    edate = edate or '2099-12-31'

    dates = []
    years = os.listdir(DIR_FUNDFLOW)
    for year in years:
        if not os.path.isdir(DIR_CONSTITUENTS + year):
            continue

        for root, years, files in os.walk(DIR_CONSTITUENTS + year):
            for file in files:
                date = file[:4] + '-' + file[4:6] + '-' + file[6:8]
                if (date >= sdate) and (date <= edate):
                    dates.append(date)

    return dates


def process_profile_files(sdate=None, edate=None):
    """
    """
    const_columns = [  # columns whose values are time-invariant
        'ticker',
        'issuer',
        'description',
        'inception_date',
        'primary_benchmark',
        'tax_classification',
        'is_etn',
        'asset_class',
        'category',
        'focus',
        'development_class',
        'region',
        'is_levered',
        'levered_amount',
        'is_active',
        'administrator',
        'advisor',
        'custodian',
        'distributor',
        'portfolio_manager',
        'subadvisor',
        'transfer_agent',
        'trustee',
        'futures_commission_merchant',
        'fiscal_year_end',
        'distribution_frequency',
        'listing_exchange',
        'creation_unit_size',
        'creation_fee',
        'lead_market_maker',
        'date',
    ]

    dates = get_avaiable_dates(sdate, edate)
    df_list = []
    for date in dates:
        print(date)
        try:
            df = read_profile_file(date)
        except Exception as e:
            print(e)
            continue

        df = df[const_columns]
        df = df.groupby('ticker').last()

        df_list.append(df)

    df = pd.concat(df_list)
    df = df.groupby('ticker').last()
    df = df.rename(columns={'date': 'last_date'})
    df.to_pickle(f'data/profile.pickle')
    return df


def process_fundflow_files(sdate=None, edate=None):
    """
    """
    dates = get_avaiable_dates(sdate, edate)
    df_list = []
    for date in dates:
        print(date)
        try:
            df = read_fundflow_file(date)
        except Exception as e:
            print(e)
            continue

        df = df[['date', 'ticker', 'shrout', 'nav', 'fundflow']]
        df_list.append(df)

    df = pd.concat(df_list)
    df = df.sort_values(['ticker', 'date'])
    df['date'] = pd.to_datetime(df['date'])
    df.to_pickle(f'data/fundflow.pickle')
    return df


def cleanse_ticker(ticker):
    if ticker is None:
        return ''
    if isinstance(ticker, float):
        return ''
    ticker = ticker.strip('*')
    if not ticker.isalpha():
        return ''
    ticker = ticker.split()[0][:5]
    if ticker.isdecimal():
        return ''
    return ticker


v_cleanse_ticker = np.vectorize(cleanse_ticker)


def process_constituent_files1(sdate=None, edate=None):
    """
    """
    columns1 = [
        'cusip',
        'ticker',
        'name',
        'country',
        'exchange',
        'asset_class',
        'security_type',
        'date',
    ]

    dates = get_avaiable_dates(sdate, edate)
    securities = None
    for date in dates:
        print(date)
        try:
            df = read_constituents_file(date)
        except Exception as e:
            print(e)
            continue

        df1 = df[columns1].copy()
        df1['ticker'] = v_cleanse_ticker(df1['ticker'])
        df1.loc[df1.ticker == '', 'ticker'] = None
        df1['cusip'] = df1['cusip'].str[:8]
        df1['name'] = df1['name'].str.lower()
        log(f'Data size before dropping duplicates: {df1.shape}')
# 2      2012-01-03             AMLP    SXL             sunoco logistics partners lp   0.030           NaN   NaN           NaN           NaN      NaN           NaN     NaN          NaN         NaN           NaN    NaN
        is_cusip_null = df1['cusip'].isna()
        df2 = df1[~is_cusip_null].drop_duplicates(['cusip'], keep='last')
        df3 = df1[is_cusip_null].drop_duplicates(columns1[1:-1], keep='last')
        df1 = pd.concat([df2, df3])
        log(f'Data size after dropping duplicates: {df1.shape}')

        # df2 = []
        # for k, g in df1.groupby('cusip', dropna=False):
        #     if (k is not None) and len(g) > 1:
        #         if len(g['name'].unique()) > 1:
        #             g['name'] = g['name'].sort_values().iloc[0]
        #         n = g['ticker'].isna().sum()
        #         if (n > 0) and (n < len(g)):
        #             g.loc[g['ticker'].isna(), 'ticker'] = g['ticker'].sort_values().iloc[0]
        #
        #     df2.append(g)
        #
        # df1 = pd.concat(df2)

        if securities is None:
            securities = df1
        else:
            securities = pd.concat([securities, df1])
        # securities = securities.groupby(['cusip', 'ticker', 'name'], as_index=False, dropna=False).last()
        is_cusip_null = securities['cusip'].isna()
        df2 = securities[~is_cusip_null].drop_duplicates(['cusip'], keep='last')
        df3 = securities[is_cusip_null].drop_duplicates(columns1[1:-1], keep='last')
        securities = pd.concat([df2, df3])
        log(f'securities size: {securities.shape}')

    securities.sort_values('cusip', inplace=True)
    securities = securities.drop_duplicates(columns1[1:-1], keep='first')
    # df2 = []
    # for k, g in df1.groupby(['ticker', 'name']):
    #     if (k[0] is not None) and (k[1] is not None) and len(g) > 1:
    #         n = g['cusip'].isna().sum()
    #         print(n, len(g))
    #         if (n > 0) and (n < len(g)):
    #             g.loc[g['cusip'].isna(), 'cusip'] = g['cusip'].sort_values().iloc[0]
    #
    #     df2.append(g)
    #
    # df2.append(df1[df1['ticker'].isna() | df1['name'].isna()])
    # df1 = pd.concat(df2)
    securities = securities.rename(columns={'date': 'last_date'})
    securities.reset_index(drop=True, inplace=True)
    securities['null_cusip'] = securities['cusip'].isna()
    n = 0
    for idx in securities.index:
        if securities.loc[idx, 'null_cusip']:
            cusip = f'{1000000 + n}X'
            securities.loc[idx, 'cusip'] = cusip
            n += 1

    securities.to_pickle(f'data/securities.pickle')
    return securities


def process_constituent_files2(sdate=None, edate=None):
    """
    columns =
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

    :return:
    """
    columns2 = [
        'date',
        'composite_ticker',
        'cusip',
        'weight',
        'market_value',
        'shares_held'
    ]

    dates = get_avaiable_dates(sdate, edate)
    securities = pd.read_pickle('data/securities.pickle')
    securities.rename(columns={'cusip': 'cusip2'}, inplace=True)
    holdings = []
    for date in dates:
        print(date)
        # if date[:4] != year:
        #     holdings = pd.concat(holdings)
        #     holdings['date'] = pd.to_datetime(holdings['date'])
        #     holdings = holdings.sort_values(['composite_ticker', 'date'])
        #     holdings.to_pickle(f'data/holdings_{year}.pickle')
        #     holdings = []
        #     year = date[:4]

        try:
            df = read_constituents_file(date)
        except Exception as e:
            print(e)
            continue

        df['ticker'] = v_cleanse_ticker(df['ticker'])
        df.loc[df.ticker == '', 'ticker'] = None
        df['cusip'] = df['cusip'].str[:8]
        df['name'] = df['name'].str.lower()

        is_cusip_null = df['cusip'].isna()
        df1 = df[is_cusip_null]
        l1 = len(df1)
        df1 = df1.merge(securities[['cusip2', 'ticker', 'name', 'country', 'exchange', 'asset_class', 'security_type']],
                      on=['ticker', 'name', 'country', 'exchange', 'asset_class', 'security_type'], how='left')
        if l1 != len(df1):
            print(l1, len(df1))

        df1['cusip'] = df1['cusip2']
        df2 = pd.concat([df.loc[~is_cusip_null, columns2], df1[columns2]])
        print('null cusip: ', df2.cusip.isna().sum())
        holdings.append(df2)

    holdings = pd.concat(holdings)
    holdings['date'] = pd.to_datetime(holdings['date'])
    holdings = holdings.sort_values(['composite_ticker', 'date'])
    holdings.to_pickle(f'data/holdings.pickle')


def create_name_map():
    wrds = WRDS('fehouse')
    stocknames = wrds.read_data('stocknames')
    with open("etfg_names.json", 'r') as f:
        etfg_names = json.load(f)

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

    name_mapping = pd.Series(name_mapping)
    name_mapping.index.name = 'etfg_name'
    name_mapping = name_mapping.to_frame(name='crsp_name').reset_index()
    name_mapping['match1'] = np.nan
    name_mapping['match2'] = np.nan
    name_mapping['etfg_name2'] = name_mapping['etfg_name'].str[:10]
    name_mapping['crsp_name2'] = name_mapping['crsp_name'].str[:10]
    for i in name_mapping.index:
        print(f"{i}/{len(name_mapping)}: {name_mapping.loc[i, 'etfg_name']}")
        name_mapping.loc[i, 'match1'] = SequenceMatcher(None, name_mapping.loc[i, 'etfg_name'], name_mapping.loc[i, 'crsp_name']).ratio()
        name_mapping.loc[i, 'match2'] = SequenceMatcher(None, name_mapping.loc[i, 'crsp_name'], name_mapping.loc[i, 'etfg_name']).ratio()
        name_mapping.loc[i, 'match3'] = SequenceMatcher(None, name_mapping.loc[i, 'crsp_name'], name_mapping.loc[i, 'etfg_name']).ratio()

    name_mapping.to_pickle('data/name_mapping.pickle')


def link_etfg_permno():
    """
    TICKER
The combination of ticker, exchange, and date uniquely identifies a security. A ticker may be one to three characters for NYSE and AMEX securities or four or five characters for Nasdaq securities. Nasdaq trading tickers have four base characters and a fifth character suffix that provides information about an issue's type or temporary information about an issue's status. CRSP only includes the suffix when it provides permanent descriptive information. This table describes the suffixes appearing on the CRSP file.

Nasdaq 5th Character Suffixes

Suffix	Definition
A	Class A
B	Class B
F	Companies incorporated outside the US
S	Shares of Beneficial Interest
U	Unit
V	When-issued
Y	ADR
Z	Miscellaneous common issues

Occasionally Nasdaq will add two additional suffixes to the base ticker to identify certain issues. However, because the Nasdaq ticker field only allows for five characters, one letter of the base ticker will be dropped.
For example:

If a foreign company with a class A stock has a base ticker symbol ABCD, Nasdaq adds two additional characters, A and F. Due to Nasdaq's limited fields, they will delete a letter from the base ticker, so ABCDAF would be truncated to ABCAF.

Nasdaq tickers in an issue's name history are presumed to represent legitimate trading symbols for that issue at some point in time, although these symbols may be listed out of proper chronological sequence. In addition, the Nasdaq file ticker symbols provided do not necessarily comprise a definitive list of all symbols used throughout an issue's trading history. Due to source limitations, the TICKER field may be blank in name histories of Nasdaq securities that stopped trading from the early 1970's through the early 1980's. NYSE tickers prior to July 1962 are blank.


CUSIP
CUSIP is the latest eight-character CUSIP identifier for the security through the end of the file. CUSIP identifiers are supplied to CRSP by the CUSIP Service Bureau, Standard & Poor's, a division of McGraw-Hill, Inc., American Bankers Associate database, Copyright 1997. See Appendix A.6 for more CUSIP copyright information.

CUSIP identifiers were first assigned in 1968 as integers and expanded in 1984 to include alphabetic characters. The first six characters (including leading zeroes) identify the issuer, while the last two characters identify the issue. CUSIP issuer identifiers are assigned to maintain an approximately alphabetical sequence. The CUSIP identifier may change for a security if its name or capital structure changes. No header or historical CUSIPs are reused on our files. For securities no longer in existence or that were never assigned an official CUSIP identifier, CRSP has assigned a dummy CUSIP identifier for use in this field in accordance with the rules published in the CUSIP Directory.

There are two potential dummy CUSIPs which are assigned by CRSP. One, ***99*9*, (containing a 9 in the 4th, 5th and 7th character positions) represents a CRSP assigned CUSIP with a dummy issuer number (the first 6 character positions) and a dummy issue number (the last 2 character positions). The other, ******9*, (containing a 9 in the 7th character position) represents a CRSP-assigned CUSIP with a real issuer number but a dummy issue number.

For example:

A CUSIP such as 12399099 or 12345699 is assigned by CRSP, and an identifier such as 12345610 is assigned by the CUSIP Agency. Securities actively traded on an international basis, domiciled outside the United States and Canada, will be identified by a CINS (CUSIP International Numbering System) number. CINS numbers employ the same Issuer (6 characters)/Issue (2 characters) 8-character identifier system set by the CUSIP Numbering System. It is important to note that the first portion of a CINS code is always represented by an alpha character, signifying the issuer's country code (domicile) or geographic region. See the current CUSIP Directory for more information. See Appendix A.1 in the Stock File User's Guide for a list of CINS country codes.


    """
    wrds = WRDS('fehouse')
    stocknames = wrds.read_data('stocknames')
    securities = pd.read_pickle('./data/securities.pickle')

    cusip = stocknames[['permno', 'cusip']].drop_duplicates()
    ncusip = stocknames[['permno', 'ncusip']].drop_duplicates().rename({'ncusip': 'cusip'})
    cusip = pd.concat([cusip, ncusip]).drop_duplicates().dropna()

    securities = securities.merge(cusip, on='cusip', how='left')
    securities.rename(columns={'permno': 'permno1'})
    # holdings_mapped = holdings[~holdings['permno'].isna()]

    # holdings = holdings[holdings['permno'].isna()]
    ticker = stocknames[['permno', 'ticker', 'namedt', 'nameenddt']].drop_duplicates()
    securities = securities.merge(ticker, on='ticker', how='left')
    securities = securities[(securities['date'] >= securities['namedt']) & (securities['date'] <= securities['nameenddt'])]

    holdings_mapped = pd.concat([holdings_mapped, securities[~securities['permno'].isna()]])

    securities = securities[securities['permno'].isna()]



if __name__ == '__main__':
    # df = process_profile_files()
    # df = process_fundflow_files()
    # process_constituent_files2()

    # wrds = WRDS('fehouse')
    # stocknames = wrds.read_data('stocknames')
    # sec = pd.read_pickle('./data/securities.pickle')

    df = pd.read_pickle('data/holdings.pickle')
    df[df.date.dt.year==2012].to_pickle('data/holdings_2012.pickle')
    df[df.date.dt.year==2013].to_pickle('data/holdings_2013.pickle')
    df[df.date.dt.year==2014].to_pickle('data/holdings_2014.pickle')
    df[df.date.dt.year==2015].to_pickle('data/holdings_2015.pickle')
    df[df.date.dt.year==2016].to_pickle('data/holdings_2016.pickle')
    df[df.date.dt.year==2017].to_pickle('data/holdings_2017.pickle')
    df[df.date.dt.year==2018].to_pickle('data/holdings_2018.pickle')
    df[df.date.dt.year==2019].to_pickle('data/holdings_2019.pickle')
    df[df.date.dt.year==2020].to_pickle('data/holdings_2020.pickle')
    df[df.date.dt.year==2021].to_pickle('data/holdings_2021.pickle')

    # process_constituent_files2('2017-01-01', '2017-01-10')
    # profile = read_profile('2021-12-31')
    # flow = read_fundflow('2021-12-31')
    # const = read_constituents('2021-12-31')
    #
    # spy_profile = profile[profile.composite_ticker == 'SPY']
    # spy_flow = flow[flow.composite_ticker == 'SPY']
    # spy_const = const[const.composite_ticker == 'SPY']
    #
    # print('nav: ', np.squeeze(spy_flow.nav))
    # print('aum: ', np.squeeze(spy_profile.aum))
    # print('nav x shares: ', np.squeeze(spy_flow.nav * spy_flow.shares_outstanding))
    # print('sum(market_value): ', spy_const.market_value.sum())
    #
    # profile2 = profile[profile.asset_class == 'Equity']
    # profile2 = profile2[profile2.is_etn == 0]
    # profile2 = profile2[profile2.is_active == 1]
    # name_list = gather_constituent_files()
    # name_list = list(set(name_list))
    # name_mapping = link_etfg_permno()

    # wrds = WRDS('fehouse')
    # # wrds.download_table('crsp', 'stocknames')
    #


