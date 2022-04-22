####
# functions for converting binance aggregated trade data to 5 second price data
####

from os import listdir
from os.path import isfile, join

import numpy as np
import pandas as pd


# import price data for ETH
def clean_binance_agg_data(df):
    """ convert aggregated trade data to minute data """
    df.columns = ['trade_id', 'price', 'q', 'first_tid', 'last_tid', 'time', 'buyer_maker']
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    df['n_trades'] = df['last_tid'] - df['first_tid']
    df = df[['time', 'price', 'q', 'n_trades']].set_index('time').groupby(pd.Grouper(freq='5s')).apply(
        lambda x: [np.sum(x['price'] * x['q']) / x['q'].sum(), x['q'].sum(), x['n_trades'].sum()]).reset_index()
    df['price'] = df[0].apply(lambda x: x[0])
    df['q'] = df[0].apply(lambda x: x[1])
    df['n_trades'] = df[0].apply(lambda x: x[2])

    return df.drop(columns=[0])


def make_binance_data_csv(dir='./binance_eth_agg_data/', file_name='binance_eth_data.csv'):
    onlyfiles = [dir + f for f in listdir(dir) if isfile(join(dir, f)) and '.DS_Store' not in f]
    dfs = []
    for f in onlyfiles:
        dfs.append(clean_binance_agg_data(pd.read_csv(f)))
        print(f)
    df = pd.concat(dfs)

    df.sort_values(by='time').reset_index(drop=True).to_csv(file_name)


def import_binance_data(csv='binance_eth_data.csv'):
    df = pd.read_csv(csv)
    df = df[['time', 'price']]
    df['time'] = pd.to_datetime(df['time'])

    return df.set_index('time')


