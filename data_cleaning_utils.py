import json
import random
import requests
import os
from time import sleep

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import ast


TOKEN_ADDR_TO_NAME = {
    '0x82af49447d8a07e3bd95bd0d56f35241523fbab1': 'WETH',
    '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8': 'USDC',
    '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1': 'DAI',
    '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f': 'WBTC',
    '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9': 'USDT',
    '0xf97f4df75117a78c1a5a0dbb814af92458539fb4': 'LINK',
    '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0': 'UNI',
    '0xfea7a6a0b346362bf88a9e4a88416b77a57d6c2a': 'MIM',
    '0x17fc002b466eec40dae837fc4be5c67993ddbd6f': 'FRAX'
}


def parse_liquidation(liq):
    try:
        account = liq['account']
        return True
    except:
        return False


def string_to_dict(x):
    try:
        return ast.literal_eval(x)
    except:
        return None


def drop_unused_cols(df):
    return df.drop(columns=['timestamp', 'collateralToken', 'indexToken', 'settledTimestamp', '__typename',
                            'liquidatedPosition'])


def clean_trade_data(df):
    df['collateralTokenName'] = df['collateralToken'].apply(lambda x: TOKEN_ADDR_TO_NAME[x])
    df['indexTokenName'] = df['indexToken'].apply(lambda x: TOKEN_ADDR_TO_NAME[x])
    df['averagePrice'] = df['averagePrice'].astype(float) / 10**30
    df['realisedPnl'] = df['realisedPnl'].astype(float) / 10**30
    df['tradeOpenDate'] = pd.to_datetime(df['timestamp'], unit='s')
    df['tradeClosedDate'] = pd.to_datetime(df['settledTimestamp'], unit='s')
    df['collateral'] = df['collateral'].astype(float) / 10**30
    df['sizeDelta'] = df['sizeDelta'].astype(float) / 10**30
    df['collateralDelta'] = df['collateralDelta'].astype(float) / 10**30
    df['size'] = df['size'].astype(float) / 10**30
    df['fee'] = df['fee'].astype(float) / 10**30
    df['increaseList'] = df['increaseList'].apply(string_to_dict)
    df['decreaseList'] = df['decreaseList'].apply(string_to_dict)
    df['updateList'] = df['updateList'].apply(string_to_dict)
    df['closedPosition'] = df['closedPosition'].apply(string_to_dict)
    df['liquidatedPosition'] = df['liquidatedPosition'].apply(string_to_dict)
    df['positionLiquidated'] = df['liquidatedPosition'].apply(parse_liquidation)
    return drop_unused_cols(df)


def clean_positions(df):
    df['collateralToken'] = df['collateralToken'].apply(lambda x: TOKEN_ADDR_TO_NAME[x])
    df['indexToken'] = df['indexToken'].apply(lambda x: TOKEN_ADDR_TO_NAME[x])
    df['time'] = pd.to_datetime(df['timestamp'], unit='s')
    df['sizeDelta'] = df['sizeDelta'].astype(float) / 10**30
    df['price'] = df['price'].astype(float) / 10**30
    df['collateralDelta'] = df['collateralDelta'].astype(float) / 10**30
    df['fee'] = df['fee'].astype(float) / 10**30
    df['position_type'] = df['__typename']
    return df[['time', 'account', 'collateralToken', 'indexToken', 'isLong', 'position_type', 'sizeDelta', 'price',
               'collateralDelta', 'fee', 'id', 'key', 'timestamp']].sort_values(by='time').reset_index(drop=True)


####
# binance data helper functions
####


def sign(x):
    if x:
        return 1
    else:
        return -1

def get_trade_direction(row):
    """ returns +1 if long -1 if short """
    isLong = - sign(row['isLong'])
    position_type = - sign(row['position_type'] == 'IncreasePosition')
    return isLong * position_type


def get_max_price_edge(row):
    binance_price = row['max_binance_price'] if row['trade_direction'] == 1 else row['min_binance_price']
    price_diff =  100 * (row['price'] / binance_price - 1)
    return - row['trade_direction'] * (price_diff / (price_diff + 100)) * 100


def select_only_eth_trades(trade_data, start_time, end_time):
    eth_trade_data = trade_data.loc[trade_data['indexToken'] == 'WETH'].reset_index(drop=True)
    eth_trade_data = eth_trade_data.loc[(eth_trade_data['time'] >= start_time) &
                                        (eth_trade_data['time'] <= end_time)].reset_index(drop=True)
    return eth_trade_data


def clean_binance_data(binance_df, timestamp_uncertainty):
    binance_df = binance_df.reindex(
        pd.date_range(start=binance_df.index[0], end=binance_df.index[-1], freq='1s')).interpolate()
    binance_df = binance_df.reset_index().rename(columns={'index': 'time', 'price': 'binance_price'})
    binance_df['min_binance_price'] = binance_df['binance_price'].rolling(
        timestamp_uncertainty, min_periods=0).min().shift(-timestamp_uncertainty).fillna(binance_df['binance_price'])
    binance_df['max_binance_price'] = binance_df['binance_price'].rolling(
        timestamp_uncertainty, min_periods=0).max().shift(-timestamp_uncertainty).fillna(binance_df['binance_price'])
    return binance_df


####
# entrypoint functions
####


def import_trade_data():
    trade_data = pd.read_csv('gmx_trade_data.csv').drop(columns=['Unnamed: 0'])
    return clean_trade_data(trade_data)


def get_individual_trades(trade_data):
    position_increases = trade_data['increaseList'].explode().apply(pd.Series).reset_index(drop=True)
    position_decreases = trade_data['decreaseList'].explode().apply(pd.Series).reset_index(drop=True)
    return clean_positions(pd.concat([position_increases, position_decreases]).drop(
        columns=[0]).dropna().reset_index(drop=True))


def merge_eth_trade_data_with_binance(trade_data, binance_df, timestamp_uncertainty=120):
    eth_trade_data = select_only_eth_trades(trade_data, binance_df.index[0], binance_df.index[-1])
    binance_df = clean_binance_data(binance_df, timestamp_uncertainty)

    merged_eth_trade_data = eth_trade_data.merge(binance_df, how='left', on='time')
    merged_eth_trade_data['trade_direction'] = merged_eth_trade_data.apply(get_trade_direction, axis=1)
    merged_eth_trade_data['price_edge'] = merged_eth_trade_data.apply(get_max_price_edge, axis=1)

    return merged_eth_trade_data