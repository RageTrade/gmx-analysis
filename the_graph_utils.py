####
# note: the main notebook does not use this code, but it enables querying GMX's The Graph Api
####

import numpy as np
import pandas as pd
import requests

GRAPH_URL = 'https://api.thegraph.com/subgraphs/name/gmx-io/gmx-stats'
GMX_GRAPH_URL = 'https://api.thegraph.com/subgraphs/name/gkrasulya/gmx'


def query_the_graph(query, url):
    """ general query for The Graph """
    request = requests.post(url,
                            '',
                            json={'query': query})
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception('Query failed. return code is {}.      {}'.format(request.status_code, query))


def query_gmx(query):
    return query_the_graph(query, GMX_GRAPH_URL)


def format_query(last_id):
    return """
{
  orders(first: 1000, where: {id_gt: "%s"}) {
    id
    type
    account
    status
    index
    size
    createdTimestamp
    cancelledTimestamp
    executedTimestamp
  }
}
""" % (last_id)


def format_swap_query(last_id):
    return """
{
  swaps(first: 1000, where: {id_gt: "%s"}){
    id
    account
    tokenIn
    tokenOut
    amountIn
    amountOut
    amountOutAfterFees
    feeBasisPoints
    tokenInPrice
    timestamp
  }
}
""" % (last_id)


def type_cast_df(df, time_col='createdTimestamp'):
    df['date'] = pd.to_datetime(df[time_col], unit='s')

    return df


def clean_responses(responses, col='orders', time_col='createdTimestamp'):
    df = pd.concat([pd.DataFrame(r['data'][col]) for r in responses]).reset_index(drop=True) # .drop_duplicates(subset=['id']).reset_index(drop=True)
    df = type_cast_df(df, time_col=time_col).sort_values(by='date').reset_index(drop=True)

    return df

####
# Entrypoint functions
####


def query_swaps():
    last_id = ''
    responses = []
    try:
        for i in range(100):
            r = query_gmx(format_swap_query(last_id))
            responses.append(r)
            df = pd.DataFrame(r['data']['swaps'])
            last_id = df['id'].max()
    except:
        pass

    return clean_responses(responses, col='swaps', time_col='timestamp')


def query_orders():
    last_id = ''
    responses = []
    try:
        for i in range(100):
            r = query_gmx(format_query(last_id))
            responses.append(r)
            df = pd.DataFrame(r['data']['orders'])
            last_id = df['id'].max()
    except:
        pass

    return clean_responses(responses)
    