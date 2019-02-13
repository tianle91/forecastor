import numpy as np
import pandas as pd


def sdweighted(x, weights):
    mean = np.average(x, weights=weights)
    meanofsq = np.average(np.power(x, 2), weights=weights)
    return np.sqrt(meanofsq - np.power(mean, 2))


def features_gpbyprice(df):
    '''return features of all trades aggregated by price'''
    ntrades = len(df)
    dfgpbyprx = df.groupby('price').agg({'quantity': 'sum'})
    print (dfgpbyprx)

    prx = df['price']
    qty = df['quantity']
    wgt = qty / np.sum(qty)

    mean = 0
    sd = 0
    tradeq = 0
    qtypertrade = 0

    if ntrades > 0:
        mean =  np.average(prx, weights=wgt)
        sd = sdweighted(prx, weights=wgt)
        tradeq = np.sum(qty)
        qtypertrade = tradeq/ntrades

    out = {'mean': mean, 
        'sd': sd, 
        'Number-of-Trades': ntrades, 
        'Quantity-Traded': tradeq, 
        'AvgVol': qtypertrade}

    return out


def features_gpbybroker(df, transfermatrix):
    '''return features of all trades aggregated by broker'''

    tradecounts = df.groupby(['buy_broker', 'sell_broker']).count()
    tfmcounts = transfermatrix.copy()
    tradevolume = df.groupby(['buy_broker', 'sell_broker'])['quantity'].sum()
    tfmvolume = transfermatrix.copy()
    return {tradecounts, tradevolume}


def features(df, transfermatrix):
    '''return dict of features'''
    if type(df) is not pd.DataFrame:
        raise TypeError('df is not pd.DataFrame!')
    elif not {'price', 'quantity', 'buy_broker', 'sell_broker'}.issubset(df.columns):
        raise ValueError('''['price', 'quantity', 'buy_broker', 'sell_broker'] not in df.columns!''')
    else:
        df = df.astype({'price': float, 'quantity': float})

    out = {'gpbyprice': features_gpbyprice(df),
        'gpbybroker': features_gpbybroker(df, transfermatrix)}

    return out