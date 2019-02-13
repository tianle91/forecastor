import numpy as np
import pandas as pd


def features_gpbyprice(df):
    '''return features of all trades grouped by price'''
    if type(df) is not pd.DataFrame:
        raise TypeError('df is not pd.DataFrame!')
    elif not {'price', 'quantity'}.issubset(df.columns):
        raise ValueError('''['price', 'quantity'] not in df.columns!''')
    else:
        df = df.astype({'price': float, 'quantity': float})

    ntrades = len(df)
    dfgpbyprx = df.groupby('price')['quantity'].agg('sum')
    print (dfgpbyprx)

    prx = df['price']
    qty = df['quantity']
    wgt = qty / np.sum(qty)

    mean = 0
    stdev = 0
    tradeq = 0
    qtypertrade = 0

    if tradecount > 0:
        mean =  np.average(prx, weights=wgt)
        stdev = np.average(np.power(prx, 2), weights=wgt)
        stdev = np.sqrt(stdev - np.power(mean, 2))
        tradeq = np.sum(qty)
        qtypertrade = tradeq/tradecount

    out = {'mean': mean, 
           'sd': stdev, 
           'Number-of-Trades': tradecount, 
           'Quantity-Traded': tradeq, 
           'AvgVol': qtypertrade}

    return out


def features_gpbybroker(df):
    '''return features of all trades grouped by broker'''
    if type(df) is not pd.DataFrame:
        raise TypeError('df is not pd.DataFrame!')
    elif not {'buy_broker', 'sell_broker'}.issubset(df.columns):
        raise ValueError('''['buy_broker', 'sell_broker'] not in df.columns!''')
    pass


def features(df):
    out = {'gpbyprice': features_gpbyprice(df)}
    return out