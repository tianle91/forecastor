import numpy as np
import pandas as pd


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
        sd = np.average(np.power(prx, 2), weights=wgt)
        sd = np.sqrt(sd - np.power(mean, 2))
        tradeq = np.sum(qty)
        qtypertrade = tradeq/ntrades

    out = {'mean': mean, 
        'sd': sd, 
        'Number-of-Trades': ntrades, 
        'Quantity-Traded': tradeq, 
        'AvgVol': qtypertrade}

    return out


def features_gpbybroker(df):
    '''return features of all trades aggregated by broker'''

    def worker(brokercolname):
        print (df.groupby(brokercolname).agg('count'))
        
    pass


def features(df):
    '''return dict of features'''
    if type(df) is not pd.DataFrame:
        raise TypeError('df is not pd.DataFrame!')
    elif not {'price', 'quantity', 'buy_broker', 'sell_broker'}.issubset(df.columns):
        raise ValueError('''['price', 'quantity', 'buy_broker', 'sell_broker'] not in df.columns!''')
    else:
        df = df.astype({'price': float, 'quantity': float})

    out = {'gpbyprice': features_gpbyprice(df),
        'gpbybroker': features_gpbybroker(df)}

    return out