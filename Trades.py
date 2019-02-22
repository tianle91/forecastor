# NO SPARK HERE, ONLY PANDAS
import time
import numpy as np
import pandas as pd


def sdweighted(x, weights):
    mean = np.average(x, weights=weights)
    meanofsq = np.average(np.power(x, 2), weights=weights)
    return np.sqrt(meanofsq - np.power(mean, 2))


def features_gpbyprice(df, verbose=0):
    '''return features of all trades aggregated by price'''
    ntrades = len(df)
    dfgpbyprx = df.groupby('price').agg({'quantity': 'sum'})
    #print (dfgpbyprx)

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

    out = {
        'mean': mean, 
        'sd': sd, 
        'Number-of-Trades': ntrades, 
        'Quantity-Traded': tradeq, 
        'AvgVol': qtypertrade}

    return out


def filltrfm(aggdf, transfermatrix):
    '''return np.array.tolist() of transfer of traded stocks
    '''
    m = transfermatrix.copy()
    for index, row in aggdf.iterrows():
        buybro, sellbro = index
        m.loc[sellbro, buybro] += float(row[0])
    return m.tolist()


def features_gpbybroker(df, transfermatrix, verbose=0):
    '''return features of all trades aggregated by broker'''
    tradecounts = df.groupby(['buy_broker', 'sell_broker']).agg({'time': 'count'})
    tradevolume = df.groupby(['buy_broker', 'sell_broker']).agg({'quantity': 'sum'})

    out = {
        'Counts': filltrfm(tradecounts, transfermatrix), 
        'Volume': filltrfm(tradevolume, transfermatrix)}
    return out


class Trades(object):

    def __init__(self, df, verbose=0):
        if type(df) is not pd.DataFrame:
            raise TypeError('df is not pd.DataFrame!')
        elif not {'price', 'quantity', 'buy_broker', 'sell_broker'}.issubset(df.columns):
            raise ValueError('''['price', 'quantity', 'buy_broker', 'sell_broker'] not in df.columns!''')
        else:
            df = df.astype({
                'price': float, 
                'quantity': float,
                'buy_broker': int,
                'sell_broker': int})
            df = df.astype({
                'buy_broker': str,
                'sell_broker': str})
            self.df = df
            self.verbose = verbose


    def features(self, transfermatrix):
        '''return dict of features'''
        out = {'gpbyprice': features_gpbyprice(self.df, verbose=self.verbose)}
        out['gpbybroker'] = features_gpbybroker(self.df, transfermatrix, verbose=self.verbose)
        if self.verbose > 0:
            print (out)
        return out