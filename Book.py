# NO SPARK HERE.
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def touch(df):
    '''return tuple of (bestbid, bestask)'''

    if len(df) > 0:
        bestbid = max(self.df.loc[self.df['side'] == 'Buy', 'price'])
        bestask = min(self.df.loc[self.df['side'] == 'Sell', 'price'])
        touch = bestbid, bestask
    else:
        touch = (0, 99999)

    if not (bestbid < bestask):
        raise ValueError('not (bestbid < bestask)!')
    else:
        return {'bestbid': bestbid, 'bestask': bestask}


class Book(object):

    def __init__(self, df):

        if type(df) is not pd.DataFrame:
            return TypeError('df is not pd.DataFrame!')
        if not {'side', 'price', 'quantity'}.issubset(df.columns):
            return ValueError('df does not have side, price, quantity columns!')

        self.df = df
        self.touch = touch(df)


    def isbat(self):
        '''return bool of buy-at-touch'''
        bestbid = self.touch['bestbid']
        out = self.df['side'] == 'Buy'
        return np.logical_and(out, self.df['price'] >= bestbid)


    def issat(self):
        '''return bool of sell-at-touch'''
        bestask = self.touch['bestask']
        out = self.df['side'] == 'Sell'
        return np.logical_and(out, self.df['price'] <= bestask)


    def features(self):
        '''return dict of orderbook features'''
        bestbid = self.touch['bestbid']
        bestask = self.touch['bestask']

        spread = bestask - bestbid
        prxmid = .5*(bestask + bestbid)

        sumqbat = np.sum(self.df.loc[self.isbat(), 'quantity'])
        sumqsat = np.sum(self.df.loc[self.issat(), 'quantity'])
        prxwgt = sumqsat*bestbid + sumqbat*bestask
        prxwgt = prxweightedtouch/(sumqsat+sumqbat)

        out = {'bestbid': bestbid, 
               'bestask': bestask,
               'spread': spread,
               'prxmid': prxmid,
               'prxwgt': prxwgt}

        return out


    def depthview(self, viewpct=.8, plot=True):
        '''return dict to construct depth view'''
        bestbid = self.touch['bestbid']
        bestask = self.touch['bestask']

        bk = self.df.copy()
        isbuy = bk['side'] == 'Buy'
        issell = bk['side'] == 'Sell'

        plotlow, plothigh = (viewpct*bestbid, (1.+viewpct)*bestask)
        isplot = np.logical_and(bk['price'] > plotlow, bk['price'] < plothigh)
        bk = bk.loc[isplot, :]

        buycum = np.cumsum(bk.loc[isbuy, 'quantity'].values[::-1])[::-1]
        sellcum = np.cumsum(bk.loc[issell, 'quantity'].values)

        out = {'bidp': bk.loc[isbuy, 'price'],
               'bidq': buycum,
               'askp': bk.loc[issell, 'price'],
               'askq': sellcum}

        if plot:
            plt.plot(out['bidp'], out['bidq'], color='green', alpha=.5)
            plt.plot(out['askp'], out['askq'], color='red', alpha=.5)
            plt.title('Depth View')
            plt.show()
        else:
            return out