# NO SPARK HERE, ONLY PANDAS
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def touch(df):
    '''return tuple of (bestbid, bestask)'''
    if len(df) > 0:
        maxbid = max(df.loc[df['side'] == 'Buy', 'price'])
        minask = min(df.loc[df['side'] == 'Sell', 'price'])
        if maxbid < minask:
            return maxbid, minask
        else:
            raise ValueError('df has maxbid >= minask!')
    else:
        raise ValueError('df is empty!')


class Book(object):

    def __init__(self, df, verbose=0):
        '''initialize with dataframe and touch'''

        if type(df) is not pd.DataFrame:
            raise TypeError('df is not pd.DataFrame!')
        elif not {'side', 'price', 'quantity'}.issubset(df.columns):
            raise ValueError('df does not have side, price, quantity columns!')
        else:
            self.df = df.astype({'price': float, 'quantity': float})

        if len(self.df) > 0:
            self.maxbid, self.minask = touch(self.df)

        self.verbose = verbose


    def isbat(self):
        '''return bool of buy-at-touch'''
        return np.logical_and(
            self.df['side'] == 'Buy', 
            self.df['price'] >= self.maxbid)


    def issat(self):
        '''return bool of sell-at-touch'''
        return np.logical_and(
            self.df['side'] == 'Sell', 
            self.df['price'] <= self.minask)


    def prices(self):
        '''return predictions of price based on orderbook'''
        out = {}

        # midprice
        out['mid'] = .5*(self.maxbid + self.minask)

        # weighted price based on volume at touch
        sumqbat = np.sum(self.df.loc[self.isbat(), 'quantity'])
        sumqsat = np.sum(self.df.loc[self.issat(), 'quantity'])
        # compute weights
        if sumqbat+sumqsat > 0:
            prxwgt = sumqsat*self.maxbid + sumqbat*self.minask
            prxwgt = prxwgt/(sumqsat+sumqbat)
        else:
            prxwgt = 0
        out['weighted-volume-at-touch'] = prxwgt

        return out


    def features(self):
        '''return dict of orderbook features'''
        out = None
        if len(self.df) > 0:
            out = {
                'maxbid': self.maxbid,
                'minask': self.minask,
                'spread': self.minask - self.maxbid,
                'prices': self.prices()}
        if self.verbose > 0:
            print (out)
        return out


    def depthview(self, viewpct=.8, plot=True):
        '''return dict to construct depth view'''
        bk = self.df.copy()
        isbuy = bk['side'] == 'Buy'
        issell = bk['side'] == 'Sell'

        # only plot somewhere around bestbid, bestask
        plotlow, plothigh = (viewpct*self.maxbid, (1.+(1.-viewpct))*self.minask)
        isplot = np.logical_and(bk['price'] > plotlow, bk['price'] < plothigh)
        bk = bk.loc[isplot, :]

        buycum = np.cumsum(bk.loc[isbuy, 'quantity'].values[::-1])[::-1]
        sellcum = np.cumsum(bk.loc[issell, 'quantity'].values)

        out = {'bidp': bk.loc[isbuy, 'price'], 'bidq': buycum,
               'askp': bk.loc[issell, 'price'], 'askq': sellcum}

        if plot:
            plt.plot(out['bidp'], out['bidq'], color='green', alpha=.5)
            plt.plot(out['askp'], out['askq'], color='red', alpha=.5)
            plt.title('Depth View')
            plt.show()
        else:
            return out