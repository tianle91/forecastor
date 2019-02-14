# NO SPARK HERE.
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def touch(df):
    '''return tuple of (bestbid, bestask)'''
    bestbid, bestask = (0, 99999)
    if len(df) > 0:
        bestbid = max(df.loc[df['side'] == 'Buy', 'price'])
        bestask = min(df.loc[df['side'] == 'Sell', 'price'])
    return {'bestbid': bestbid, 'bestask': bestask}


class Book(object):

    def __init__(self, df, verbose=0):
        '''initialize with dataframe and touch'''
        if type(df) is not pd.DataFrame:
            raise TypeError('df is not pd.DataFrame!')
        if not {'side', 'price', 'quantity'}.issubset(df.columns):
            raise ValueError('df does not have side, price, quantity columns!')

        if verbose > 0:
            print ('len(df):', len(df))
        self.df = df.astype({'price': float, 'quantity': float})
        self.touch = touch(self.df)


    def updatebook(self, dfqchange):
        '''return df new Book with updated quantities'''
        newdf = self.df.copy()
        newdf = newdf.merge(dfqchange, on=['price', 'side'], how='outer', suffixes=('_bk', '_bkch'))
        newdf['quantity'] = newdf['quantity_bk'] + newdf['quantity_bkch']
        return Book(newdf[['price', 'side', 'quantity']])


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


    def prices(self):
        '''return predictions of price based on orderbook'''
        out = {}

        # midprice
        bestbid = self.touch['bestbid']
        bestask = self.touch['bestask']
        out['mid'] = prxmid = .5*(bestask + bestbid)

        # weighted price based on volume at touch
        sumqbat = np.sum(self.df.loc[self.isbat(), 'quantity'])
        sumqsat = np.sum(self.df.loc[self.issat(), 'quantity'])
        prxwgt = sumqsat*bestbid + sumqbat*bestask
        prxwgt = prxwgt/(sumqsat+sumqbat)
        out['weighted-volume-at-touch'] = prxwgt

        return out

    def features(self):
        '''return dict of orderbook features'''
        bestbid = self.touch['bestbid']
        bestask = self.touch['bestask']
        spread = bestask - bestbid

        out = {'bestbid': bestbid, 
               'bestask': bestask,
               'spread': spread,
               'prices': self.prices()}

        return out


    def depthview(self, viewpct=.8, plot=True):
        '''return dict to construct depth view'''
        bestbid = self.touch['bestbid']
        bestask = self.touch['bestask']

        if not (bestbid < bestask):
            print ('not (bestbid:%s < bestask:%s)' % (bestbid, bestask))

        bk = self.df.copy()
        isbuy = bk['side'] == 'Buy'
        issell = bk['side'] == 'Sell'

        # only plot somewhere around bestbid, bestask
        plotlow, plothigh = (viewpct*bestbid, (1.+(1.-viewpct))*bestask)
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