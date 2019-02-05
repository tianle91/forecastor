%pyspark


#import pyspark as spark
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def orderbook(symbol, timestamp, venue='TSX'):
    '''return pandas table of order book'''
    s = '''SELECT
            SUM(book_change) AS quantity, 
            side, 
            price
        FROM orderbook_tsx 
        WHERE symbol='%s' 
            AND date_string='%s' 
            AND time <= timestamp '%s'
            AND venue = '%s'
            AND price > 0
            AND price < 99999
        GROUP BY side, price 
        ORDER BY price ASC'''
    
    sargs = (
        symbol,
        timestamp.split()[0],
        timestamp,
        venue)
        
    df = spark.sql(s % sargs)
    
    # save as temp table, return where qty>0
    df.createOrReplaceTempView('orderbook')
    df = spark.sql('SELECT * FROM orderbook WHERE quantity > 0')
    
    df = df.toPandas()
    df['price'] = df['price'].astype(float)
    df['quantity'] = df['quantity'].astype(float)
        
    return df


class Book(object):

    def __init__(self, symbol, timestamp, venue='TSX'):
        '''df has columns (quantity, side, price)
        takes some time to run, bu should only need to be done once.
        '''
        self.df = orderbook(symbol, timestamp, venue)

        # make touch
        self.touch = (0, 99999)
        if len(df) > 0:
            bestbid = max(self.df.loc[self.df['side'] == 'Buy', 'price'])
            bestask = min(self.df.loc[self.df['side'] == 'Sell', 'price'])
            self.touch = bestbid, bestask


    def features(self, q=None):
        '''return orderbook features'''
        bestbid, bestask = self.touch
        spread = bestask - bestbid
        prxmid = .5*(bestask + bestbid)

        isbuyattouch = np.logical_and(self.df['side'] == 'Buy', self.df['price'] >= bestbid)
        qbuytouch = np.sum(self.df.loc[isbuyattouch, 'quantity'])
        issellattouch = np.logical_and(self.df['side'] == 'Sell', self.df['price'] <= bestask)
        qselltouch = np.sum(self.df.loc[issellattouch, 'quantity'])
        prxweightedtouch = qselltouch*bestbid + qbuytouch*bestask
        prxweightedtouch = prxweightedtouch/(qselltouch+qbuytouch)

        out = {'spread': spread, 
               'bestbid': bestbid, 
               'bestask': bestask,
               'prxmid': prxmid,
               'prxweightedtouch': prxweightedtouch}

        return out


    def updatebook(self, bkchanges):
        '''return new Book updated with bkchanges'''

        bkchanges = bkchanges.toPandas()

        if len(bkchanges) > 0:
            oldbk = self.df
            bkdfnew = oldbk.merge(bkchanges, on=['price', 'side'], how='outer').fillna(0)
            bkdfnew = bkdfnew.rename(columns={'quantity': 'oldq'})
            bkdfnew['quantity'] = bkdfnew['oldq'] + bkdfnew['sum(book_change)']
            bkdfnew = bkdfnew.loc[bkdfnew['quantity'] > 0, :]
            bkdfnew = bkdfnew.sort_values('price')
            return Book(bkdfnew[['side', 'price', 'quantity']])
        else:
            return self


    def depthview(self, viewpct=.5, plot=True):
        '''return dict to construct depth view'''

        features = self.features()
        bestbid = features['bestbid']
        bestask = features['bestask']

        bk = self.df
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
        
        return out