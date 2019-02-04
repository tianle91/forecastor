#import pyspark as spark
import numpy as np
import pandas as pd


class Book(object):

    def __init__(self, df):
        '''df has columns (quantity, side, price)
        takes some time to run, bu should only need to be done once.
        '''
        self.df = df

        # make touch
        askprx = self.df.filter(self.df.side == 'Sell')
        bidprx = self.df.filter(self.df.side == 'Buy')
        bestask = askprx.agg({'price': 'min'}).collect()[0]['min(price)']
        bestbid = bidprx.agg({'price': 'max'}).collect()[0]['max(price)']
        self.touch = (float(bestbid), float(bestask))


    def features(self, q=None):
        '''return orderbook features (i.e. spread, midprice, bestbid, bestask)'''
        bestbid, bestask = self.touch
        spread = bestask - bestbid
        midprice = .5*(bestask + bestbid)

        out = {'spread': spread, 
               'midprice': midprice, 
               'bestbid': bestbid, 
               'bestask': bestask}

        return out


    def updatebook(self, ordersdf):
        '''return new orderbook updated with ordersdf'''

        # aggregate new orders to get change in orderbook
        bookdelta = ordersdf.groupBy('side', 'price').agg({'book_change': 'sum'})

        # join new orders with change qty to old orderbook
        oldbk = self.df
        newbk = oldbk.join(other=bookdelta, 
                           on=(oldbk.price == bookdelta.price) & (oldbk.side == bookdelta.side),
                           how='outer')

        # rename colnames and update to get new orderbook
        newbk = newbk.withColumnRenamed('quantity', 'qold')
        newbk = newbk.withColumnRenamed('sum(book_change)', 'qdelta')
        newbk = newbk.withColumn('quantity', newbk.qold + newbk.qdelta)
        newbk = newbk.select([oldbk.side, oldbk.price, 'quantity'])
        newbk = newbk.filter('quantity > 0').orderBy('price')
        return Book(newbk)



    def depthview(self, midppct=.5):
        '''return dict to construct depth view'''
        features = self.features()
        midp = features['midprice']

        bk = self.toPandas()
        isbuy = bk['side'] == 'Buy'
        issell = bk['side'] == 'Sell'

        plotlow, plothigh = (midppct, 1+midppct)
        plotprice = np.logical_and(bk['price'] > plotlow*midp, bk['price'] < plothigh*midp)
        bk = bk.loc[plotprice, :]

        buycum = np.cumsum(bk.loc[isbuy, 'quantity'].values[::-1])[::-1]
        sellcum = np.cumsum(bk.loc[issell, 'quantity'].values)

        out = {'bidp': bk.loc[isbuy, 'price'],
               'bidq': buycum,
               'askp': bk.loc[issell, 'price'],
               'askq': sellcum}
        
        return out