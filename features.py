#import pyspark as spark
import numpy as np
import pandas as pd


class Book(object):

    def __init__(self, symbol, timestamp, venue='TSX'):
        '''initialize self.df'''

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
        df.createOrReplaceTempView('orderbook')
        self.df = spark.sql('SELECT * FROM orderbook WHERE quantity > 0')

        self.makePandas()
        self.makeTouch()


    def makePandas(self):
        '''make pandas version of self.df'''
        df = self.df.toPandas()
        df['price'] = df['price'].astype(float)
        df['quantity'] = df['quantity'].astype(float)
        self.pandas = df


    def makeTouch(self):
        '''make self.touch'''
        orderbook = self.pandas
        bestbid = max(orderbook.loc[orderbook['side'] == 'Buy', 'price'])
        bestask = min(orderbook.loc[orderbook['side'] == 'Sell', 'price'])
        self.touch = (bestbid, bestask)


    def features(self):
        '''return orderbook features (i.e. spread, midprice, bestbid, bestask)'''
        bestbid, bestask = self.touch
        spread = bestask - bestbid
        midprice = .5*(bestask + bestbid)

        out = {'spread': spread, 
               'midprice': midprice, 
               'bestbid': bestbid, 
               'bestask': bestask}

        return out

    

class Orders(object):

    def __init__(self, symbol, timestamp0, timestamp1, venue='TSX'):
        '''initialize self.df'''

        if not (timestamp0 <= timestamp1):
            raise ValueError('not (timestamp0:%s <= timestamp1:%s)!' % (timestamp0, timestamp1))
            
        s = '''SELECT
                book_change, 
                side, 
                price
            FROM orderbook_tsx 
            WHERE symbol='%s' 
                AND date_string='%s' 
                AND time >  timestamp '%s'
                AND time <= timestamp '%s'
                AND venue = '%s'
                AND price > 0
                AND price < 99999
            ORDER BY price ASC'''
        
        sargs = (
            symbol,
            timestamp0.split()[0],
            timestamp0,
            timestamp1,
            venue)
            
        self.df = spark.sql(s % sargs)
        self.makePandas()


    def makePandas(self):
        '''make pandas version of self.df'''
        df = df.toPandas()
        df['price'] = df['price'].astype(float)
        df['book_change'] = df['book_change'].astype(float)
        self.pandas = df


    def count_stuff(self, ordtype='new', side='Buy', counttype='orders', touch=None):
        '''return count of counttype in order table
        Arguments:
            ordersdf: table of orders
            ordtype: 'new' or 'cancelled'
            side: 'Buy' or 'Sell' or 'All'
            counttype: 'orders' or 'quantity' or 'qperorder'
            touch: None or (bestbuy, bestask)
        '''
        ordersdf = self.pandas
        subsetbool = np.repeat(True, len(ordersdf))
    
        # ordtype
        if ordtype == 'new':
            subsetbool = ordersdf['book_change'] > 0
        elif ordtype == 'cancelled':
            subsetbool = ordersdf['book_change'] < 0
        else:
            raise ValueError('invalid orddtype argument: %s' % ordtype)
        
        # touch
        if touch is not None:
            bestbuy, bestask = touch
            isattouch = np.logical_or(ordersdf['price'] >= bestbuy, ordersdf['price'] <= bestask)
            subsetbool = np.logical_and(subsetbool, isattouch)

        # side
        if side in ['Buy', 'Sell']:
            subsetbool = np.logical_and(subsetbool, ordersdf['side'] == side)
        elif side == 'All':
            pass
        else:
            raise ValueError('invalid side argument: %s' % side)
        
        # do the subset
        df = ordersdf.loc[subsetbool, :]
        
        # counttype
        if counttype == 'orders':
            return len(df)
        elif counttype == 'quantity':
            return np.sum(np.abs(df['book_change']))
        elif counttype == 'qperorder':
            return np.sum(np.abs(df['book_change'])) / len(df)
        else:
            raise ValueError('invalid counttype argument: %s' % counttype)
    

    def features(self, touchval):
        '''return dict of features'''
        argl = [{'ordtype': ordtype, 'side': side, 'counttype': counttype, 'touch': touch}
                for ordtype in ['new', 'cancelled']
                for side in ['Buy', 'Sell', 'All']
                for counttype in ['orders', 'quantity', 'qperorder']
                for touch in [None, touchval]]

        out = {}
        for s in argl:
            out[str(s)] = self.count_stuff(**s)

        return out



class Trades(object):

    def __init__(self, symbol, timestamp0, timestamp1, venue='TSX'):
        '''initialize self.df'''

        if not (timestamp0 <= timestamp1):
            raise ValueError('not (timestamp0:%s <= timestamp1:%s)!' % (timestamp0, timestamp1))
            
        s = '''SELECT
                trade_size AS quantity, 
                price
            FROM trades 
            WHERE symbol='%s' 
                AND date_string='%s' 
                AND time >  timestamp '%s'
                AND time <= timestamp '%s'
                AND venue = '%s'
                AND listing_exchange = '%s'
                AND price > 0
                AND price < 99999
            ORDER BY price ASC'''
        
        sargs = (
            symbol,
            timestamp0.split()[0],
            timestamp0,
            timestamp1,
            venue, 
            venue)
            
        self.df = spark.sql(s % sargs)
        self.makePandas()


    def makePandas(self):
        '''make pandas version of self.df'''
        df = self.df.toPandas()
        df['price'] = df['price'].astype(float)
        df['quantity'] = df['quantity'].astype(float)
        self.pandas = df
    

    def features(self):
        '''return dict of features'''
        tradesdf = self.pandas
        prx = tradesdf['price']
        qty = tradesdf['quantity']
        
        mean = 0
        stdev = 0
        tradeq = 0
        tradecount = 0
        qtypertrade = 0
        
        if len(prx) > 0:
            mean =  np.average(prx, weights=qty)
            stdev = np.sqrt(np.average(np.power(prx, 2), weights=qty) - mean**2)
            tradeq = np.sum(qty)
            tradecount = len(prx)
            qtypertrade = tradeq/tradecount
            
        return {'mean': mean, 'stdev': stdev, 'traded_count': tradecount, 'traded_qty': tradeq, 'meanq_pertrade': qtypertrade}