#import pyspark as spark
import numpy as np
import pandas as pd


def orderbook(symbol, timestamp, venue='TSX', lazy=False):
    ''' return pandas table of order book '''
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
    
    if not lazy:
        df = df.toPandas()
        df['price'] = df['price'].astype(float)
        df['quantity'] = df['quantity'].astype(float)
        
    return df


def get_touch(orderbook):
    '''return (best_bid, best_ask)'''
    bestbid = max(orderbook.loc[orderbook['side'] == 'Buy', 'price'])
    bestask = min(orderbook.loc[orderbook['side'] == 'Sell', 'price'])
    return bestbid, bestask

    
def orderbook_features(orderbookdf):
    '''return orderbook features'''
    # Spread, Midprice, Touch
    bestbid, bestask = get_touch(orderbookdf)
    spread = bestask - bestbid
    midprice = .5*(bestask + bestbid)
    return {'spread': spread, 'midprice': midprice, 'bestbid': bestbid, 'bestask': bestask}
    
    
def orders(symbol, timestamp0, timestamp1, venue='TSX', lazy=False):
    ''' return pandas table of new orders '''
    
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
        
    df = spark.sql(s % sargs)
    
    if not lazy:
        df = df.toPandas()
        df['price'] = df['price'].astype(float)
        df['book_change'] = df['book_change'].astype(float)

    return df


def count_stuff(ordersdf, ordtype='new', side='Buy', counttype='orders', touch=None):
    '''return count of counttype in order table
    Arguments:
        ordersdf: table of orders
        ordtype: 'new' or 'cancelled'
        side: 'Buy' or 'Sell' or 'All'
        counttype: 'orders' or 'quantity' or 'qperorder'
        touch: None or (bestbuy, bestask)
    '''
    
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
    

def orders_features(ordersdf, touchval):
    # ordtype:{new, cancelled} x side:{buy, sell} x counttype:{orders, quantity} x touch:{None, touch}
    argl = [{'ordtype': ordtype, 'side': side, 'counttype': counttype, 'touch': touch}
            for ordtype in ['new', 'cancelled']
            for side in ['Buy', 'Sell', 'All']
            for counttype in ['orders', 'quantity', 'qperorder']
            for touch in [None, touchval]]

    out = {}
    for s in argl:
        out[str(s)] = count_stuff(ordersdf, **s)

    return out
    

def trades(symbol, timestamp0, timestamp1, venue='TSX', lazy=False):
    '''return table of trades'''
    
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
        
    df = spark.sql(s % sargs)
    
    if not lazy:
        df = df.toPandas()
        df['price'] = df['price'].astype(float)
        df['quantity'] = df['quantity'].astype(float)

    return df
    

def trades_features(tradesdf):
    # mean, stdev, traded_count, traded_qty
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
    
    
def features(symbol, timestamp0, timestamp1):
    '''return dict of all features'''
    
    oldbk = orderbook(symbol, timestamp0)
    neworders = orders(symbol, timestamp0, timestamp1)
    newtrades = trades(symbol, timestamp0, timestamp1)

    out = {'bk0': orderbook_features(oldbk),
           'neworders': orders_features(neworders, get_touch(oldbk)), 
           'newtrades': trades_features(newtrades)}
           
    return out