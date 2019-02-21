import time
import pandas as pd
import numpy as np
import sparkdfutils as utils
from Book import Book
from Orders import Orders


def dailyorders(symbol, date_string, venue):
    '''return table of orders'''
    s = '''SELECT
            book_change, 
            side, 
            price,
            reason,
            time
        FROM orderbook_tsx 
        WHERE symbol = '%s' 
            AND date_string = '%s' 
            AND price > 0
            AND venue = '%s'
        ORDER BY time ASC'''
    sargs = (symbol, date_string, venue)
    return spark.sql(s%sargs) 


def orderbook(ordersdf, verbose=0):
    '''return table of orders agg by side, price'''
    bk = ordersdf.groupby(['side', 'price']).agg({'book_change': 'sum'})
    bk = bk.withColumnRenamed('sum(book_change)', 'quantity')
    bk = bk.filter('quantity != 0')
    bk = bk.orderBy('price')
    if verbose > 0:
        print ('len:', bk.count())
    return bk


def features(symbol, date_string, venue = 'TSX',
    freq = '1min', tstart_string = '09:30', tend_string = '16:00',
    verbose = 0):
    '''return dict of features of orderbook and new orders
    features at HH:MM are computed using orderbook [00:00, HH:MM) and 
    orders that came in during [HH:MM, HH:(MM+1))

    Args:
        symbol: str of ticker
        date_string: str of YYYY-MM-DD
        venue: str of exchange
        freq: pandas freq (e.g. ['1H', '30min', '5min', '1min'])
        tstart_string: str of 'HH:MM' for start time
        tend_string: str of 'HH:MM' for end time
    '''
    if verbose > 0:
        t0 = time.time()

    tradingtimes = utils.tradingtimes(date_string, 
        tstart_string, tend_string, freq, tz='US/Eastern')

    # get all transactions prior to tradingtimes[-1]
    dfday = dailyorders(symbol, date_string, venue)
    dfday.cache()

    if verbose > 0:
        print ('cached orders for %s done in: %.2f norders: %d' %\
            (date_string, time.time()-t0, dfday.count()))


    # book features
    if verbose > 1:
        t1 = time.time()
        print ('doing book features')
    bookfeatures = {}
    for dt in tradingtimes:
        if verbose > 1:
            t2 = time.time()
        dftemp = utils.subsetbytime(dfday, dt).toPandas()
        bookfeatures[dt] = Book(dftemp, verbose=verbose-1).features()
        if verbose > 1:
            print ('dt: %s done in: %.2f' % (dt, time.time()-t2))
    if verbose > 1:
        print ('book features done in: %.2f' % (time.time()-t1))


    # orders features
    if verbose > 1:
        t1 = time.time()
        print ('doing orders features')
    ordersfeatures = {}
    ordersfeatures[-1] = None
    dt = tradingtimes[0]
    for dtnext in tradingtimes[1:]:
        if verbose > 1:
            t2 = time.time()
        dftemp = utils.subsetbytime(dfday, dt, dtnext).toPandas()
        touchval = bookfeatures[dt]['maxbid'], bookfeatures[dt]['minask']
        ordersfeatures[dt] = Orders(dftemp, verbose=verbose-1).features(touchval)
        dt = dtnext
        if verbose > 1:
            print ('dt: %s done in: %.2f' % (dt, time.time()-t2))
    if verbose > 1:
        print ('orders features done in: %.2f' % (time.time()-t1))

    out = {}
    for dt in tradingtimes:
        out[dt] = {'book': bookfeatures[dt], 'orders': ordersfeatures[dt]}
    if verbose > 0:
        print ('all book / orders features done in: %.2f' % (time.time()-t0))
    
    return out


if __name__ == '__main__':

    params = {
        'symbol': 'TD',
        'date_string': '2019-02-04',
        'freq': '1H',
        'tstart_string': '10:00',
        'tend_string': '12:00',
        'verbose': 1}

    x = features(**params)