import time
import pandas as pd
import numpy as np

import sparkdfutils as utils
import orders_functions as ordfn
from Orderbook import Book


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
    bk = bk.filter('quantity > 0')
    bk = bk.orderBy('price')
    if verbose > 0:
        print ('len:', bk.count())
    return bk


def updateiter(ordersprev, ordersnew, bkold=None):
    '''return bknew, bkft, ordft
    # -------------------------------------------------- #
    #    dtprev               dt                  dtnext #
    # -------------------------------------------------- # 
    # -- | --- ordersprev --- | --- ordersnew --- | ---- # 
    # -------------------------------------------------- # 
    #    bkold                bknew:bkft                 #
    # -------------------------------------------------- # 
    #                         | --- ordft ------- | ---- # 
    # -------------------------------------------------- #
    '''
    nordprev = ordersprev.count()
    if nordprev > 0:
        # only update if there are ordersprev
        bkch = orderbook(ordersprev).toPandas()
        if bkold is None:
            bknew = Book(bkch)
        else:
            bknew = bkold.updatebook(bkch)
    else:
        if bkold is None:
            raise ValueError('ordersprev.count()==0 and bkold is None')
        else:
            bknew = bkold
    # get book features
    bkft = bknew.features()

    nordnew = ordersnew.count()
    if nordnew > 0:
        # only get order features if there are ordersnew
        touchtemp = bkft['bestbid'], bkft['bestask']
        ordft = ordfn.features(ordersnew.toPandas(), touchtemp)
    else:
        ordft = None

    return bknew, bkft, ordft


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
        t0all = time.time()
        t0 = time.time()

    tradingtimes = utils.tradingtimes(date_string, 
        tstart_string, tend_string, freq, tz='US/Eastern')

    # get all transactions prior to tradingtimes[-1]
    dfday = dailyorders(symbol, date_string, venue)
    dfday = utils.subsetbytime(dfday, tradingtimes[-1])
    dfday.cache()
    if verbose > 0:
        print ('get dailyorders done in: %.2f norders: %d before: %s' %\
            (time.time()-t0, dfday.count(), tend_string))

    # count number of new orders
    tstartdt = tradingtimes[0]
    tenddt = tradingtimes[-1]
    nordersnew = utils.subsetbytime(dfday, tstartdt, tenddt).count()

    if verbose > 0:
        print ('freq:%s tstart: %s tend: %s len(tradingtimes): %d' %\
            (freq, tstart_string, tend_string, len(tradingtimes)))


    # orderbook, neworders features
    # --------------------------------------------------------------------------
    if verbose > 0:
        print ('running features for all dt in tradingtimes...')
    
    t1 = time.time()
    bkordfeatures = {}
    bkordfeatures[tradingtimes[-1]] = None

    # initialize
    dt = tradingtimes[0]
    dtnext = tradingtimes[1]
    ordersprev = utils.subsetbytime(dfday, dt)
    ordersnew = utils.subsetbytime(dfday, dt, dtnext)
    # update
    bk, bkft, ordft = updateiter(ordersprev, ordersnew)
    bkordfeatures[dt] = {'book': bkft, 'orders': ordft}

    dtprev = tradingtimes[0]
    dt = tradingtimes[1]
    for dtnext in tradingtimes[2:]:

        t0 = time.time()

        ordersprev = ordersnew
        ordersnew = utils.subsetbytime(dfday, dt, dtnext)
        bk, bkft, ordft = updateiter(ordersprev, ordersnew, bk)
        bkordfeatures[dt] = {'book': bkft, 'orders': ordft}

        # update times
        dtprev = dt
        dt = dtnext

        if verbose > 0:
            sreport = 'dt: %s done in: %.2f' % (dt, time.time()-t0)
            if verbose > 1:
                sreport += '\n\tbook features:' + str(bkft)
                sreport += '\n\torder features:' + str(ordft)
            print (sreport)

    if verbose > 1:
        print ('all order/book features done in: %.2f' % (time.time()-t0all))
    return tradingtimes, bkordfeatures


if __name__ == '__main__':

    params = {
        'symbol': 'TD',
        'date_string': '2019-02-04',
        'freq': '1H',
        'tstart_string': '10:00',
        'tend_string': '12:00',
        'verbose': 1}

    x = features(**params)