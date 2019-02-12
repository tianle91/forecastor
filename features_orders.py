import time
import pandas as pd
import numpy as np

import sparkdfutils as utils
import orders_functions as ordfn
from Orderbook import Book


def dailyorders(symbol, date_string, venue, tlim='23:59'):
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
            AND time < timestamp '%s %s:00'
        ORDER BY time ASC'''
    sargs = (symbol, date_string, date_string, tlim)
    return spark.sql(s%sargs) 


def orderbook(ordersdf, timestamp, verbose=0):
    '''return table of orderbook'''
    tstr = utils.utctimestamp(timestamp)
    bk = ordersdf.filter('''time < timestamp '%s' ''' % (tstr))
    bk = bk.groupby(['side', 'price']).agg({'book_change': 'sum'})
    bk = bk.withColumnRenamed('sum(book_change)', 'quantity')
    bk = bk.filter('quantity > 0')
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
    t0all = time.time()
    t0 = time.time()
    dfday = dailyorders(symbol, date_string, venue, tlim = tend_string)
    dfday.cache()
    if verbose > 0:
        print ('get dailyorders done in: %.2f norders: %d' %\
            (time.time()-t0, dfday.count()))

    tradingtimes = pd.date_range(
        start = pd.to_datetime(date_string + ' 09:30:01'),
        end = pd.to_datetime(date_string + ' 16:00:01'),
        tz = 'US/Eastern',
        freq = freq)

    if verbose > 0:
        print ('freq:%s len(tradingtimes): %d' % (freq, len(tradingtimes)))


    # orderbook features
    if verbose > 0:
        print ('running orderbook features...')
    
    t1 = time.time()
    bkfeatures = {}
    dtprev = tradingtimes[0]
    bkftprev = Book(orderbook(dfday, dtprev).toPandas()).features()

    for dt in tradingtimes:
        t0 = time.time()
        bkft = bkftprev

        if dt > dtprev:
            # only when dt has advanced past tradingtimes[0]
            if utils.subsetbytime(dfday, dtprev, dt).count() > 0:
                # only when new orders arrived
                bkft = Book(orderbook(dfday, dt).toPandas()).features()
        
        bkfeatures[dt] = bkft
        dtprev = dt

        if verbose > 0:
            sreport = 'dt: %s done in: %.2f' % (dt, time.time()-t0)
            if verbose > 1:
                sreport += '\n\tfeatures:' + str(bkft)
            print (sreport)

    if verbose > 0:
        print ('orderbook features done in: %.2f' % (time.time()-t1))
        # 3secs each if new orders, around 5mins total


    # new orders features
    if verbose > 0:
        print ('running new orders features...')

    t1 = time.time()
    ordfeatures = {}
    ordfeatures[tradingtimes[-1]] = None
    dt = tradingtimes[0]

    for dtnext in tradingtimes[1:]:
        # we run on new orders between [dt, dtnext)
        t0 = time.time()
        dftemp = utils.subsetbytime(dfday, dt, dtnext)
        
        ordft = None
        if dftemp.count() > 0:
            # only when new orders arrived
            touchtemp = bkfeatures[dt]['bestbid'], bkfeatures[dt]['bestask']
            ordft = ordfn.features(dftemp.toPandas(), touchtemp)
            
        ordfeatures[dt] = ordft
        dt = dtnext
        
        if verbose > 0:
            sreport = 'dt: %s norders: %d done in: %.2f' % (dt, norders, time.time()-t0)
            if verbose > 1:
                sreport += '\n\tfeatures:\n\t' + str(ordft)
            print (sreport)

    if verbose > 0:
        print ('new orders features done in: %.2f' % (time.time()-t1))


    # aggregate into dict with time as key
    out = {}
    for dt in tradingtimes:
        out[dt] = {'book': bkfeatures[dt], 'orders': ordfeatures[dt]}

    if verbose > 1:
        print ('all order/book features done in: %.2f' % (time.time()-t0a))
    return out


if __name__ == '__main__':

    symbol = 'TD'
    date_string = '2019-02-04'
    x = features(symbol, date_string, verbose=2)