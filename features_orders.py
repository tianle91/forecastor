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
        ORDER BY time ASC'''
    sargs = (symbol, date_string)
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

    dfday = dailyorders(symbol, date_string, venue)
    dfday.cache()

    tradingtimes = pd.date_range(
        start = pd.to_datetime(date_string + ' 09:30:01'),
        end = pd.to_datetime(date_string + ' 16:00:01'),
        tz = 'US/Eastern',
        freq = freq)

    if verbose > 0:
        print ('len(tradingtimes):', len(tradingtimes))


    # orderbook features
    if verbose > 0:
        print ('running orderbook features...')
    
    bkfeatures = {}
    dtprev = tradingtimes[0]
    bkftprev = Book(orderbook(dfday, dtprev).toPandas()).features()

    for dt in tradingtimes:
        t0 = time.time()
        bkft = bkftprev

        if dt > dtprev:
            # only when dt has advanced past tradingtimes[0]
            norders =  utils.subsetbytime(dfday, dtprev, dt).count()
            if norders > 0:
                # only when new orders arrived
                bkft = Book(orderbook(dfday, dt).toPandas()).features()
        
        bkfeatures[dt] = bkft
        dtprev = dt

        if verbose > 0:
            s = 'dt: %s done in: %s' % (dt, time.time()-t0)
            if verbose > 1:
                s += '\nfeatures:\n' + str(bkft)
            print (sreport)


    # new orders features
    if verbose > 0:
        print ('running new orders features...')

    ordfeatures = {}
    ordfeatures[tradingtimes[-1]] = None
    dt = tradingtimes[0]

    for dtnext in tradingtimes[1:]:
        # we run on new orders between [dt, dtnext)
        t0 = time.time()
        dftemp = utils.subsetbytime(dfday, dt, dtnext)
        norders = dftemp.count()
        
        ordft = None
        if norders > 0:
            # only when new orders arrived
            touchtemp = bkfeatures[dt]['bestbid'], bkfeatures[dt]['bestask']
            ordft = ordfn.features(dftemp, touchtemp)
            
        ordfeatures[dt] = ordft
        dt = dtnext
        
        if verbose > 0:
            sreport = 'dt: %s norders: %s done in: %s' % (dt, norders, time.time()-t0)
            if verbose > 1:
                sreport += '\nfeatures:\n' + str(ordft)
            print (sreport)            

    # aggregate into dict with time as key
    out = {}
    for dt in tradingtimes:
        featuresdt = {'book': bkfeatures[dt], 'orders': ordfeatures[dt]}
        out[dt] = featuresdt
    return out


if __name__ == '__main__':

    symbol = 'TD'
    date_string = '2019-02-04'
    x = features(symbol, date_string, verbose=2)