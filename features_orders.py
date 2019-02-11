import pandas as pd
import numpy as np

import sparkdfutils as utils
import orders_functions as ordfn
from Book import Book


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


if __name__ == '__main__':

    symbol = 'TD'
    venue = 'TSX'
    date_string = '2019-02-04'

    dfday = dailyorders(symbol, date_string, venue)
    dfday.cache()

    
    #freq = '1H'
    freq = '30min'
    #freq = '5min'
    #freq = '1min'
    tradingtimes = pd.date_range(
        start = pd.to_datetime(date_string + ' 09:30:01'),
        end = pd.to_datetime(date_string + ' 16:00:01'),
        tz = 'US/Eastern',
        freq = freq)
    print ('len(tradingtimes):', len(tradingtimes))


    # orderbook features
    bkfeatures = {}
    # keep track of previous orderbook features
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
        print ('dt: %s done in: %s \n\tfeatures: %s' % (dt, time.time()-t0, bkft))
        #print ('dt: %s done in: %s' % (dt, time.time()-t0))
    # 3sec*14 = 1min


    # orders features
    ordfeatures = {}
    # no new orders for first timestamp
    dtprev = tradingtimes[0]
    ordfeatures[dtprev] = None

    for dt in tradingtimes[1:]:
        t0 = time.time()
        dftemp = utils.subsetbytime(dfday, dtprev, dt)
        norders = dftemp.count()
        
        ordft = None
        if norders > 0:
            # only when new orders arrived
            touchtemp = bkfeatures[dtprev]['bestbid'], bkfeatures[dtprev]['bestask']
            ordft = ordfn.features(dftemp, touchtemp)
            
        ordfeatures[dt] = ordft
        dtprev = dt
        print ('dt: %s norders: %s done in: %s \n\tfeatures: %s' % (dt, norders, time.time()-t0, ordft))
        #print ('dt: %s norders: %s done in: %s' % (dt, norders, time.time()-t0))
    # 30sec*14 = 10min