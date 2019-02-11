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
        ORDER BY time ASC'''
    sargs = (symbol, date_string)
    return spark.sql(s%sargs) 


def orderbook(ordersdf, timestamp, verbose=0):
    '''return table of orderbook'''
    tstr = utctimestamp(timestamp)
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
        start = pd.to_datetime(date_string + ' 09:30'),
        end = pd.to_datetime(date_string + ' 16:00'),
        tz = 'US/Eastern',
        freq = freq)


    bkfeatures = {}
    for dt in tradingtimes:
        print ('doing dt:', dt)
        bkfeatures[dt] = Book(orderbook(dfday, dt).toPandas()).features()


    ordfeatures = {}
    dtprev = tradingtimes[0]
    ordfeatures[dtprev] = None
    for dt in tradingtimes[1:]:
        print ('doing dtprev:%s dt:%s' % (dtprev, dt))
        dftemp = utils.subsetbytime(dfday, dtprev, dt)
        touchtemp = bkfeatures[dtprev]['bestbid'], bkfeatures[dtprev]['bestask']
        ordfeatures[dt] = ordfn.features(dftemp, touchtemp)