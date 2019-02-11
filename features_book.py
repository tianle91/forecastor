import pandas as pd
import numpy as np
import orders_functions as ordfn
from Book import Book


def utctimestamp(dt):
    s = dt.tz_convert('UTC')
    s = s.strftime('%Y-%m-%d %H:%M:%S')
    return s
    

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


def orderinterval(ordersdf, timestamp0, timestamp1=None, verbose=0):
    '''return orders between timestamp0 and timestamp1'''
    if timestamp1 is None:
        s = '''time < '%s' ''' % (utctimestamp(timestamp0))
    else:
        if not (timestamp0 < timestamp1):
            raise ValueError('not (timestamp0 < timestamp1)!')
        t0, t1 = utctimestamp(timestamp0), utctimestamp(timestamp1)
        s = '''time BETWEEN '%s' AND '%s' ''' % (t0, t1)
    
    df = ordersdf.filter(s)
    if verbose > 0:
        print ('len:', df.count())
    return df


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
        dftemp = orderinterval(dfday, dtprev, dt)
        touchtemp = bkfeatures[dtprev]['bestbid'], bkfeatures[dtprev]['bestask']
        ordfeatures[dt] = ordfn.features(dftemp, touchtemp)