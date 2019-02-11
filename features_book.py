import pandas as pd
import numpy as np
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


def orderbook(ordersdf, timestamp):
    '''return table of orderbook'''
    # the bloody time up there is in UTC!
    tstr = str(timestamp.tz_convert('UTC'))
    bk = ordersdf.filter('''time < timestamp '%s' ''' % (tstr))
    bk = bk.groupby(['side', 'price']).agg({'book_change': 'sum'})
    bk = bk.withColumnRenamed('sum(book_change)', 'quantity')
    bk = bk.filter('quantity > 0')
    return bk.orderBy('price')


def orderinterval(ordersdf, timestamp0, timestamp1):
    '''return orders between timestamp0 and timestamp1'''
    if timestamp0 < timestamp1:
        t0, t1 = str(timestamp0), str(timestamp1)
    else:
        raise ValueError('not (timestamp0 < timestamp1)!')
    filstr = '''time BETWEEN '%s' AND '%s' '''
    return ordersdf.filter(filstr % (t0, t1))


if __name__ == '__main__':

    symbol = 'TD'
    venue = 'TSX'
    date_string = '2019-02-04'
    
    freq = '1H'
    #freq = '30min'
    #freq = '5min'
    #freq = '1min'
    #freq = '500ms'
    #freq = '1ms'
    tradingtimes = pd.date_range(
        start = pd.to_datetime(date_string + ' 09:30'),
        end = pd.to_datetime(date_string + ' 16:30'),
        freq = freq)

    dfday = dailyorders(symbol, date_string, venue)
    dfday.cache()

    bkfeatures = {}
    for dt in tradingtimes:
        print ('doing dt:', dt)
        bkfeatures[dt] = Book(orderbook(dfday, dt).toPandas()).features()

    ordfeatures = {}
    for dt in tradingtimes:
        print ('doing dt:', dt)
