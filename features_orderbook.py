import pandas as pd
import numpy as np
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
            AND venue = '%s'
            AND price > 0
            AND price < 99999
        ORDER BY time ASC'''
    sargs = (symbol, date_string, venue)
    return spark.sql(s%sargs) 


def orderbook(ordersdf, timestamp):
    '''return table of orderbook'''
    tstr = str(timestamp)
    df = ordersdf.filter('''time <= '%s' ''' % (tstr))
    bk = df.groupby(['side', 'price']).agg({'book_change': 'sum'})
    bk = bk.withColumnRenamed('sum(book_change)', 'quantity')
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
    freq = '1min'
    tradingtimes = pd.date_range(
        start = pd.to_datetime(date_string + ' 09:30'),
        end = pd.to_datetime(date_string + ' 16:30'),
        freq = freq)

    dfday = dailyorders(symbol, date_string, venue)
    dfday.show(5) # 1min

    features = {}
    features['orderbook'] = [Book(orderbook(dfday, dt).toPandas()).features() for dt in tradingtimes]

