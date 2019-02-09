#import pyspark as spark
import numpy as np
import pandas as pd
from Book import Book
from Orders import Orders


def dailyorders(symbol, date_string, venue, timestamptrunc):

    # s = '''SELECT
    #         SUM(book_change) AS quantity, 
    #         side, 
    #         price
    #     FROM orderbook_tsx 
    #     WHERE symbol='%s' 
    #         AND date_string='%s' 
    #         AND time < timestamp '%s'
    #         AND venue = '%s'
    #         AND price > 0
    #         AND price < 99999
    #     GROUP BY side, price 
    #     ORDER BY price ASC'''

    s = '''SELECT
            book_change, 
            side, 
            price,
            reason,
            time,
            TRUNC(time, 'mm') AS time_discrete
        FROM orderbook_tsx 
        WHERE symbol='%s' 
            AND date_string='%s' 
            AND venue = '%s'
            AND price > 0
            AND price < 99999
        ORDER BY time_discrete ASC'''
    #CAST(LEFT(CAST(time AS STRING), %s) AS timestamp) AS time_discrete

    sargs = (
        timestamptrunc,
        symbol,
        date_string,
        venue)
        
    return spark.sql(s % sargs)


def ordersbyminutes(symbol, date_string, venue, verbose=0):
    '''return (orderstimes, ordersdf_date_string)'''

    # set time discretization to 1min
    # timestamp = '2019-01-23 09:30:00.000'
    timestamptrunc = 16

    ordersdf = dailyorders(symbol, date_string, venue, timestamptrunc)

    if verbose > 0:
        print ('ordersdf:')
        ordersdf.show(2)

    # all discretized times with orders
    orderstimes = ordersdf.select('time_discrete').distinct().orderBy('time_discrete')
    orderstimes = orderstimes.toPandas()['time_discrete']

    if verbose > 0:
        print ('orderstimes:')
        print (orderstimes.head(2))

    return orderstimes, ordersdf

    # out = {}
    # for dt in orderstimes:
    #     dtstr = str(dt)

    #     if verbose > 1:
    #         print ('dt:', dt, 'dtstr:', dtstr)
    #     out[dt] = ordersdf.filter('''time_discrete == '%s' ''' % (dtstr))

    # return out


if __name__ == '__main__':

    # set symbol and date_string
    symbol = 'TD'
    date_string = '2019-01-22'
    venue = 'TSX'


    orderstimes, ordersdf = ordersbyminutes(symbol, date_string, venue)
    print ('len(orderstimes):', len(orderstimes))


    exec(open('Book.py').read())
    exec(open('Orders.py').read())

    # update recursively to get all orderbooks
    bk0df = orderbook(symbol, str(orderstimes[0]), venue=venue)
    bk0 = Book(bk0df)
    bkresdict = {orderstimes[0]: bk0}

    bktemp = bk0
    for dt in orderstimes:
        print ('dt:', dt)
        dftemp = ordersdf.filter('''time_discrete == '%s' ''' % dt)
        bktemp = bktemp.updatebook(bkchange(dftemp), verbose=1)
        bkresdict[dt] = bktemp


    # !!! IPR
    # package up ordersdf

    resl_ordordbk = zip((resl_ordersdf, bklist[:-1]))


    # get all features
    # order features need orderbook touch as input
    def worker(x):
        orderstemp, bktemp = x
        bkfeaturestemp = bktemp.features()
        touchval = (bkfeaturestemp['bestbid'], bkfeaturestemp['bestask'])
        return {'orderbook': bkfeaturestemp,
                'orders': Orders(orderstemp).features(touchval)}
    resl_features = map(worker, resl_ordordbk)