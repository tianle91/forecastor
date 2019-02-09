#import pyspark as spark
import numpy as np
import pandas as pd
from Book import Book
from Orders import Orders


def dailyorders(symbol, date_string, venue, timestamptrunc):

    s = '''SELECT
            book_change, 
            side, 
            price,
            reason,
            CAST(LEFT(CAST(time AS STRING), %s) AS timestamp) AS time_discrete
        FROM orderbook_tsx 
        WHERE symbol='%s' 
            AND date_string='%s' 
            AND venue = '%s'
            AND price > 0
            AND price < 99999
        ORDER BY time_discrete ASC'''

    sargs = (
        timestamptrunc,
        symbol,
        date_string,
        venue)
        
    return spark.sql(s % sargs)


def ordersbyminutes(symbol, date_string, venue, verbose=0):
    '''return dict of {order_times_inminutes: orders_by_times}'''

    # set time discretization to 1min
    # timestamp = '2019-01-23 09:30:00.000'
    timestamptrunc = 16

    ordersdf = dailyorders(symbol, date_string, venue, timestamptrunc)
    ordersdf.cache()

    if verbose > 0:
        print ('ordersdf:')
        ordersdf.show(2)

    # all discretized times with orders
    orderstimes = ordersdf.select('time_discrete').distinct().orderBy('time_discrete')
    orderstimes = orderstimes.toPandas()['time_discrete']

    if verbose > 0:
        print ('orderstimes:')
        print (orderstimes.head(2))

    out = {}
    for dt in orderstimes:
        dtstr = str(dt)

        if verbose > 1:
            print ('dt:', dt, 'dtstr:', dtstr)
        out[dt] = ordersdf.filter('''time_discrete == '%s' ''' % (dtstr))

    return out


if __name__ == '__main__':

    # set symbol and date_string
    symbol = 'TD'
    date_string = '2019-01-22'
    venue = 'TSX'

    ordersdict = ordersbyminutes(symbol, date_string, venue)
    print ('len(ordersdict):', len(ordersdict))

    # get all the book changes
    resl = map(lambda kv: (kv[0], Orders(kv[1]).bkchange()), list(ordersdict.items()))
    resdict = {k: v for k, v in resl}
    orderstimes = list(resdict.keys())
    # 5mins

    # update recursively to get all orderbooks
    exec(open('Book.py').read())
    bktemp = Book(symbol, str(orderstimes[0]))
    bklist = [bktemp]
    for dt in orderstimes:
        bktemp = bktemp.updatebook(resdict[dt])
        bklist.append(bktemp)
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