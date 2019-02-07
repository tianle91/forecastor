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
        
    return spark.sql(s % sargs).cache()


def ordersbyminutes(symbol, date_string, venue):
    '''return dict of {order_times_inminutes: orders_by_times}'''

    # set time discretization to 1min
    # timestamp = '2019-01-23 09:30:00.000'
    timestamptrunc = 16
    tfmt = '%Y-%M-%D %H:%m:%s'

    ordersdf = dailyorders(symbol, date_string, venue, timestamptrunc)
    ordersdf.show()

    # all discretized times with orders
    orderstimes = ordersdf.select('time_discrete').distinct().orderBy('time_discrete')
    orderstimes = orderstimes.toPandas()['time_discrete']
    print (orderstimes.head())

    out = {}
    for ordertime in ordertimes:
        ordtstr = ordertime.strftime(tfmt)
        out[ordertime] = ordersdf.filter(ordersdf.time_discrete == ordtstr)

    return out


if __name__ == '__main__':

    # set symbol and date_string
    symbol = 'TD'
    date_string = '2019-01-22'
    venue = 'TSX'

    ordersdict = ordersbyminutes(symbol, date_string, venue)

    # get all the book changes
    resl_bkch = map(lambda x: Orders(x).bkchange(), list(ordersdict))


    # update recursively to get all orderbooks
    bktemp = Book(symbol, orderstimes[0].strftime(tfmt))
    bklist = [bkini]
    for bkch in resl_bkch:
        bktemp = bktemp.updatebook(bkch)
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