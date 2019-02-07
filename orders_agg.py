%pyspark

import numpy as np
import pandas as pd
#import pyspark as spark
#from Book import Book
#from Orders import Orders


# set symbol and date_string
symbol = 'TD'
date_string = '2019-01-22'

# set time discretization to 1min
# timestamp = '2019-01-23 09:30:00.000'
timestamptrunc = 16
tfmt = '%Y-%M-%D %H:%m:%s'
tradingdt = pd.date_range(date_string+' 09:30', date_string+' 16:00', freq='1min')


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


ordersday = dailyorders(symbol, date_string, venue, timestamptrunc)
ordersday.show(5)


# all discretized times with orders
orderstimes = ordersday.select('time_discrete').distinct().orderBy('time_discrete')
orderstimes = orderstimes.toPandas()['time_discrete']
orderstimes


# testing for single time period
#ordersday_t = ordersday.filter(ordersday.time_discrete == '2019-01-22 12:00:00')
ordersday_t = ordersday.filter(ordersday.time_discrete == orderstimes[0].strftime(tfmt))
ordersday_t.show(5)


# all orders filtered by time_discrete
resl_ordersdf = [ordersday.filter(ordersday.time_discrete == tdt.strftime(tfmt)) for tdt in orderstimes]


# get all the book changes
resl_bkch = map(lambda x: Orders(x).bkchange(), resl_ordersdf)


# update to get all orderbooks
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