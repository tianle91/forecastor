#import pyspark as spark
from  pyspark.sql.functions import abs
import numpy as np
import pandas as pd


def dailytrades(symbol, date_string, venue, timestamptrunc):
    '''return table of trades'''
        
    s = '''SELECT
            trade_size AS quantity, 
            price,
            CAST(LEFT(CAST(time AS STRING), %s) AS timestamp) AS time_discrete
        FROM trades 
        WHERE symbol = '%s' 
            AND date_string = '%s' 
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


def features(tradesdf, verbose=0):
    ''' return dict of features of tradesdf

    Args:
        tradesdf: pyspark.sql.dataframe
        verbose: verbosity
    '''
    df = tradesdf.groupBy('price').agg({'quantity': 'sum'}).toPandas()
    prx = df['price'].astype(float)
    qty = df['sum(quantity)']
    wgt = qty / np.sum(qty)

    if verbose > 0:
        print ('prx:', prx)
        print ('wgt:', wgt)

    tradecount = len(prx)

    mean = 0
    stdev = 0
    tradeq = 0
    qtypertrade = 0

    if tradecount > 0:
        mean =  np.average(prx, weights=wgt)
        stdev = np.average(np.power(prx, 2), weights=wgt)
        stdev = np.sqrt(stdev - np.power(mean, 2))
        tradeq = np.sum(qty)
        qtypertrade = tradeq/tradecount

    out = {'mean': mean, 
           'stdev': stdev, 
           'traded_count': tradecount, 
           'traded_qty': tradeq, 
           'meanq_pertrade': qtypertrade}

    return out


def tradesbyminutes(symbol, date_string, venue, verbose=0):
    '''return dict of {trade_times_inminutes: trades_by_times}'''

    # set time discretization to 1min
    # timestamp = '2019-01-23 09:30:00.000'
    timestamptrunc = 16

    # get all trades in day
    tradesdf = dailytrades(symbol, date_string, venue, timestamptrunc)
    tradesdf.cache()
    if verbose > 0:
        print ('\ntradesdf:')
        tradesdf.show(2)

    # all discretized times with trades
    tradestimes = tradesdf.select('time_discrete').distinct().orderBy('time_discrete')
    tradestimes = tradestimes.toPandas()['time_discrete']
    if verbose > 0:
        print ('\ntradetimes:')
        print (tradestimes.head(2))

    out = {}
    for dt in tradestimes:
        dtstr = str(dt)
        if verbose > 1:
            print ('dt:', dt, 'dtstr:', dtstr)
        out[dt] = tradesdf.filter('''time_discrete == '%s' ''' % (dtstr))

    return out


if __name__ == '__main__':

    # set symbol and date_string
    symbol = 'TD'
    date_string = '2019-01-22'
    venue = 'TSX'


    tradesdict = tradesbyminutes(symbol, date_string, venue, verbose=1)

    resl = map(lambda kv: (kv[0], features(kv[1])), list(tradesdict.items()))
    featuresdict = {k: v for k, v in resl}