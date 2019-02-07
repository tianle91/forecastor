import pyspark as spark
from  pyspark.sql.functions import abs
import numpy as np
import pandas as pd


def dailytrades(symbol, date_string, venue, timestamptrunc):
    '''return table of trades'''
    
    if not (timestamp0 <= timestamp1):
        raise ValueError('not (timestamp0:%s <= timestamp1:%s)!' % (timestamp0, timestamp1))
    
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
        timestamptrunc
        symbol,
        date_string)
        
    return spark.sql(s % sargs).cache()


def features(tradesdf):
    ''' return dict of features of tradesdf

    Args:
        tradesdf: pyspark.sql.dataframe
    '''
    df = tradesdf.groupBy('price').agg({'quantity': 'sum'}).toPandas()
    prx = df['price']
    qty = df['sum(quantity)']
    
    mean = 0
    stdev = 0
    tradeq = 0
    tradecount = 0
    qtypertrade = 0
    
    if len(prx) > 0:
        mean =  np.average(prx, weights=qty)
        stdev = np.sqrt(np.average(np.power(prx, 2), weights=qty) - mean**2)
        tradeq = np.sum(qty)
        tradecount = len(prx)
        qtypertrade = tradeq/tradecount
    
    out = {'mean': mean, 
           'stdev': stdev, 
           'traded_count': tradecount, 
           'traded_qty': tradeq, 
           'meanq_pertrade': qtypertrade}

    return out


if __name__ == '__main__':

    # set symbol and date_string
    symbol = 'TD'
    date_string = '2019-01-22'

    # set time discretization to 1min
    # timestamp = '2019-01-23 09:30:00.000'
    timestamptrunc = 16
    tfmt = '%Y-%M-%D %H:%m:%s'

    # trading timestamps
    dailydt = pd.date_range(date_string+' 09:30', date_string+' 16:00', freq='1min')
    print (dailydt.head())


    tradesdf = dailytrades(symbol, date_string, venue, timestamptrunc)
    tradesdf.show(5)


    # all discretized times with trades
    tradestimes = tradesdf.select('time_discrete').distinct().orderBy('time_discrete')
    tradestimes = tradestimes.toPandas()['time_discrete']
    print (tradestimes.head())


    # testing for single time period
    tradesdf_t = tradesdf.filter(tradesdf.time_discrete == tradestimes[0].strftime(tfmt))
    tradesdf_t.show(5)


    # all orders filtered by time_discrete
    resl_tradesdf = [tradesdf.filter(tradesdf.time_discrete == tdt.strftime(tfmt)) for tdt in tradestimes]


    # get all features
    resl_features = map(features, resl_tradesdf)