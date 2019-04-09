import time
import pandas as pd
import numpy as np
import sparkdfutils as utils
import pyspark.sql.functions as F


def dailytrades(symbol, date_string, tsunit):
    '''return table of trades'''
    s = '''SELECT
            time,
            trade_size AS quantity,
            price,
            buy_broker,
            sell_broker,
            trade_condition,
            time,
            date_trunc('%s', time) as timed,
            ROW_NUMBER() OVER (ORDER BY time) row
        FROM trades 
        WHERE symbol = '%s' 
            AND date_string = '%s' 
            AND price > 0
        ORDER BY time ASC'''
    sargs = (tsunit, symbol, date_string)
    spark.sql(s % sargs).createOrReplaceTempView('tradetemp')
    
    s = '''SELECT
            a.*, 
            a.price - b.price AS price_diff,
            LOG(a.price/b.price) AS logreturn
        FROM tradetemp a
        LEFT JOIN tradetemp b
            ON a.row = b.row-1'''
    df = spark.sql(s)
    df = df.withColumn('price_diff2', F.pow(df.price_diff, 2))
    return df



def features(symbol, date_string, venue = 'TSX',
    tsunit = 'MINUTE', tstart_string = '09:30', tend_string = '16:00',
    verbose = 0):
    '''return dict of features of trades
    features at HH:MM are computed trades that came in during [HH:MM, HH:(MM+1))

    Args:
        symbol: str of ticker
        date_string: str of YYYY-MM-DD
        freq: pandas freq (e.g. ['1H', '30min', '5min', '1min'])
        tstart_string: str of 'HH:MM' for start time
        tend_string: str of 'HH:MM' for end time
    '''
    # set up key for output dictionary, trading times is in US/Eastern
    freq = '1H' if tsunit == 'HOUR' else '1min' if tsunit == 'MINUTE' else '1S' if tsunit == 'SECOND' else None
    tradingtimesdf = utils.tradingtimes(date_string, tstart_string, tend_string, freq, tz='US/Eastern')
    tradingtimesdf.sort()
    tdelta = tradingtimesdf[-1]-tradingtimesdf[-2]

    if verbose > 1:
        print ('trades: len(tradingtimesdf):', len(tradingtimesdf))
        if verbose > 2:
            print ('trades: trading times in dfday in US/Eastern:')
            print ([str(dt) for dt in tradingtimesdf])

    
    # --------------------------------------------------------------------------
    # get all trades in day
    # --------------------------------------------------------------------------
    if verbose > 0:
        t0 = time.time()

    # get all transactions prior to tradingtimes[-1]
    dfday = dailytrades(symbol, date_string, tsunit)

    dfday = utils.subsetbytime(dfday, tradingtimesdf[-1]+tdelta)

    if verbose > 0:
        t1 = time.time()
        print ('trades: get df %s done in: %.2f ntrades: %d' %\
            (date_string, time.time()-t0, dfday.count()))


    # --------------------------------------------------------------------------
    # trades features
    # --------------------------------------------------------------------------
    if verbose > 0:
        print ('trades: doing features')

    txfeaturesparams = [
        ('count', '*'),
        ('mean', 'quantity'),
        ('mean', 'price'),
        ('mean', 'price_diff2'),
        ('mean', 'logreturn'),
        ('stddev', 'quantity'),
        ('stddev', 'price'),
        ('stddev', 'price_diff2'),
        ('stddev', 'logreturn'),
        ('min', 'quantity'),
        ('min', 'price'),
        ('min', 'price_diff2'),
        ('min', 'logreturn'),
        ('max', 'quantity'),
        ('max', 'price'),
        ('max', 'price_diff2'),
        ('max', 'logreturn'),
        ('sum', 'price_diff2')
    ]

    if verbose > 2:
        print ('trades: txfeaturesparams:')
        for aggfn, colname in txfeaturesparams:
            print ('\taggfn: %s\tcolname: %s' % (aggfn, colname))

    dfday.cache()
    tradesfeaturesbycovname = {}
    for aggfn, colname in txfeaturesparams:
        tradesfeaturesbycovname['%s(%s)' % (aggfn, colname)] = dfday.groupBy('timed').agg({colname: aggfn}).toPandas()

    # additional covariates
    tradesfeaturesbycovname['vol-weighted-price']       = dfday.groupBy('timed').agg((F.sum(dfday.quantity*dfday.price      )/F.sum(dfday.quantity))).toPandas()
    tradesfeaturesbycovname['vol-weighted-price_diff2'] = dfday.groupBy('timed').agg((F.sum(dfday.quantity*dfday.price_diff2)/F.sum(dfday.quantity))).toPandas()
    tradesfeaturesbycovname['vol-weighted-logreturn']   = dfday.groupBy('timed').agg((F.sum(dfday.quantity*dfday.logreturn  )/F.sum(dfday.quantity))).toPandas()
    dfday.unpersist()

    if verbose > 0:
        print ('trades: tradesfeaturesbycovname done in: %.2f' % (time.time()-t1))

    # --------------------------------------------------------------------------
    # change to dt key
    # --------------------------------------------------------------------------
    dummydict = {}
    for covname in tradesfeaturesbycovname:
        dummydict[covname] = None
    tradesfeatures = {dt: dummydict.copy() for dt in tradingtimesdf}    

    for covname in tradesfeaturesbycovname:
        dftemp = tradesfeaturesbycovname[covname]
        for index, row in dftemp.iterrows():
            dt, value = row[0], row[1]
            dt = utils.utctimestamp_to_tz(dt, 'US/Eastern')
            if dt in tradesfeatures:
                try:
                    tradesfeatures[dt][covname] = float(value)
                except:
                    print ('trades: not converted! covname: %s, dt: %s, value: %s' % (covname, dt, value))

    
    if verbose > 0:
        print ('trades: number of covariates:', len(tradesfeatures[tradingtimesdf[0]]))
        print ('trades: all done in: %.2f' % (time.time()-t0))

    # --------------------------------------------------------------------------
    # collect and return
    # --------------------------------------------------------------------------   
    out = {}
    for dt in tradingtimesdf:
    	out[dt] = {'trades_' + k: v for k, v in tradesfeatures[dt].items()}
    	
    return out


if __name__ == '__main__':

    params = {
        'symbol': 'TD',
        'date_string': '2019-02-04',
        'tsunit': 'MINUTE',
        'tstart_string': '10:00',
        'tend_string': '12:00',
        'verbose': 2}

    x = features(**params)