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

    if verbose > 1:
        print ('len(tradingtimesdf):', len(tradingtimesdf))
        if verbose > 2:
            print ('trading times in dfday in US/Eastern:')
            print ([str(dt) for dt in tradingtimesdf])

    
    # --------------------------------------------------------------------------
    # get all trades in day
    # --------------------------------------------------------------------------
    if verbose > 0:
        t0 = time.time()

    # get all transactions prior to tradingtimes[-1]
    dfday = dailytrades(symbol, date_string, tsunit)
    dfday = utils.subsetbytime(dfday, tradingtimesdf[-1])

    if verbose > 0:
        t1 = time.time()
        print ('get trades for %s done in: %.2f ntrades: %d' %\
            (date_string, time.time()-t0, dfday.count()))


    # --------------------------------------------------------------------------
    # trades features
    # --------------------------------------------------------------------------
    if verbose > 0:
        print ('doing features for trades')

    def worker(colname, aggfn, verbose):
        if verbose > 0:
            t2 = time.time()

        

        if verbose > 0:
            print ('done in: %.2f' % (time.time()-t2))
            if verbose > 1:
                print (dftemp.head(5))
        return 'trades_%s(%s)' % (aggfn, colname), dftemp

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

    dfday.cache()
    tradesfeaturesbycovname = {}
    for aggfn, colname in txfeaturesparams:
        tradesfeaturesbycovname['%s(%s)' % (aggfn, colname)] = dfday.groupBy('timed').agg({colname: aggfn}).toPandas()

    # additional covariates
    ## vwap
    vwap = dfday.groupBy('timed').agg((F.sum(dfday.price*dfday.quantity)/F.sum(dfday.quantity)))
    tradesfeaturesbycovname['vol-weighted-price'] = vwap.toPandas()
    ## vol-weighted quad variation
    vwquadvar = dfday.groupBy('timed').agg((F.sum(dfday.price_diff2*dfday.quantity)/F.sum(dfday.quantity)))
    tradesfeaturesbycovname['vol-weighted-price_diff2'] = vwquadvar.toPandas()
    ## vol-weighted log return
    vwlogreturn = dfday.groupBy('timed').agg((F.sum(dfday.logreturn*dfday.quantity)/F.sum(dfday.quantity)))
    tradesfeaturesbycovname['vol-weighted-logreturn'] = vwlogreturn.toPandas()
    dfday.unpersist()

    if verbose > 0:
        print ('\ttrades features done in: %.2f' % (time.time()-t1))


    # change to dt key    
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
                    #print ('covname: %s value: %s not converted!' % (covname, value))
                    #print (row)
                    pass

    
    if verbose > 0:
        print ('number of covariates in trades:', len(tradesfeatures[tradingtimesdf[0]]))
        print ('all done in: %.2f' % (time.time()-t0))

    return {'trades_'+k: v for k, v in tradesfeatures.items()}


if __name__ == '__main__':

    params = {
        'symbol': 'TD',
        'date_string': '2019-02-04',
        'tsunit': 'MINUTE',
        'tstart_string': '10:00',
        'tend_string': '12:00',
        'verbose': 2}

    x = features(**params)