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
            date_trunc('%s', time) as timed
        FROM trades 
        WHERE symbol = '%s' 
            AND date_string = '%s' 
            AND price > 0
        ORDER BY time ASC'''
    sargs = (tsunit, symbol, date_string)
    return spark.sql(s % sargs)


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
    tradingtimes = utils.tradingtimes(date_string, tstart_string, tend_string, freq, tz='US/Eastern')
    tradingtimes.sort()

    
    # --------------------------------------------------------------------------
    # get all orders in day
    # --------------------------------------------------------------------------
    if verbose > 0:
        t0 = time.time()

    # get all transactions prior to tradingtimes[-1]
    dfday = dailytrades(symbol, date_string, tsunit)
    dfday = utils.subsetbytime(dfday, tradingtimes[-1])
    dfday.cache()

    if verbose > 0:
        print ('cached trades for %s done in: %.2f ntrades: %d' %\
            (date_string, time.time()-t0, dfday.count()))

    # utc stuff is from dfday where there are observations
    tradingtimesdf = dfday.select('timed').distinct().toPandas()
    tradingtimesdf = [val for index, val in tradingtimesdf['timed'].iteritems()]
    tradingtimesdf = [utils.utctimestamp_to_tz(dt, 'US/Eastern') for dt in tradingtimesdf]
    tradingtimesdf = [dt for dt in tradingtimesdf if dt >= tradingtimes[0]]
    tradingtimesdf.sort()

    if verbose > 1:
        print ('len(tradingtimesdf):', len(tradingtimesdf))
        if verbose > 2:
            print ('trading times in dfday in US/Eastern:')
            print ([str(dt) for dt in tradingtimesdf])


    # --------------------------------------------------------------------------
    # trades features
    # --------------------------------------------------------------------------
    if verbose > 0:
        t1 = time.time()
        print ('doing trades features')

    def covnamer(colname, aggfn):
        k = '%s(%s)_trades' %\
            (aggfn, colname)
        return k

    def worker(colname, aggfn, verbose):
        k = covnamer(colname, aggfn)
        if verbose > 0:
            t2 = time.time()

        dftemp = dfday.groupBy('timed').agg({colname: aggfn})
        dftemp = dftemp.toPandas()

        if verbose > 0:
            print ('done in: %.2f' % (time.time()-t2))
            if verbose > 1:
                print (dftemp.head(5))
        return k, dftemp

    params = [
        {'colname': colname, 'aggfn': aggfn, 'verbose': verbose-1}
        for colname, aggfn in [
           ('*', 'count'), 
           ('quantity', 'mean'), 
           ('quantity', 'stddev'),
           ('price', 'mean'), 
           ('price', 'stddev'), 
           ('price', 'min'), 
           ('price', 'max')]
    ]

    resl = map(lambda x: worker(**x), params)
    tradesfeaturesbycovname = {k: v for k, v in resl}

    price_wgtbyqty = dfday.groupBy('timed').agg((F.sum(dfday.price*dfday.quantity)/F.sum(dfday.quantity)))
    price_wgtbyqty = price_wgtbyqty.toPandas()
    tradesfeaturesbycovname['price_wgtbyqty'] = price_wgtbyqty

    if verbose > 0:
        print ('trades features done in: %.2f' % (time.time()-t1))
    if verbose > 1:
        print ('number of covariates for new trades:', len(list(tradesfeaturesbycovname.keys())))


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
                    print ('value: %s not converted!' % (value))

    
    if verbose > 0:
        print ('all done in: %.2f' % (time.time()-t0))

    dfday.unpersist()
    return tradesfeatures


if __name__ == '__main__':

    params = {
        'symbol': 'TD',
        'date_string': '2019-02-04',
        'tsunit': 'MINUTE',
        'tstart_string': '10:00',
        'tend_string': '12:00',
        'verbose': 2}

    x = features(**params)