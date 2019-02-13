import time
import pandas as pd
import numpy as np

import sparkdfutils as utils
import trades_functions as trxfn


def dailytrades(symbol, date_string):
    '''return table of trades'''
    s = '''SELECT
            time,
            trade_size AS quantity,
            price,
            buy_broker,
            sell_broker,
            trade_condition
        FROM trades 
        WHERE symbol = '%s' 
            AND date_string = '%s' 
            AND price > 0
        ORDER BY time ASC'''
    sargs = (symbol, date_string)
    return spark.sql(s % sargs)


def brokertrfmatrix():
    brokerdf = spark.sql('SELECT * FROM broker_metadata').toPandas()
    brokeridlist = np.unique(brokerdf['broker_id'])
    brokeridlist = brokeridlist[np.logical_not(np.isnan(brokeridlist))]
    brokeridlist = np.arange(start=1, stop=max(brokeridlist)+1)
    brokeridlist = brokeridlist.astype(int).astype(str)
    out = pd.DataFrame(
        [np.zeros(len(brokeridlist))]*len(brokeridlist), 
        index=brokeridlist, columns=brokeridlist)
    return out


def features(symbol, date_string,
    freq = '1min', tstart_string = '09:30', tend_string = '16:00',
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
    if verbose > 0:
        t0all = time.time()
        t0 = time.time()

    tradingtimes = utils.tradingtimes(date_string, 
        tstart_string, tend_string, freq, tz='US/Eastern')
    brotrfm = brokertrfmatrix()

    # get all transactions prior to tradingtimes[-1]
    dfday = dailytrades(symbol, date_string)
    #dfday = utils.subsetbytime(dfday, tradingtimes[-1])
    dfday.cache()
    if verbose > 0:
        print ('get dailyorders done in: %.2f norders: %d before: %s' %\
            (time.time()-t0, dfday.count(), tend_string))

    # count number of new orders
    tstartdt = tradingtimes[0]
    tenddt = tradingtimes[-1]
    ntradesnew = utils.subsetbytime(dfday, tstartdt, tenddt).count()

    if verbose > 0:
        print ('freq:%s tstart: %s tend: %s len(tradingtimes): %d' %\
            (freq, tstart_string, tend_string, len(tradingtimes)))


    # new trades features
    # --------------------------------------------------------------------------
    if verbose > 0:
        print ('running new trades features for all dt in tradingtimes...')
        print ('there are %d new orders from %s to %s' %\
            (ntradesnew, tstartdt, tenddt))

    t1 = time.time()
    tradesfeatures = {}
    tradesfeatures[tradingtimes[-1]] = None
    dt = tradingtimes[0]

    for dtnext in tradingtimes[1:]:
        # we run on new orders between [dt, dtnext)
        t0 = time.time()
        dftemp = utils.subsetbytime(dfday, dt, dtnext)
        
        trxft = None
        ntrades = 0
        if ntradesnew > 0:
            # don't bother counting if no new orders in entire time period
            ntrades = dftemp.count() 
            if ntrades > 0:
                trxft = trxfn.features(dftemp.toPandas(), brotrfm)
            
        tradesfeatures[dt] = trxft
        dt = dtnext
        
        if verbose > 0:
            sreport = 'dt: %s ntrades: %d done in: %.2f' %\
                (dt, ntrades, time.time()-t0)
            if verbose > 1:
                sreport += '\n\tfeatures:\n\t' + str(trxft)
            print (sreport)

    if verbose > 0:
        print ('all trades features done in: %.2f' % (time.time()-t0all))
    return tradesfeatures


if __name__ == '__main__':

    params = {
        'symbol': 'TD',
        'date_string': '2019-02-04',
        'freq': '1H',
        'tstart_string': '10:00',
        'tend_string': '12:00',
        'verbose': 1}

    x = features(**params)