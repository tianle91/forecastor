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


def features(symbol, date_string, venue = 'TSX',
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
        t0 = time.time()

    tradingtimes = utils.tradingtimes(date_string, 
        tstart_string, tend_string, freq, tz='US/Eastern')
    brotrfm = brokertrfmatrix()

    dfday = dailytrades(symbol, date_string)
    dfday.cache()

    if verbose > 0:
        print ('cached orders for %s done in: %.2f norders: %d' %\
            (date_string, time.time()-t0, dfday.count()))


    # orders features
    if verbose > 1:
        t1 = time.time()
    tradesfeatures = {}
    tradesfeatures[-1] = None
    dt = tradingtimes[0]
    for dtnext in tradingtimes[1:]:
        dftemp = utils.subsetbytime(dfday, dt, dtnext).toPandas()
        tradesfeatures[dt] = Trades(dftemp, verbose=verbose-1).features(brotrfm)
        dt = dtnext
    if verbose > 1:
        print ('trades features done in: %.2f' % (time.time()-t1))

    if verbose > 0:
        print ('all trades features done in: %.2f' % (time.time()-t0))
    
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