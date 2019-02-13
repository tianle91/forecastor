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
    out = pd.DataFrame(
        [np.zeros(len(brokeridlist))]*len(brokeridlist), 
        index=brokeridlist, columns=brokeridlist)
    return out


if __name__ == '__main__':

    symbol = 'TD'
    date_string = '2019-02-04'
    venue = 'TSX'
    freq = '30min'
    tstart_string = '10:00'
    tend_string = '12:00'
    verbose = 2

    tradingtimes = utils.tradingtimes(date_string, 
        tstart_string, tend_string, freq, tz='US/Eastern')

    # get all transactions prior to tradingtimes[-1]
    dfday = dailytrades(symbol, date_string)
    dfday = utils.subsetbytime(dfday, tradingtimes[-1])
    dfday.cache()
    if verbose > 0:
        print ('get dailyorders done in: %.2f norders: %d before: %s' %\
            (time.time()-t0, dfday.count(), tend_string))

    trxfn.features(dfday.toPandas(), brokertrfmatrix())