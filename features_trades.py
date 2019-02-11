import pandas as pd
import numpy as np
import sparkdfutils as utils


def dailytrades(symbol, date_string):
    '''return table of trades'''
        
    s = '''SELECT
            time,
            trade_size AS quantity,
            price,
            listing_exchange,
            buy_broker,
            sell_broker,
            trade_condition,
            record_type,
        FROM trades 
        WHERE symbol = '%s' 
            AND date_string = '%s' 
            AND price > 0
        ORDER BY time ASC'''
    
    sargs = (symbol, date_string)
    return spark.sql(s % sargs)


def features(df):

    df = df.groupBy('price').agg({'quantity': 'sum'}).toPandas()
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


if __name__ == '__main__':

    symbol = 'TD'
    venue = 'TSX'
    date_string = '2019-02-04'

    dfday = dailytrades(symbol, date_string)
    dfday.cache()

    
    #freq = '1H'
    #freq = '30min'
    #freq = '5min'
    freq = '1min'
    tradingtimes = pd.date_range(
        start = pd.to_datetime(date_string + ' 09:30:01'),
        end = pd.to_datetime(date_string + ' 16:00:01'),
        tz = 'US/Eastern',
        freq = freq)
    print ('len(tradingtimes):', len(tradingtimes))


    trxfeatures = {}
    # no new trades for first timestamp
    dtprev = tradingtimes[0]
    trxfeatures[dtprev] = None

    for dt in tradingtimes[1:]:
        t0 = time.time()
        dftemp = utils.subsetbytime(dfday, dtprev, dt)
        ntrades = dftemp.count()
        
        trxfttemp = None
        if ntrades > 0:
            trxfttemp = features(dftemp)
            
        trxfeatures[dt] = trxfttemp
        dtprev = dt
        print ('dtprev:%s dt:%s, done in:%s' % (dtprev, dt, time.time()-t0))