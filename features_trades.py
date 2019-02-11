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
            AND price < 99999
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