import time
import pandas as pd
import numpy as np

import sparkdfutils as utils
from Book import Book
from Orders import Orders

from pyspark.sql.functions import pandas_udf, PandasUDFType, date_trunc


def dailyorders(symbol, date_string, venue=):
    '''return table of orders'''
    s = '''SELECT
            book_change, 
            side, 
            price,
            reason,
            time
        FROM orderbook_tsx 
        WHERE symbol = '%s' 
            AND date_string = '%s' 
            AND price > 0
            AND venue = '%s'
        ORDER BY time ASC'''
    sargs = (symbol, date_string, venue)
    return spark.sql(s%sargs) 


def orderbook(ordersdf, verbose=0):
    '''return table of orders agg by side, price'''
    bk = ordersdf.groupby(['side', 'price']).agg({'book_change': 'sum'})
    bk = bk.withColumnRenamed('sum(book_change)', 'quantity')
    bk = bk.filter('quantity != 0')
    bk = bk.orderBy('price')
    if verbose > 0:
        print ('len:', bk.count())
    return bk


def features(symbol, date_string, venue = 'TSX',
    dttruncfmt = 'minute', tstart_string = '09:30', tend_string = '16:00',
    verbose = 0):
    '''return dict of features of orderbook and new orders
    features at HH:MM are computed using orderbook [00:00, HH:MM) and 
    orders that came in during [HH:MM, HH:(MM+1))

    Args:
        symbol: str of ticker
        date_string: str of YYYY-MM-DD
        venue: str of exchange
        dttruncfmt:  [
            'year', 'yyyy', 'yy', 'month', 'mon', 'mm', 'day', 'dd', 'hour', 
            'minute', 'second', 'week', 'quarter'
        ]
        tstart_string: str of 'HH:MM' for start time
        tend_string: str of 'HH:MM' for end time
    '''
    if verbose > 0:
        t0 = time.time()

    tstartdt = pd.to_datetime(date_string + ' %s' % (tstart_string))
    tstartdt = tstartdt.tz_localize(tz='US/Eastern')
    tenddt = pd.to_datetime(date_string + ' %s' % (tend_string))
    tenddt = tenddt.tz_localize(tz='US/Eastern')

    # get all transactions in day
    dfday = dailyorders(symbol, date_string, venue)
    dfday.cache()

    # add str of discretized time (trunc to dttruncfmt) for new orders
    dfnewords = utils.subsetbytime(dfday, tstartdt, tenddt)
    dfnewords = dfnewords.withColumn('utcdtdiscrete', date_trunc(dfnewords, dttruncfmt).cast('string'))
    dfnewords = dfnewords.withColumn('outputjson', dfnewords['utcdtdiscrete'])
    # extract trading times
    tradingtimesutc = [row.utcdtdiscrete for row in dfnewords.select('utcdtdiscrete').distinct().collect()]

    if verbose > 0:
        print ('cached orders for %s done in: %.2f norders: %d' %\
            (date_string, time.time()-t0, dfday.count()))

    # book features
    if verbose > 1:
        t1 = time.time()
    bookfeatures = {}
    for dt in tradingtimesutc:
        dftemp = utils.subsetbytime(dfday, pd.to_datetime(dt).tz_localize(tz='UTC')).toPandas()
        bookfeatures[dt.tz_convert('US/Eastern')] = Book(dftemp, verbose=verbose-1).features()
    if verbose > 1:
        print ('book features done in: %.2f' % (time.time()-t1))


    # orders features
    if verbose > 1:
        t1 = time.time()

    group_column = 'utcdtdiscrete'
    json_column = 'outputjson'
    schema = dfnewords.select(group_column, json_column).schema

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def orderfeatures(pdf):
        '''return features as json string from new orders in pdf'''
        dtutc = pd.to_datetime(pdf[group_column].iloc[0]).tz_localize(tz='UTC')
        dtlocal = dtutc.tz_convert('US/Eastern')
        touchval = bookfeatures[dt]['maxbid'], bookfeatures[dt]['minask']
        ordft = Orders(pdf, verbose=verbose-1).features(touchval)
        return pd.DataFrame([[dtutc, json.dumps(ordft)]], columns = [group_column, json_column])

    ordfeaturespdf = dfnewords.groupby(group_column).apply(orderfeatures).toPandas()
    ordfeatures = {pd.to_datetime(row['utcdtdiscrete']).tz_localize(tz='UTC').tz_convert('US/Eastern'):\
        json.loads(row['outputjson'])
        for index, row in ordfeaturespdf}

    if verbose > 1:
        print ('orders features done in: %.2f' % (time.time()-t1))

    # aggregate features
    out = {}
    for k in bookfeatures:
        out[k] = {'book': bookfeatures[k]}
    for k in ordfeatures:
        if k in out:
            out[k]['orders': ordfeatures[k]]
        else:
            raise ValueError('k: %s not in ordfeatures!' % (k))
    if verbose > 0:
        print ('all book / orders features done in: %.2f' % (time.time()-t0))
    
    return out


if __name__ == '__main__':

    params = {
        'symbol': 'TD',
        'date_string': '2019-02-04',
        'freq': '1H',
        'tstart_string': '10:00',
        'tend_string': '12:00',
        'verbose': 1}

    x = features(**params)