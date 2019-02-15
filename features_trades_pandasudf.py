import time
import pandas as pd
import numpy as np

import sparkdfutils as utils
import trades_functions as trxfn

from pyspark.sql.functions import pandas_udf, PandasUDFType, date_trunc


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
    dttruncfmt = 'minute', tstart_string = '09:30', tend_string = '16:00',
    verbose = 0):
    '''return dict of features of trades
    features at HH:MM are computed trades that came in during [HH:MM, HH:(MM+1))

    Args:
        symbol: str of ticker
        date_string: str of YYYY-MM-DD
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
    dfday = dailytrades(symbol, date_string)
    dfday.cache()

    # add str of discretized time (trunc to dttruncfmt) for new orders
    dfnewords = utils.subsetbytime(dfday, tstartdt, tenddt)
    dfnewords = dfnewords.withColumn('utcdtdiscrete', date_trunc(dfnewords, dttruncfmt).cast('string'))
    dfnewords = dfnewords.withColumn('outputjson', dfnewords['utcdtdiscrete'])
    # extract trading times
    tradingtimesutc = [row.utcdtdiscrete for row in dfnewords.select('utcdtdiscrete').distinct().collect()]

    # broker transfer matrix
    brotrfm = brokertrfmatrix()

    if verbose > 0:
        print ('cached orders for %s done in: %.2f norders: %d' %\
            (date_string, time.time()-t0, dfday.count()))


    # trades features
    if verbose > 1:
        t1 = time.time()

    group_column = 'utcdtdiscrete'
    json_column = 'outputjson'
    schema = dfnewords.select(group_column, json_column).schema

    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def tradesfeatures(pdf):
        '''return features as json string from new orders in pdf'''
        dtutc = pd.to_datetime(pdf[group_column].iloc[0]).tz_localize(tz='UTC')
        dtlocal = dtutc.tz_convert('US/Eastern')
        tradesft = Trades(dftemp, verbose=verbose-1).features(brotrfm)
        return pd.DataFrame([[dtutc, json.dumps(tradesft)]], columns = [group_column, json_column])

    tradesfeaturespdf = dfnewords.groupby(group_column).apply(tradesfeatures).toPandas()
    tradesfeatures = {pd.to_datetime(row['utcdtdiscrete']).tz_localize(tz='UTC').tz_convert('US/Eastern'):\
        json.loads(row['outputjson'])
        for index, row in ordfeaturespdf}

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