import time
import pandas as pd
import numpy as np
import sparkdfutils as utils
from pyspark.sql.types import *
import pyspark.sql.functions as F


def dailyorders(symbol, date_string, venue, tsunit):
    '''return table of orders
    Args:
        tsunit: one of ["HOUR", "MINUTE", "SECOND"]
            https://spark.apache.org/docs/2.3.0/api/sql/#date_trunc
    '''
    s = '''SELECT
            book_change,
            side, 
            price,
            reason,
            time,
            date_trunc('%s', time) as timed
        FROM orderbook_tsx 
        WHERE symbol = '%s' 
            AND date_string = '%s' 
            AND price > 0
            AND venue = '%s'
        ORDER BY time ASC'''
    sargs = (tsunit, symbol, date_string, venue)
    return spark.sql(s%sargs)


def dailycbbo(symbol, date_string, tsunit):
    '''return table of consolidated order
    Args:
        tsunit: one of ["HOUR", "MINUTE", "SECOND"]
            https://spark.apache.org/docs/2.3.0/api/sql/#date_trunc
    '''
    s = '''SELECT
            bid_price,
            bid_size,
            ask_price,
            ask_size,
            ask_price - bid_price AS spread,
            (bid_price + ask_price) / 2 AS mid_price,
            ((ask_size * bid_price) + (bid_size * ask_price)) / (ask_size + bid_size) AS weighted_price,
            time,
            date_trunc('%s', time) AS timed,
            ROW_NUMBER() OVER (ORDER BY time) row
        FROM cbbo
        WHERE symbol = '%s' 
            AND date_string = '%s' 
        ORDER BY time ASC'''
    sargs = (tsunit, symbol, date_string)
    dftemp = spark.sql(s%sargs)
    dftemp.createOrReplaceTempView('cbbotemp')

    s = '''SELECT
            a.*, 
            a.mid_price - b.mid_price AS mid_price_diff,
            LOG(a.mid_price/b.mid_price) AS mid_price_logreturn,
            a.weighted_price - b.weighted_price AS weighted_price_diff,
            LOG(a.weighted_price/b.weighted_price) AS weighted_price_logreturn
        FROM cbbotemp a
        LEFT JOIN cbbotemp b
            ON a.row = b.row-1'''
    dftemp = spark.sql(s)
    dftemp = dftemp.withColumn('mid_price_diff2', F.pow(dftemp.mid_price_diff, 2))
    dftemp = dftemp.withColumn('weighted_price_diff2', F.pow(dftemp.weighted_price_diff, 2))
    return dftemp


def getordersfilstr(ordtype=None, side=None, touch=False):
    '''string for ordersdf.filter(string)
    Args:
        ordtype: one of [None, 'New', 'Cancelled', 'Executed']
        side: one of [None, 'Buy', 'Sell']
        touch: bool
    '''
    s = ''

    if ordtype == 'Cancelled':
        s += '''reason = 'Cancelled' '''
    elif ordtype == 'New':
        s += '''(reason = 'New Order' OR reason = 'Changed')'''
    elif ordtype == 'Executed':
        s += '''(reason = 'Filled' OR reason = 'Partial Fill')'''
    
    if touch:
        if s != '':
            s += ' AND '
        s += '''((price >= maxbid AND side == 'Buy') OR (price <= minask AND side == 'Sell'))'''

    if side in ['Buy', 'Sell']:
        if s != '':
            s += ' AND '
        s += '''(side == '%s')''' % (side)
    
    return s


def features(symbol, date_string, venue = 'TSX',
    tsunit = 'MINUTE', tstart_string = '09:30', tend_string = '16:00',
    verbose = 0):
    '''return dict of features of orderbook and new orders
    features at HH:MM are computed using orderbook [00:00, HH:MM) and 
    orders that came in during [HH:MM, HH:(MM+1))

    Args:
        symbol: str of ticker
        date_string: str of YYYY-MM-DD
        venue: str of exchange
        tsunit: one of ["HOUR", "MINUTE", "SECOND"]
            https://spark.apache.org/docs/2.3.0/api/sql/#date_trunc
        tstart_string: str of 'HH:MM' for start time
        tend_string: str of 'HH:MM' for end time
    '''

    # set up key for output dictionary, trading times is in US/Eastern
    freq = '1H' if tsunit == 'HOUR' else '1min' if tsunit == 'MINUTE' else '1S' if tsunit == 'SECOND' else None
    # tradingtimes = utils.tradingtimes(date_string, tstart_string, tend_string, freq, tz='US/Eastern')
    # tradingtimes.sort()
    tradingtimesdf = utils.tradingtimes(date_string, tstart_string, tend_string, freq, tz='US/Eastern')
    tradingtimesdf.sort()

    if verbose > 1:
        print ('len(tradingtimesdf):', len(tradingtimesdf))
        if verbose > 2:
            print ('trading times in dfday in US/Eastern:')
            print ([str(dt) for dt in tradingtimesdf])


    # --------------------------------------------------------------------------
    # book features
    # --------------------------------------------------------------------------
    if verbose > 0:
        t1 = time.time()
        print ('doing book features')

    # get all consolidated book changes prior to tradingtimes[-1]
    bkday = dailycbbo(symbol, date_string, tsunit)
    #bkday = utils.subsetbytime(bkday, tradingtimes[-1])
    bkday = utils.subsetbytime(bkday, tradingtimesdf[-1])
    if verbose > 0:
        print ('get cbbo for %s done in: %.2f nrows: %d' %\
            (date_string, time.time()-t1, bkday.count()))


    params = [
        {'colname': colname, 
            'aggfn': aggfn, 
            'covname': colname if aggfn == 'first_value' else 'cbbo_%s(%s)' % (aggfn, colname),
            'verbose': verbose-1}
        for aggfn, colname in [
           ('first_value', 'bid_price'), 
           ('first_value', 'ask_price'), 
           ('first_value', 'spread'),
           ('first_value', 'mid_price'),
           ('first_value', 'weighted_price'),
           ('sum', 'mid_price_diff2'),
           ('mean', 'mid_price_diff2'),
           ('stddev', 'mid_price_diff2'),
           ('mean', 'mid_price_logreturn'),
           ('stddev', 'mid_price_logreturn'),
           ('sum', 'weighted_price_diff2'),
           ('mean', 'weighted_price_diff2'),
           ('stddev', 'weighted_price_diff2'),
           ('mean', 'weighted_price_logreturn'),
           ('stddev', 'weighted_price_logreturn')
           ]
        ]

    if verbose > 0:
        print ('running features for cbbo')
    if verbose > 2:
        print ('feature params:')
        for paramtemp in params:
            print (paramtemp)

    if verbose > 0:
        t2 = time.time()

    aggparams = {partemp['colname']: partemp['aggfn'] for partemp in params}

    bkday.cache()
    bkdaygrouped = bkday.groupBy('timed').agg(aggparams).toPandas()
    bkday.unpersist()

    if verbose > 2:
        print ('bkdaygrouped.head()')
        print (bkdaygrouped.head())

    renameparams = {'%s(%s)' % (partemp['aggfn'], partemp['colname']): partemp['covname'] for partemp in params}
    bkdaygrouped = bkdaygrouped.rename(columns=renameparams)
    bkdaygrouped['maxbid'] = bkdaygrouped['bid_price']
    bkdaygrouped['minask'] = bkdaygrouped['ask_price']

    bookfeaturesbycovname = {}
    for colname in bkdaygrouped:
        if colname != 'timed':
            bookfeaturesbycovname[colname] = bkdaygrouped[['timed', colname]]

    if verbose > 0:
        print ('bookfeaturesbycovname for cbbo collected in %s' % (time.time()-t1))


    # --------------------------------------------------------------------------
    # change to dt key
    # --------------------------------------------------------------------------
    dummydict = {}
    for covname in bookfeaturesbycovname:
        dummydict[covname] = None
    bookfeatures = {dt: dummydict.copy() for dt in tradingtimesdf}    

    for covname in bookfeaturesbycovname:
        dftemp = bookfeaturesbycovname[covname]
        for index, row in dftemp.iterrows():
            dt, value = row[0], row[1]
            dt = utils.utctimestamp_to_tz(dt, 'US/Eastern')
            if dt in bookfeatures:
                try:
                    bookfeatures[dt][covname] = float(value)
                except:
                    pass

    # fill in missing values
    for covname in bookfeaturesbycovname:
        dtprev = None
        valueprev = None
        hitfirst = False
        # find first value
        for dt in tradingtimesdf:
            if not hitfirst:
                value = bookfeatures[dt][covname]
                if value is not None:
                    dtprev = dt
                    valueprev = value
                    hitfirst = True
        # fill missing
        for dt in tradingtimesdf:
            value = bookfeatures[dt][covname]
            if value is not None:
                dtprev = dt
                valueprev = value
            else:
                bookfeatures[dt][covname] = valueprev

    if verbose > 0:
        print ('\tcbbo features done in: %.2f' % (time.time()-t1))


    # --------------------------------------------------------------------------
    # create touch dataframe
    # --------------------------------------------------------------------------
    if verbose > 0:
        t0 = time.time()

    # get all transactions prior to tradingtimes[-1]
    dfday = dailyorders(symbol, date_string, venue, tsunit)
    dfday = utils.subsetbytime(dfday, tradingtimesdf[-1])
    if verbose > 0:
        print ('get orders for %s done in: %.2f norders: %d' %\
            (date_string, time.time()-t0, dfday.count()))

    schema = StructType([
        StructField("timedstr", StringType()),
        StructField("maxbid", DoubleType()),
        StructField("minask", DoubleType())])

    touchdf = spark.createDataFrame(
        [(utils.utctimestamp(dt), 
            bookfeatures[dt]['maxbid'], 
            bookfeatures[dt]['minask']) 
            for dt in bookfeatures],
        schema)

    touchdf = touchdf.withColumn('timed', touchdf.timedstr.cast("timestamp"))
    dfday = dfday.join(touchdf, "timed")
    dfday = dfday.withColumn('ABS(book_change)', F.abs(dfday.book_change))
    


    # --------------------------------------------------------------------------
    # orders features
    # --------------------------------------------------------------------------
    if verbose > 0:
        t2 = time.time()
        print ('doing features for orders')

    def covnamer(ordtype, side, touch):
        k = 'orders_type:%s_side:%s' %\
            (ordtype if ordtype is not None else 'All', 
                side if side is not None else 'All')
        k += '_at_touch' if touch else ''
        return k

    def worker(ordtype, side, touch, verbose):
        filstr = getordersfilstr(ordtype, side, touch)
        k = covnamer(ordtype, side, touch)
        if verbose > 0:
            t3 = time.time()
            print ('k:%s\nfilstr:%s' % (k, filstr))

        dftemp = dfday
        if filstr != '':
            dftemp = dftemp.filter(filstr)
        aggparams = {'*': 'COUNT', 'ABS(book_change)': 'sum'}
        dftemp = dftemp.groupBy('timed').agg(aggparams).toPandas()

        if verbose > 0:
            print ('done in: %.2f' % (time.time()-t3))
            if verbose > 1:
                print (dftemp.head(5))
        return k, dftemp

    params = [
        {'ordtype': ordtype, 
            'side': side, 
            'touch': touch,
            'verbose': verbose-1
        }
        for ordtype in [None, 'New', 'Cancelled', 'Executed']
        for side in [None, 'Buy', 'Sell']
        for touch in [False, True]
    ]

    dfday.cache()
    resl = map(lambda x: worker(**x), params)
    dfday.unpersist()

    orderfeaturesbycovname = {}
    for ktemp, groupeddftemp in resl:
        for colname in groupeddftemp:
            if colname != 'timed':
                orderfeaturesbycovname[ktemp+'_'+colname] = groupeddftemp[['timed', colname]]

    if verbose > 0:
        print ('\torders features done in: %.2f' % (time.time()-t2))


    # change to dt key    
    dummydict = {}
    for covname in orderfeaturesbycovname:
        dummydict[covname] = None
    ordersfeatures = {dt: dummydict.copy() for dt in tradingtimesdf}    

    for covname in orderfeaturesbycovname:
        dftemp = orderfeaturesbycovname[covname]
        for index, row in dftemp.iterrows():
            dt, value = row[0], row[1]
            dt = utils.utctimestamp_to_tz(dt, 'US/Eastern')
            if dt in ordersfeatures:
                try:
                    ordersfeatures[dt][covname] = float(value)
                except:
                    pass
                

    # --------------------------------------------------------------------------
    # collect and return
    # --------------------------------------------------------------------------   
    out = {}
    for dt in tradingtimesdf:
        out[dt] = {'book': bookfeatures[dt], 'orders': ordersfeatures[dt]}

    if verbose > 0:
        print ('number of covariates in book:', len(out[tradingtimesdf[0]]['book']))
        print ('number of covariates in orders:', len(out[tradingtimesdf[0]]['orders']))
        print ('all done in: %.2f' % (time.time()-t0))

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