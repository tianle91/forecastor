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


def getordersfilstr(ordtype=None, side=None, touch=False, ticks=None):
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
        if ticks is None:
            s += '''((price >= maxbid AND side == 'Buy') OR (price <= minask AND side == 'Sell'))'''
        else:
            vtmp = .01*ticks
            s += '''((price >= (maxbid-%f) AND side == 'Buy') OR (price <= (minask+%f) AND side == 'Sell'))''' % (vtmp, vtmp)

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
    tradingtimesdf = utils.tradingtimes(date_string, tstart_string, tend_string, freq, tz='US/Eastern')
    tradingtimesdf.sort()
    tdelta = tradingtimesdf[-1]-tradingtimesdf[-2]

    if verbose > 1:
        print ('len(tradingtimesdf):', len(tradingtimesdf))
    if verbose > 2:
        print ('trading times in dfday in US/Eastern:')
        print ([str(dt) for dt in tradingtimesdf])


    # --------------------------------------------------------------------------
    # book features
    # --------------------------------------------------------------------------
    if verbose > 0:
        t0 = time.time()
        print ('doing cbbo features')

    # get all consolidated book changes prior to tradingtimes[-1]
    bkday = dailycbbo(symbol, date_string, tsunit)
    bkday = utils.subsetbytime(bkday, tradingtimesdf[-1]+tdelta)
    bkday.cache()

    if verbose > 0:
        t1 = time.time()
        print ('cbbo: df %s done in: %.2f nrows: %d' %\
            (date_string, time.time()-t0, bkday.count()))

    bkfeaturesparaml = [
        ('first_value', 'bid_price'), 
        ('first_value', 'ask_price'), 
        ('first_value', 'spread'),
        ('first_value', 'mid_price'),
        ('first_value', 'weighted_price'),
        ('last_value', 'bid_price'), 
        ('last_value', 'ask_price'), 
        ('last_value', 'spread'),
        ('last_value', 'mid_price'),
        ('last_value', 'weighted_price'),
        ('max', 'bid_price'),
        ('max', 'ask_price'),
        ('max', 'spread'),
        ('max', 'mid_price'),
        ('max', 'weighted_price'),
        ('min', 'bid_price'),
        ('min', 'ask_price'),
        ('min', 'spread'),
        ('min', 'mid_price'),
        ('min', 'weighted_price'),
        ('sum', 'mid_price_diff2'),
        ('sum', 'weighted_price_diff2'),
        ('mean', 'mid_price_diff2'),
        ('mean', 'mid_price_logreturn'),
        ('mean', 'weighted_price_diff2'),
        ('mean', 'weighted_price_logreturn'),
        ('stddev', 'mid_price_diff2'),
        ('stddev', 'mid_price_logreturn'),
        ('stddev', 'weighted_price_diff2'),
        ('stddev', 'weighted_price_logreturn')
    ]

    if verbose > 2:
        print ('bkfeaturesparaml:')
        for aggfn, colname in bkfeaturesparaml:
            print ('aggfn: %s\tcolname: %s' % (aggfn, colname))

    bookfeaturesbycovname = {}
    for aggfn, colname in bkfeaturesparaml:
        bookfeaturesbycovname['%s(%s)' % (aggfn, colname)] = bkday.groupBy('timed').agg({colname: aggfn}).toPandas()
    bkday.unpersist()

    # add dummies
    bookfeaturesbycovname['maxbid'] = bookfeaturesbycovname['first_value(bid_price)']
    bookfeaturesbycovname['minask'] = bookfeaturesbycovname['first_value(ask_price)']

    if verbose > 0:
        t2 = time.time()
        print ('cbbo: bookfeaturesbycovname collected in %.2f' % (time.time()-t1))


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
                    print ('cbbo: not converted! covname: %s, dt: %s, value: %s' % (covname, dt, value))

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
        t3 = time.time()
        print ('cbbo: change to dt key done in: %.2f' % (t3-t2))
        print ('cbbo: all done in: %.2f' % (t3-t0))


    # --------------------------------------------------------------------------
    # create touch dataframe
    # --------------------------------------------------------------------------
    if verbose > 0:
        print ('create touch df')

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

    if verbose > 0:
        t4 = time.time()
        print ('touch df done in: %.2f' % (t4-t3))

    dfday = dailyorders(symbol, date_string, venue, tsunit)
    dfday = utils.subsetbytime(dfday, tradingtimesdf[-1]+tdelta)
    dfday.cache()

    if verbose > 0:
        t5 = time.time()
        print ('orders: df %s done in: %.2f nrows: %d' %\
            (date_string, t5-t4, dfday.count()))

    dfday.unpersist()
    dfday = dfday.join(touchdf, "timed")
    dfday = dfday.withColumn('ABS(book_change)', F.abs(dfday.book_change))
    dfday = dfday.orderBy("timed", ascending=True)
    dfday.cache()

    if verbose > 0:
        t6 = time.time()
        print ('orders: join with touchdf done in: %.2f' % (t6-t5))


    # --------------------------------------------------------------------------
    # orders features
    # --------------------------------------------------------------------------
    def worker(ordtype, side, touch, ticks, counttype):
        if verbose > 3:
            ttemp = time.time()
            print ('orders: filter ordtype: %s, side: %s, touch: %s, ticks: %s, counttype: %s' % (ordtype, side, touch, ticks, counttype))
        
        # covariate name
        covname = '%s_of_%s-type_%s-side' % (counttype, ordtype, side)
        if touch:
            covname += '_at-touch'
            if ticks is not None:
                covname += '-%s-ticks' % (ticks)

        # filter string
        filstr = getordersfilstr(ordtype, side, touch, ticks)

        # get covariate grouped by timed
        ## aggregate type
        if counttype == 'Volume':
            aggparams = {'ABS(book_change)': 'sum'}
        elif counttype == 'Number':
            aggparams = {'*': 'COUNT'}
        else:
            raise ValueError('invalid counttype: %s' % (counttype))
        ## filter string
        dftemp = dfday
        if filstr != '':
            dftemp = dftemp.filter(filstr)
        ## do groupby
        dftemp = dftemp.groupBy('timed').agg(aggparams).toPandas()
        
        if verbose > 3:
            print ('orders: groupby done in: %.2f' % (time.time()-ttemp))
            if verbose > 1:
                print (dftemp.head(5))

        return covname, dftemp


    params = [
        {'ordtype': ordtype, 
            'side': side, 
            'touch': True,
            'ticks': ticks,
            'counttype': counttype}
        for ordtype in [None, 'New', 'Executed']
        for side in [None, 'Buy', 'Sell']
        for counttype in ['Volume', 'Number']
        #for ticks in [None, 5, 10, 25, 50, 100, 250, 500, 1000]
        for ticks in [None, 5, 10, 50]
    ]
    params += [
        {'ordtype': ordtype, 
            'side': side, 
            'touch': False,
            'ticks': None,
            'counttype': counttype}
        for ordtype in [None, 'New', 'Cancelled', 'Executed']
        for side in [None, 'Buy', 'Sell']
        for counttype in ['Volume', 'Number']
    ]


    resl = map(lambda x: worker(**x), params)
    orderfeaturesbycovname = {k: v for k, v in resl}
    dfday.unpersist()

    if verbose > 0:
        t7 = time.time()
        print ('orders: orderfeaturesbycovname done in: %.2f' % (t7-t6))


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
                    print ('orders: not converted! covname: %s, dt: %s, value: %s' % (covname, dt, value))
              
    if verbose > 0:
        t8 = time.time()
        print ('orders: change to dt key done in: %.2f' % (t8-t7))


    # --------------------------------------------------------------------------
    # collect and return
    # --------------------------------------------------------------------------   
    out = {}
    for dt in tradingtimesdf:
        bkftdttemp = {'book_' + k: v for k, v in bookfeatures[dt].items()}
        odftdttemp = {'orders_' + k: v for k, v in ordersfeatures[dt].items()}
        out[dt] = {'book': bkftdttemp, 'orders': odftdttemp}

    if verbose > 0:
        t9 = time.time()
        print ('number of covariates in book:', len(out[tradingtimesdf[0]]['book']))
        print ('number of covariates in orders:', len(out[tradingtimesdf[0]]['orders']))
        print ('all done in: %.2f' % (t9-t0))

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