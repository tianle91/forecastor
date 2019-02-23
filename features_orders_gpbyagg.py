import time
import pandas as pd
import numpy as np
import sparkdfutils as utils
from Book import Book


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


def orderbook(ordersdf, verbose=0):
    '''return table of orders agg by side, price'''
    bk = ordersdf.groupby(['side', 'price']).agg({'book_change': 'sum'})
    bk = bk.withColumnRenamed('sum(book_change)', 'quantity')
    bk = bk.filter('quantity != 0')
    bk = bk.orderBy('price')
    if verbose > 0:
        print ('len:', bk.count())
    return bk


def getordersfilstr(ordtype=None, side=None, touch=None):
    '''string for ordersdf.filter(string)
	Args:
		ordertype: one of [None, 'New', 'Cancelled', 'Executed']
		side: one of [None, 'Buy', 'Sell']
		touch: one of [None, (maxbid, minask)]
    '''
    s = None

    if ordtype is None:
        pass
    elif ordtype == 'New':
        s = 'book_change > 0'
    elif ordtype == 'Cancelled':
        s = '''reason = 'Cancelled' '''
    elif ordtype == 'Executed':
        s = '''(reason = 'Filled' OR reason = 'Partial Fill')'''
    
    if touch is not None:
        maxbid, minask = touch
        if s is not None:
        	s += ' AND '
        else: 
        	s = ''
        s += 'price BETWEEN %s AND %s' % (maxbid, minask)

    if side in ['Buy', 'Sell']:
    	if s is not None:
    		s += ' AND '
    	else:
    		s = ''
    	s += '''side == '%s' ''' % (side)
    
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
    freq = None
    if tsunit == 'MINUTE':
        freq = '1min'
    else:
        raise ValueError('freq not done for tsunit:'+tsunit)

    # key for output dictionary
    # trading times is in US/Eastern
    tradingtimes = utils.tradingtimes(date_string, tstart_string, tend_string, freq, tz='US/Eastern')

    # get all orders in day
    if verbose > 0:
        t0 = time.time()

    # get all transactions prior to tradingtimes[-1]
    dfday = dailyorders(symbol, date_string, venue, tsunit)
    dfday = utils.subsetbytime(dfday, tradingtimes[-1])
    dfday.cache()

    if verbose > 0:
        print ('cached orders for %s done in: %.2f norders: %d' %\
            (date_string, time.time()-t0, dfday.count()))

    # utc stuff is from dfday where there are observations
    tradingtimesutc = dfday.select('timed').distinct().toPandas()
    tradingtimesutc = [val for index, val in tradingtimesutc['timed'].iteritems()]
    if verbose > 1:
        print ('trading times in dfday in US/Eastern:')
        print ([utils.utctimestamp_to_tz(dtutc, 'US/Eastern') for dtutc in tradingtimesutc])


    # book features
    if verbose > 1:
        t1 = time.time()
        print ('doing book features')

    bookfeatures = {}
    for dtutc in tradingtimesutc:
        if verbose > 2:
            t2 = time.time()
        bookdf = orderbook(dfday.filter('''time < %s''' % (dtutc)), verbose=verbose-2).toPandas()
        dt = utils.utctimestamp_to_tz(dtutc, 'US/Eastern')
        bookfeatures[dt] = Book(bookdf, verbose=verbose-2).features()
        if verbose > 2:
            print ('dt: %s done in: %.2f' % (dt, time.time()-t2))

    if verbose > 1:
        print ('book features done in: %.2f' % (time.time()-t1))












    # orders features
    if verbose > 1:
        t1 = time.time()
        print ('doing orders features')


    def worker(colname, aggfn, ordtype, side, touch, verbose=verbose-1):
    	k = '%s(%s)_for_%s_%s_orders' % (aggfn, colname, ordtype if ordtype is not None else 'All', side if side is not None else 'All')
    	k += '_at_touch' if touch is not None else ''
    	filstr = getordersfilstr(ordtype, side, touch)
    	if verbose > 0:
    		t2 = time.time()
    		print ('k:%s\nfilstr:%s' % (k, filstr))
    	v = utils.get_fil_agg(dfday, colname, aggfn, filstr=filstr)
    	if verbose > 0:
    		print ('done in: %.2f' % (time.time()-t0))
    		if verbose > 1:
    			print (v)
    	return k, v

    ftorders = [worker(colname, aggfn, ordtype, side, touch)
    	for colname, aggfn in [('*', 'count'), ('book_change', 'sum')]
    	for ordtype in [None, 'New', 'Cancelled', 'Executed']
    	for side in [None, 'Buy', 'Sell']
    	for touch in [None]]

    if verbose > 1:
        print ('orders features done in: %.2f' % (time.time()-t1))

    
    dfday.unpersist()
    return ftorders


if __name__ == '__main__':

    params = {
        'symbol': 'TD',
        'date_string': '2019-02-04',
        'tsunit': 'MINUTE',
        'tstart_string': '10:00',
        'tend_string': '12:00',
        'verbose': 1}

    x = features(**params)