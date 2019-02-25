import gzip
import json
import time
import pickle
import numpy as np
import pandas as pd


datelenname = '1wk'
timelenname = '1h'
tsunit = 'MINUTE'
symbol = 'TD'


# process arguments
if timelenname == '1h':
    tstart_string = '10:00'
    tend_string = '11:00'
elif timelenname == 'fullday':
    tstart_string = '09:30'
    tend_string = '16:00'

if datelenname == '1wk':
    start = pd.to_datetime('2018-04-01')
    end = pd.to_datetime('2018-04-08')
elif datelenname == '1mo':
    start = pd.to_datetime('2018-04-01')
    end = pd.to_datetime('2018-05-31')

jobname = '%s-%s' % (datelenname, timelenname)


# initialize parameters
dates = pd.date_range(start = start, end = end, freq = 'B')
print ('dates:', dates)


def getparams(dt, verbose):
    out = {
        'symbol': symbol,
        'date_string': dt.strftime('%Y-%m-%d'),
        'tsunit': tsunit, 
        'tstart_string': tstart_string, 
        'tend_string': tend_string, 
        'verbose': verbose,
    }
    return out


t1 = time.time()
exec(open('features_orders_gpbyagg.py').read())
resdorders = {dt: features(**getparams(dt, verbose=1)) for dt in dates}
print ('done in: %.2f' % (time.time()-t1))

pickle.dump(resdorders, gzip.open('%s_SYM:%s_orders.pickle.gz' % (jobname, symbol), 'wb'))


t1 = time.time()
exec(open('features_trades.py').read())
resdtrades = {dt: features(**getparams(dt, verbose=1)) for dt in dates}
print ('done in: %.2f' % (time.time()-t1))

pickle.dump(resdtrades, gzip.open('%s_SYM:%s_trades.pickle.gz' % (jobname, symbol), 'wb'))