import os
import gzip
import time
import pickle
import argparse
import numpy as np
import pandas as pd


tsunit = 'MINUTE'

symbol = 'TD'
#symbol = 'BPY.UN'
#symbol = 'UFS'
#symbol = 'VFV'

#datelenname = '1wk'
datelenname = '1mo'

timelenname = '1h'
#timelenname = 'fullday'


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


# ------------------------------------------------------------------------------
# dump the dates
# ------------------------------------------------------------------------------
pickle.dump(dates, open(os.getcwd() + '/data/%s_SYM:%s_dates.pickle' % (jobname, symbol), 'wb'))


# ------------------------------------------------------------------------------
# run the orders features
# ------------------------------------------------------------------------------
exec(open('features_orders_gpbyagg.py').read())

def worker(dt, jobid, overwrite=True, verbose=1):
    fname = os.getcwd() + '/data/%s_SYM:%s_dt:%s_orders.pickle.gz' % (jobname, symbol, dt)
    if overwrite and not os.path.isfile(fname):
        out = features(**getparams(dt, verbose=verbose))
        pickle.dump(out, gzip.open(fname, 'wb'))

for dt in dates:
    temp =  worker(dt, jobname)


# ------------------------------------------------------------------------------
# run the trades features
# ------------------------------------------------------------------------------
exec(open('features_trades_gpbyagg.py').read())

def worker(dt, jobid, overwrite=True, verbose=1):
    fname = os.getcwd() + '/data/%s_SYM:%s_dt:%s_trades.pickle.gz' % (jobname, symbol, dt)
    if overwrite and not os.path.isfile(fname):
        out = features(**getparams(dt, verbose=verbose))
        pickle.dump(out, gzip.open(fname, 'wb'))

for dt in dates:
    temp =  worker(dt, jobname)