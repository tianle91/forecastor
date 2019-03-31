import os
import gzip
import time
import pickle
import argparse
import numpy as np
import pandas as pd


# ------------------------------------------------------------------------------
# parse arguments and shit
# orders_features:
#     takes 350s/60evals ~ 6m/60evals ~ 1m/10evals
#     for 44*60evals, expect 44*6m = 260m = 4h 
# ------------------------------------------------------------------------------
#symbol = 'TD'
#symbol = 'BPY.UN'
#symbol = 'UFS'
#symbol = 'VFV'

#datelenname = '1wk'
#datelenname = '1mo'
#datelenname = '2mo'

#timelenname = '1h'
#timelenname = 'fullday'

#tsunit = 'MINUTE'
#tsunit = 'SECOND'

print ('doing datelen:%s timelen:%s tsunit:%s for symbol: %s' % (datelenname, timelenname, tsunit, symbol))
# datelenname = '1mo', timelen: '1h', tsunit: 'MINUTE', done in 1h
# datelenname = '1mo', timelen: '1h', tsunit: 'SECOND', done in 1.5h


# process arguments
if timelenname == '1h':
    tstart_string = '10:00'
    tend_string = '11:00'
elif timelenname == '2h':
    tstart_string = '10:00'
    tend_string = '12:00'
elif timelenname == 'morning':
    tstart_string = '09:30'
    tend_string = '12:00'
elif timelenname == 'fullday':
    tstart_string = '09:30'
    tend_string = '16:00'

start = pd.to_datetime('2018-04-01')
if datelenname == '1wk':
    end = pd.to_datetime('2018-04-08')
elif datelenname == '1mo':
    end = pd.to_datetime('2018-05-01')
elif datelenname == '2mo':
    end = pd.to_datetime('2018-06-01')
elif datelenname == '6mo':
    end = pd.to_datetime('2018-10-01')


# ------------------------------------------------------------------------------
# initialize parameters
# ------------------------------------------------------------------------------
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
pickle.dump(dates, open(os.getcwd() + '/data/dl:%s_tl:%s_ts:%s_SYM:%s_dates.pickle' % (datelenname, timelenname, tsunit, symbol), 'wb'))


# ------------------------------------------------------------------------------
# run the orders features
# ------------------------------------------------------------------------------
exec(open('features_orders.py').read())

def worker(dt, overwrite=False, verbose=1):
    fname = os.getcwd() + '/data/tl:%s_ts:%s_dt:%s_SYM:%s_orders.pickle.gz' % (timelenname, tsunit, dt, symbol)
    if overwrite or not os.path.isfile(fname):
        print ('caching fname: %s' % (fname))
        out = features(**getparams(dt, verbose=verbose))
        pickle.dump(out, gzip.open(fname, 'wb'))
    else:
        print ('fname: %s, overwrite: %s, os.path.isfile(fname): %s' % (fname, overwrite, os.path.isfile(fname)))

for dt in dates:
    temp =  worker(dt)


# ------------------------------------------------------------------------------
# run the trades features
# ------------------------------------------------------------------------------
exec(open('features_trades.py').read())

def worker(dt, overwrite=False, verbose=1):
    fname = os.getcwd() + '/data/tl:%s_ts:%s_dt:%s_SYM:%s_trades.pickle.gz' % (timelenname, tsunit, dt, symbol)
    if overwrite or not os.path.isfile(fname):
        print ('caching fname: %s' % (fname))
        out = features(**getparams(dt, verbose=verbose))
        pickle.dump(out, gzip.open(fname, 'wb'))
    else:
        print ('fname: %s, overwrite: %s, os.path.isfile(fname): %s' % (fname, overwrite, os.path.isfile(fname)))

for dt in dates:
    temp =  worker(dt)