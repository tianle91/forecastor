import gzip
import json
import time
import pickle
import numpy as np
import pandas as pd


jobname = 'long'

tstart_string = '10:00'
tend_string = '11:00'
freq = '1min'
symbol = 'TD'

dates = pd.date_range(
    start = pd.to_datetime('2018-04-01'),
    end = pd.to_datetime('2018-06-01') if jobname == 'short' else pd.to_datetime('2018-04-03'),
    freq = 'B')
print ('dates:', dates)


def getparams(dt, verbose):
    out = {
        'symbol': symbol,
        'date_string': dt.strftime('%Y-%m-%d'),
        'freq': freq, 
        'tstart_string': tstart_string, 
        'tend_string': tend_string, 
        'verbose': verbose,
    }
    return out


t1 = time.time()
exec(open('features_orders.py').read())
resdorders = {dt: features(**getparams(dt, verbose=1)) for dt in dates}
print ('done in: %.2f' % (time.time()-t1))

pickle.dump(resdorders, gzip.open('%s_SYM:%s_orders.pickle.gz' % (jobname, symbol), 'wb'))


t1 = time.time()
exec(open('features_trades.py').read())
resdtrades = {dt: features(**getparams(dt, verbose=1)) for dt in dates}
print ('done in: %.2f' % (time.time()-t1))

pickle.dump(resdtrades, gzip.open('%s_SYM:%s_trades.pickle.gz' % (jobname, symbol), 'wb'))