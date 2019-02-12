import numpy as np
import pandas as pd


def utctimestamp(dt):
    s = dt.tz_convert('UTC')
    s = s.strftime('%Y-%m-%d %H:%M:%S')
    return s


def subsetbytime(df, timestamp0, timestamp1=None, verbose=0):

    if timestamp1 is None:
        s = '''time < '%s' ''' % (utctimestamp(timestamp0))
    else:
        if not (timestamp0 < timestamp1):
            raise ValueError('not (timestamp0 < timestamp1)!')
        t0, t1 = utctimestamp(timestamp0), utctimestamp(timestamp1)
        s = '''time >= '%s' AND time < '%s' ''' % (t0, t1)
    
    out = df.filter(s)
    if verbose > 0:
        print ('len:', out.count())
    return out