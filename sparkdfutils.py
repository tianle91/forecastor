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


def tradingtimes(freq, tstart_string='09:30', tend_string='16:00'):
    '''return array of datetimes by freq in US/Eastern tz'''

    out = pd.date_range(
        start = pd.to_datetime(date_string + ' %s' % (tstart_string)),
        end = pd.to_datetime(date_string + ' %s' % (tend_string)),
        tz = 'US/Eastern',
        freq = freq)

    if tstart_string == '09:30':
        # displace by 1ms if at start of trading
        out[0] = pd.to_datetime(date_string + ' %s:00.001' % (tstart_string))

    return out