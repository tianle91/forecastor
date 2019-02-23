import numpy as np
import pandas as pd


def utctimestamp(dt):
    if dt.tzinfo is None:
        raise ValueError('no timezone set for dt!')
    s = dt.tz_convert('UTC')
    s = s.strftime('%Y-%m-%d %H:%M:%S')
    return s


def utctimestamp_to_tz(dt, tz='US/Eastern'):
    if dt.tzinfo is None:
        dt = dt.tz_localize('UTC')
    elif dt.tzinfo != 'UTC':
        raise ValueError('dt is set to: %s, but is not UTC!' % (dt.tzinfo))
    else:
        return dt.tz_convert(tz)


def subsetbytime(df, timestamp0, timestamp1=None, verbose=0):
    '''return subset of df. 
    if timestamp1 is None, return subset [0000, timestamp0), 
    otherwise return subset [timestamp0, timestamp1).
    df.time is UTC, so localized time is required.
    '''
    if timestamp0.tzinfo is None:
        raise ValueError('no timezone set for timestamp0! df.time is UTC time.')

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


def tradingtimes(date_string, tstart_string, tend_string, freq, tz):
    '''return array of datetimes by freq in US/Eastern tz'''

    out = pd.date_range(
        start = pd.to_datetime(date_string + ' %s' % (tstart_string)),
        end = pd.to_datetime(date_string + ' %s' % (tend_string)),
        tz = tz,
        freq = freq)
    out = list(out)

    if tstart_string == '09:30':
        # displace by 1ms if at start of trading
        tstartdt = pd.to_datetime(date_string + ' %s:00.001' % (tstart_string))
        tstartdt = tstartdt.tz_localize(tz=tz)
        out[0] = tstartdt

    return out