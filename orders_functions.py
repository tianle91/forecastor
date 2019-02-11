# NO SPARK HERE.
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import abs


def filstr(ordtype='New', side='All', touch=None):
    '''return filter condition string'''
    s = ''

    if ordtype == 'New':
        s += 'book_change > 0 '
    elif ordtype == 'Cancelled':
        s += '''reason == 'Cancelled' '''
    elif ordtype == 'Executed':
        s += '''reason == 'Filled' OR reason == 'Partial Fill' '''
    else:
        raise ValueError('invalid orddtype argument: %s' % ordtype)
    
    if touch is not None:
        bestbid, bestask = touch
        s += '''AND price BETWEEN %s AND %s ''' % (bestbid, bestask)

    if side in ['Buy', 'Sell']:
        s += '''AND side == '%s' ''' % (side)
    elif side == 'All':
        pass
    else:
        raise ValueError('invalid side argument: %s' % side)
    
    return s


def aggtype(df, filstr=None, type='Number'):
    '''return count/sum of df.filter(filstr)'''
    if filstr is not None:
        df = df.filter(filstr)
    
    count = df.count()
    if ctype == 'Number':
        return numorders
    
    # if ctype is not 'Number', need to compute sum(abs(book_change))
    df = df.withColumn('absch', abs(df.book_change))
    sumq = df.agg({'absq': 'sum'}).collect()[0]['sum(absq)']

    if ctype == 'Volume':
        return sumq
    elif ctype == 'AvgVolume':
        if numorders == 0:
            return 0
        else:
            return sumq/numorders


def features(df, touchval):
    '''return dict of new order features
    Args:
        df: spark dataframe object
        touchval: tuple of (bestbid, bestask)
    '''
    args = [{'ordtype': ordtype, 'side': side, 'ctype': ctype, 'touch': touch}
        for ordtype in ['New', 'Cancelled', 'Executed']
        for side in ['Buy', 'Sell', 'All']
        for ctype in ['Number', 'Volume', 'AvgVolume']
        for touch in [None, touchval]]

    def namer(ordtype, side, ctype, touch):
        s = ''
        s += ctype
        s += '-of-' + ordtype
        s += '-' + side + '-Orders'
        if touch is not None:
            s += '-at-touch'
        return s

    out = {}
    for arg in args:
        filstr = filstr(arg['ordtype'], arg['side'], arg['touch'])
        out[namer(**arg)] = self.counttype(filstr, arg['ctype'])

    return out