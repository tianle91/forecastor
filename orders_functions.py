# NO SPARK HERE.
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql.functions import abs


def filstr(ordtype=None, side=None, touch=None):
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

    if side is not None and side in ['Buy', 'Sell']:
        s += '''AND side == '%s' ''' % (side)
    else:
        raise ValueError('invalid side argument: %s' % side)
    
    return s


def aggtype(df, filstr=None):
    '''return count/sum of df.filter(filstr)'''
    if filstr is not None and len(filstr) > 0:
        df = df.filter(filstr)
    
    nrow = df.count()
    df = df.withColumn('absch', abs(df.book_change))
    sumq = df.agg({'absch': 'sum'}).collect()[0]['sum(absch)']

    return {'Number': nrow, 'Volume': sumq}


def namer(ordtype, side, touch):
    '''return covariate name'''
    if ordtype is None:
        s = 'All'
    else:
        s = ordtype

    if side is not None:
        s += '-' + side + '-'
    s += 'Orders'

    if touch is not None:
        s += '-at-touch'
    return s


def features(df, touchval, 
    ordtypeoptions = [None, 'New', 'Cancelled', 'Executed'], 
    sideoptions = [None, 'Buy', 'Sell']
    ):
    '''return dict of new order features
    Args:
        df: spark dataframe object
        touchval: tuple of (bestbid, bestask)
    '''
    filargs = [{'ordtype': ordtype, 'side': side, 'touch': touch}
        for ordtype in ordtypeoptions
        for side in sideoptions
        for touch in [None, touchval]]

    out = {}
    for arg in filargs:
        out[namer(**arg)] = aggtype(df, filstr(**arg))
    return out