import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def filbool(df, ordtype=None, side=None, touch=None):
    '''return filter condition boolean'''
    b = None

    if ordtype is None:
        pass
    elif ordtype == 'New':
        b = df['book_change'] > 0
    elif ordtype == 'Cancelled':
        b = df['reason'] == 'Cancelled'
    elif ordtype == 'Executed':
        b = df['reason'].isin(['Filled', 'Partial Fill'])
    else:
        raise ValueError('invalid orddtype argument: %s' % ordtype)
    
    if touch is not None:
        bestbid, bestask = touch
        b = np.logical_and(b, df['price'].between(bestbid, bestask))

    if side is None:
        pass
    elif side in ['Buy', 'Sell']:
        b = np.logical_and(b, df['side'] == side)
    else:
        raise ValueError('invalid side argument: %s' % side)
    
    return b


def aggtype(df, filbool):
    '''return count/sum of df.filter(filstr)'''
    if b is not None:
        df = df.loc[filbool,:]
    nrow = len(df)
    sumq = df['book_change'].abs().sum()
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
    ordtypeoptions = ['New', 'Cancelled', 'Executed'], 
    sideoptions = ['Buy', 'Sell']
    ):
    '''return dict of new order features
    Args:
        df: spark dataframe object
        touchval: tuple of (bestbid, bestask)
    '''
    if type(df) is not pd.DataFrame:
        raise TypeError('df is not pd.DataFrame!')

    filargs = [
        {'ordtype': ordtype, 'side': side, 'touch': touch}
        for ordtype in [None] + ordtypeoptions
        for side in [None] + sideoptions
        for touch in [None, touchval]
    ]

    out = {}
    for arg in filargs:
        out[namer(**arg)] = aggtype(df, filbool(df, **arg))
    return out