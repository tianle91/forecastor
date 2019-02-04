# import pyspark as spark
import numpy as np
import pandas as pd


def sqlfilstr(ordtype='new', side='Buy', touch=None):
    '''return string for df.filter(*)'''
    s = ''

    # ordertye == 'new'?
    if ordtype == 'new':
        s += 'book_change > 0'
    elif ordtype == 'notnew':
        s += 'book_change < 0'
    else:
        raise ValueError('invalid orddtype argument: %s' % ordtype)
    
    # side
    if side in ['Buy', 'Sell']:
        s += 'AND side == %s' % side
    elif side == 'All':
        pass
    else:
        raise ValueError('invalid side argument: %s' % side)
    
    # touch is None?
    if touch is not None:
        s += 'AND price BEWEEN %s AND %s' % touch

    return s


def countstuff(df, ordtype='new', side='Buy', touch=None, counttype='orders'):
    
    df = df.filter(sqlfilstr(ordtype=ordtype, side=side, touch=touch))
    pass


def features(df, touchval):
    '''return dict of features'''
    argl = [{'ordtype': ordtype, 'side': side, 'counttype': counttype, 'touch': touch}
            for ordtype in ['new', 'notnew']
            for side in ['Buy', 'Sell', 'All']
            for counttype in ['orders', 'quantity', 'qperorder']
            for touch in [None, touchval]]

    out = {}
    for s in argl:
        out[str(s)] = self.countstuff(**({'df': df} + s))

    return out