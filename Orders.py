# NO SPARK HERE, ONLY PANDAS
import numpy as np
import pandas as pd


def filbool(df, ordtype=None, side=None, touch=None):
    '''return boolean for filter condition'''
    b = np.repeat(True, len(df))

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
    '''return dict of Number, Volume of df.filter(filstr)'''
    df = df.loc[filbool, :]
    return {'Number': len(df), 'Volume': df['book_change'].abs().sum()}


def namer(ordtype, side, touch):
    '''return covariate name'''
    if ordtype is None:
        s = 'All'
    else:
        s = ordtype

    if side is not None:
        s += '-' + side
    s += '-Orders'

    if touch is not None:
        s += '-at-touch'
    return s


class Orders(object):

    def __init__(self, df, verbose=0):
        if type(df) is not pd.DataFrame:
            raise TypeError('df is not pd.DataFrame!')
        elif not {'price', 'book_change'}.issubset(df.columns):
            raise ValueError('df does not have price, book_change columns!')
        else:
            self.df = df.astype({'price': float, 'book_change': float})
        self.verbose = verbose


    def features(self, touchval):
        '''return dict of covariates of '''

        # types of orders to collect Count+Volume for
        # None represents no filter condition (i.e. All orders)
        filargs = [
            {'ordtype': ordtype, 'side': side, 'touch': touch}
            for ordtype in [None, 'New', 'Cancelled', 'Executed']
            for side in [None, 'Buy', 'Sell']
            for touch in [None, touchval]
        ]

        out = {}
        for arg in filargs:
        	# collect Count+Volume of df filtered by filargs
            out[namer(**arg)] = aggtype(self.df, filbool(df, **arg))
        if self.verbose > 0:
            print (out)
        return out