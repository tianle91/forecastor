# import pyspark as spark
import numpy as np
import pandas as pd


class Orders(object):

    def __init__(self, df):
        self.df = df


    def count_stuff(self, ordtype='new', side='Buy', counttype='orders', touch=None):
        '''return count of counttype in order table
        Arguments:
            ordersdf: table of orders
            ordtype: 'new' or 'cancelled'
            side: 'Buy' or 'Sell' or 'All'
            counttype: 'orders' or 'quantity' or 'qperorder'
            touch: None or (bestbuy, bestask)
        '''
        ordersdf = self.pandas
        subsetbool = np.repeat(True, len(ordersdf))
    
        # ordtype
        if ordtype == 'new':
            subsetbool = ordersdf['book_change'] > 0
        elif ordtype == 'cancelled':
            subsetbool = ordersdf['book_change'] < 0
        else:
            raise ValueError('invalid orddtype argument: %s' % ordtype)
        
        # touch
        if touch is not None:
            bestbuy, bestask = touch
            isattouch = np.logical_or(ordersdf['price'] >= bestbuy, ordersdf['price'] <= bestask)
            subsetbool = np.logical_and(subsetbool, isattouch)

        # side
        if side in ['Buy', 'Sell']:
            subsetbool = np.logical_and(subsetbool, ordersdf['side'] == side)
        elif side == 'All':
            pass
        else:
            raise ValueError('invalid side argument: %s' % side)
        
        # do the subset
        df = ordersdf.loc[subsetbool, :]
        
        # counttype
        if counttype == 'orders':
            return len(df)
        elif counttype == 'quantity':
            return np.sum(np.abs(df['book_change']))
        elif counttype == 'qperorder':
            return np.sum(np.abs(df['book_change'])) / len(df)
        else:
            raise ValueError('invalid counttype argument: %s' % counttype)
    

    def features(self, touchval):
        '''return dict of features'''
        argl = [{'ordtype': ordtype, 'side': side, 'counttype': counttype, 'touch': touch}
                for ordtype in ['new', 'cancelled']
                for side in ['Buy', 'Sell', 'All']
                for counttype in ['orders', 'quantity', 'qperorder']
                for touch in [None, touchval]]

        out = {}
        for s in argl:
            out[str(s)] = self.count_stuff(**s)

        return out