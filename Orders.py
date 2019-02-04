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
            ordtype: 'new' or 'notnew'
            side: 'Buy' or 'Sell' or 'All'
            counttype: 'orders' or 'quantity' or 'qperorder'
            touch: None or (bestbuy, bestask)
        '''
        s = ''

        # ordtype
        if ordtype == 'new':
            s += 'book_change > 0'
        elif ordtype == 'notnew':
            s += 'book_change < 0'
        else:
            raise ValueError('invalid orddtype argument: %s' % ordtype)
        
        # touch
        if touch is not None:
            s += 'AND price BEWEEN %s %s' % touch

        # side
        if side in ['Buy', 'Sell']:
            s += 'AND side == %s' % side
        elif side == 'All':
            pass
        else:
            raise ValueError('invalid side argument: %s' % side)
        
        # do the subset
        df = self.df.filter(s)

        # IPR !!!
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
                for ordtype in ['new', 'notnew']
                for side in ['Buy', 'Sell', 'All']
                for counttype in ['orders', 'quantity', 'qperorder']
                for touch in [None, touchval]]

        out = {}
        for s in argl:
            out[str(s)] = self.count_stuff(**s)

        return out