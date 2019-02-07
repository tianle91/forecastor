import pyspark as spark
from  pyspark.sql.functions import abs
import numpy as np
import pandas as pd


def filterstr(ordtype='New', side='All', touch=None):
    '''return filter condition string'''
    s = ''

    # ordtype
    if ordtype == 'New':
        s += 'book_change > 0 '
    elif ordtype == 'Cancelled':
        s += '''reason == 'Cancelled' '''
    elif ordtype == 'Executed':
        s += '''reason == 'Filled' OR reason == 'Partial Fill' '''
    else:
        raise ValueError('invalid orddtype argument: %s' % ordtype)
    
    # touch
    if touch is not None:
        bestbid, bestask = touch
        s += '''AND price BETWEEN %s AND %s ''' % (bestbid, bestask)

    # side
    if side in ['Buy', 'Sell']:
        s += '''AND side == '%s' ''' % (side)
    elif side == 'All':
        pass
    else:
        raise ValueError('invalid side argument: %s' % side)
    
    return s


class Orders(object):

    def __init__(self, sparkdf):
        self.sparkdf = sparkdf.cache()

    def bkchange(self):
        '''return pd.dataframe of orderbook changes'''
        df = self.sparkdf.groupBy('side', 'price').agg({'book_change': 'sum'})
        return df.toPandas()

    def counttype(self, filstr, ctype='Number'):
        '''return count or sum of self.df filtered by filstr

        Args:
            filstr: str to pass to df.filter()
            ctype: str in ['Number', 'Volume', 'AvgVolume']
        '''
        df = self.sparkdf.filter(filstr)
        numorders = df.count()
        if ctype == 'Number':
            return numorders
        else: 
            df = df.withColumn('absq', abs(df.book_change))
            sumq = df.agg({'absq': 'sum'}).collect()[0]
            sumq = sumq['sum(absq)']
            if ctype == 'Volume':
                return sumq
            elif ctype == 'AvgVolume':
                if numorders == 0:
                    return 0
                else:
                    return sumq/numorders

    def features(self, touchval):
        '''return dict of new order features

        Args:
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
            s += '_of_' + ordtype
            s += '_' + side + '_Orders'
            if touch is not None:
                s += '_at_touch'
            return s

        out = {}
        for arg in args:
            filstr = filterstr(arg['ordtype'], arg['side'], arg['touch'])
            out[namer(**arg)] = self.counttype(filstr, arg['ctype'])

        return out