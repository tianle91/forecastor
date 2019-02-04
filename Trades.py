# import pyspark as spark
import numpy as np
import pandas as pd


class Trades(object):

    def __init__(self, df):
        self.df = df
    

    def features(self):
        '''return dict of features'''
        tradesdf = self.pandas
        prx = tradesdf['price']
        qty = tradesdf['quantity']
        
        mean = 0
        stdev = 0
        tradeq = 0
        tradecount = 0
        qtypertrade = 0
        
        if len(prx) > 0:
            mean =  np.average(prx, weights=qty)
            stdev = np.sqrt(np.average(np.power(prx, 2), weights=qty) - mean**2)
            tradeq = np.sum(qty)
            tradecount = len(prx)
            qtypertrade = tradeq/tradecount
            
        return {'mean': mean, 'stdev': stdev, 'traded_count': tradecount, 'traded_qty': tradeq, 'meanq_pertrade': qtypertrade}