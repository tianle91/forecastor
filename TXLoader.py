import gzip
import pickle
import numpy as np


def flattendic_orders(d):
    out = None
    if d['orders'] is not None:
        out = [d['orders'][k] for k in d['orders']]
        out += [d['book'][k] if not (k == 'prices') else None for k in d['book']]
        out += [d['book']['prices'][k] for k in d['book']['prices']]
    return out
    
def flattendic_trades(d):
    out = None
    if d is not None:
        out = [d[k] for k in d]
    return out


class TXLoader(object):

    def __init__(self, jobname, symbol):
        self.orders = pickle.load(gzip.open('%s_SYM:%s_orders.pickle.gz' % (jobname, symbol), 'rb'))
        self.trades = pickle.load(gzip.open('%s_SYM:%s_trades.pickle.gz' % (jobname, symbol), 'rb'))


    def getxm(self, byday=False):

        def toflatlist(flatords, flattrades):
            if flatords is not None and flattrades is not None:
                return flatords + flattrades

        if not byday:
            #flatten resd into flat time index for just minute-level
            resdorders = {k2: self.orders[k1][k2] 
                for k1 in self.orders 
                for k2 in self.orders[k1]}
            resdtrades = {k2: self.trades[k1][k2] 
                for k1 in self.trades 
                for k2 in self.trades[k1]}

            #get all ordered time indices 
            alltimes = list(resdorders.keys())
            alltimes.sort()

            resl = [toflatlist(flattendic_orders(resdorders[dt]), flattendic_trades(resdtrades[dt])) for dt in alltimes]
            return np.array([l for l in resl if l is not None])
        else:
            alldays = list(self.orders.keys())
            alldays.sort()

            def worker(resdordersday, resdtradesday):
                alltimes = list(resdordersday.keys())
                resl = [toflatlist(flattendic_orders(resdordersday[dt]), flattendic_trades(resdtradesday[dt])) for dt in alltimes]
                return [l for l in resl if l is not None]

            return np.array([worker(self.orders[dt], self.trades[dt]) for dt in alldays])
