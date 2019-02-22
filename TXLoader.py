import gzip
import pickle
import numpy as np


def flattendic_orders(d):
    out = None
    if d['orders'] is not None:
        out = [d['orders'][k]['Volume'] for k in d['orders']]
        out += [d['orders'][k]['Number'] for k in d['orders']]
        out += [d['book'][k] if not (k == 'prices') else None for k in d['book']]
        out += [d['book']['prices'][k] for k in d['book']['prices']]
    return out
    
def flattendic_trades(d):
    out = None
    if d is not None:
        out = [d['gpbyprice'][k] for k in d['gpbyprice']]
    return out


class TXLoader(object):

    def __init__(self, jobname, symbol):

        resdorders = pickle.load(gzip.open('%s_SYM:%s_orders.pickle.gz' % (jobname, symbol), 'rb'))
        resdtrades = pickle.load(gzip.open('%s_SYM:%s_trades.pickle.gz' % (jobname, symbol), 'rb'))
        #flatten resd into single time index
        self.resdorders = {k2: resdorders[k1][k2] for k1 in resdorders for k2 in resdorders[k1]}
        self.resdtrades = {k2: resdtrades[k1][k2] for k1 in resdtrades for k2 in resdtrades[k1]}
        #get all ordered time indices 
        self.alltimes = list(resdorders.keys())
        self.alltimes.sort()


    def getxm(self):

        def tonumpy(flatords, flattrades):
            if flatords is not None and flattrades is not None:
                return flatords + flattrades

        # covariates = [tonumpy(flattendic_orders(self.resdorders[dt]), flattendic_trades(self.resdtrades[dt])) for dt in self.alltimes]
        covariates = [l for l in covariates if l is not None]
        return np.array(covariates)