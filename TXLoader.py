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

def flattencovname_orders(d):
    out = []
    if d['orders'] is not None:
        out = ['orders:' + k for k in d['orders']]
        out += ['book:' + k if not (k == 'prices') else None for k in d['book']]
        out += ['prices:' + k for k in d['book']['prices']]
    return out

def flattencovname_trades(d):
    out = []
    if d is not None:
        out = ['trades:' + k for k in d]
    return out


class TXLoader(object):

    def __init__(self, jobname, symbol, verbose=0):
        dates = pickle.load(open(os.getcwd() + '/data/%s_SYM:%s_dates.pickle' % (jobname, symbol), 'rb'))
        self.dates = dates
        self.orders = {}
        self.trades = {}
        for dt in dates:
            fname = os.getcwd() + '/data/%s_SYM:%s_dt:%s_orders.pickle.gz' % (jobname, symbol, dt)
            self.orders[dt] = pickle.load(gzip.open(fname, 'rb'))
            fname = os.getcwd() + '/data/%s_SYM:%s_dt:%s_trades.pickle.gz' % (jobname, symbol, dt)
            self.trades[dt] = pickle.load(gzip.open(fname, 'rb'))
        self.verbose = verbose


    def getcovnames(self):
        date0 = self.dates[0]
        time0 = list(self.orders[date0].keys())[0]
        out = flattencovname_orders(self.orders[date0][time0])
        out += flattencovname_trades(self.trades[date0][time0])
        return out


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

            def worker(dt):
                flatords = resdorders[dt]
                flattrades = resdtrades[dt]
                return toflatlist(flattendic_orders(flatords), flattendic_trades(flattrades))

            resl = [worker(dt) for dt in alltimes]
            resl = [l for l in resl if l is not None]
            if self.verbose > 0:
                print ('byday: %s has len(resl: %s' % (byday, len(resl)))

            return np.array(resl)

        else:
            alldays = list(self.orders.keys())
            alldays.sort()

            def workerinday(dt, dtinday):
                flatords = self.orders[dt][dtinday]
                flattrades = self.trades[dt][dtinday]
                return toflatlist(flattendic_orders(flatords), flattendic_trades(flattrades))

            def worker(dt):
                alltimesinday = list(self.orders[dt].keys())
                resl = [workerinday(dt, dtinday) for dtinday in alltimesinday]
                return [l for l in resl if l is not None]

            resll = [worker(dt) for dt in alldays]
            resll = [l for l in resll if len(l) > 0]
            if self.verbose > 0:
                print ('byday: %s:\nlen(resll): %s' % (byday, len(resll)))
                for resl in resll:
                    print ('len(resl):', len(resl))

            return np.array(resll)
