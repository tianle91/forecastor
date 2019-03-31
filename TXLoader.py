import gzip
import pickle
import numpy as np


def flattendic_orders(d):
    out = None
    if d['orders'] is not None:
        out = [d['orders'][k] for k in d['orders']]
        out += [d['book'][k] for k in d['book']]
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

def toflatlist(flatords, flattrades):
    if flatords is not None and flattrades is not None:
        return flatords + flattrades


class TXLoader(object):

    def __init__(self, datelenname, timelenname, tsunit, symbol, verbose=0):
        dates = pickle.load(open(os.getcwd() + '/data/dl:%s_tl:%s_ts:%s_SYM:%s_dates.pickle' % (datelenname, timelenname, tsunit, symbol), 'rb'))
        self.dates = dates
        self.orders = {}
        self.trades = {}
        for dt in dates:
            fname = os.getcwd() + '/data/tl:%s_ts:%s_dt:%s_SYM:%s_orders.pickle.gz' % (timelenname, tsunit, dtstr, symbol)
            self.orders[dt] = pickle.load(gzip.open(fname, 'rb'))
            fname = os.getcwd() + '/data/tl:%s_ts:%s_dt:%s_SYM:%s_trades.pickle.gz' % (timelenname, tsunit, dtstr, symbol)
            self.trades[dt] = pickle.load(gzip.open(fname, 'rb'))
        self.verbose = verbose


    def getcovnames(self):
        date0 = self.dates[0]
        time0 = list(self.orders[date0].keys())[0]
        out = flattencovname_orders(self.orders[date0][time0])
        out += flattencovname_trades(self.trades[date0][time0])
        return out


    def getxm(self, nanis=0):

        alldays = list(self.orders.keys())
        alldays.sort()

        def workerinday(dt, dtinday):
            flatords = self.orders[dt][dtinday]
            flattrades = self.trades[dt][dtinday]
            return toflatlist(flattendic_orders(flatords), flattendic_trades(flattrades))

        def worker(dt):
            alltimesinday = list(self.orders[dt].keys())
            alltimesinday.sort()
            resl = [workerinday(dt, dtinday) for dtinday in alltimesinday]
            return [l for l in resl if l is not None]

        resll = [worker(dt) for dt in alldays]
        resll = [l for l in resll if len(l) > 0]
        if self.verbose > 0:
            print ('byday: %s:\nlen(resll): %s' % (byday, len(resll)))
            for resl in resll:
                print ('len(resl):', len(resl))

        return np.array(resll)