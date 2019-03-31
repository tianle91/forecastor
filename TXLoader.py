import os
import gzip
import pickle
import numpy as np
import pandas as pd


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
        out += ['book:' for k in d['book']]
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
        for dtstr in dates:
            fname = os.getcwd() + '/data/tl:%s_ts:%s_dt:%s_SYM:%s_orders.pickle.gz' % (timelenname, tsunit, dtstr, symbol)
            self.orders[dtstr] = pickle.load(gzip.open(fname, 'rb'))
            fname = os.getcwd() + '/data/tl:%s_ts:%s_dt:%s_SYM:%s_trades.pickle.gz' % (timelenname, tsunit, dtstr, symbol)
            self.trades[dtstr] = pickle.load(gzip.open(fname, 'rb'))
        self.verbose = verbose


    def getcovnames(self):
        date0 = self.dates[0]
        time0 = list(self.orders[date0].keys())[0]
        out = flattencovname_orders(self.orders[date0][time0])
        out += flattencovname_trades(self.trades[date0][time0])
        return out


    def getxm(self, nanis=0):

        alldays = list(self.orders.keys())
        alldays = [pd.to_datetime(dtstr) for dtstr in alldays]
        alldays.sort()

        def workerinday(dtstr, dtinday):
            flatords = self.orders[dtstr][dtinday]
            flattrades = self.trades[dtstr][dtinday]
            return toflatlist(flattendic_orders(flatords), flattendic_trades(flattrades))

        def worker(dtstr):
            alltimesinday = list(self.orders[dtstr].keys())
            alltimesinday.sort()
            resl = [workerinday(dtstr, dtinday) for dtinday in alltimesinday]
            return [l for l in resl if l is not None]

        resll = [worker(dt.strftime('%Y-%m-%d')) for dt in alldays]
        resll = [l for l in resll if len(l) > 0]
        if self.verbose > 0:
            print ('byday: %s:\nlen(resll): %s' % (byday, len(resll)))
            for resl in resll:
                print ('len(resl):', len(resl))

        return np.array(resll)