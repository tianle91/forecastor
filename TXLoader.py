import os
import gzip
import time
import pickle
import numpy as np
import pandas as pd


def unpackcovdict(d):
    covnames, values = zip(*[(k, v) for k, v in d.items()])
    return covnames, values


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
        dt0 = list(self.orders[date0].keys())[0]
        return self.unpack_singledt(date0, dt0)[0]


    def unpack_singledt(self, dtstr, dt, verbose=0):
        odtmp = self.orders[dtstr][dt]
        covnamesodbk, valuesodbk = unpackcovdict(odtmp['book'])
        covnamesodod, valuesodod = unpackcovdict(odtmp['orders'])
        txtmp = self.trades[dtstr][dt]
        covnamestx, valuestx = unpackcovdict(txtmp)
        covnamesout = list(covnamesodbk) + list(covnamesodod) + list(covnamestx)
        valuesout = list(valuesodbk) + list(valuesodod) + list(valuestx)

        if verbose > 0:
            print ('dtstr:%s, dt:%s len(covnamesout):%s, len(valuesout):%s' % (dtstr, dt, len(covnamesout), len(valuesout)))
        return covnamesout, valuesout


    def unpackvalues_singleday(self, dtstr, verbose=0):
        alltimesinday = list(self.orders[dtstr].keys())
        alltimesinday.sort()
        resl = [self.unpack_singledt(dtstr, dt, verbose=verbose-1)[1] for dt in alltimesinday]
        if verbose > 0:
            print ('dtstr: %s, len(resl): %s' % (dtstr, len(resl)))
            if verbose > 1:
                print ('resl[0]:\n', resl[0])
        return resl


    def getxm(self):

        if self.verbose > 0:
        	t0 = time.time()
        	print ('getting xm...')

        alldays = list(self.orders.keys())
        alldays.sort()

        if self.verbose > 1:
            print ('alldays:', alldays)

        resl = [self.unpackvalues_singleday(dtstr, verbose=self.verbose-1) for dtstr in alldays]
        out = np.array(resl)

        if self.verbose > 0:
            print ('done in: %.2f, out.shape: %s' % (time.time()-t0, out.shape))

        return out.astype(float)