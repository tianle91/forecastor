import os
import gzip
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
        return unpack_singledt(date0, dt0)[0]


    def unpack_singledt(self, dtstr, dt):
        odtmp = self.orders[dtstr][dt]
        covnamesodbk, valuesodbk = unpackcovdict(odtmp['book'])
        covnamesodod, valuesodod = unpackcovdict(odtmp['orders'])
        txtmp = self.trades[dtstr][dt]
        covnamestx, valuestx = unpackcovdict(txtmp)
        covnamesout = list(covnamesodbk) + list(covnamesodod) + list(covnamestx)
        valuesout = list(valuesodbk) + list(valuesodod) + list(valuestx)

        if self.verbose > 3:
            print ('dtstr:%s, dt:%s len(covnamesout):%s, len(valuesout):%s' % (dtstr, dt, len(covnamesout), len(valuesout)))
        return covnamesout, valuesout


    def unpackvalues_singleday(self, dtstr):
        alltimesinday = list(self.orders[dtstr].keys())
        alltimesinday.sort()
        resl = [self.unpack_singledt(dtstr, dt)[1] for dt in alltimesinday]
        if self.verbose > 1:
            print ('dtstr: %s, len(resl): %s', (dtstr, len(resl)))
            if self.verbose > 2:
                print ('resl[0]:\n', resl[0])
        return resl


    def getxm(self):

        alldays = list(self.orders.keys())
        alldays.sort()
        if self.verbose > 0:
            print ('alldays:', alldays)

        resl = [self.unpack_singleday(dtstr) for dtstr in alldays]
        if self.verbose > 0:
            print ('len(resl): %s' % (len(resl)))
            for resltemp in resl:
                print ('len(resltemp):', len(resltemp))

        out = np.array(resl)
