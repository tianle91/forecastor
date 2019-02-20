import numpy as np
from keras.utils import Sequence 

class TradeConditionSequence(Sequence):

    def __init__(self, fnameprefix, shuffletime=False):
        '''initialize trade conditions sequence generator
        '''
        # features are stored as dicts
        # in {trades, orders, book}, keyed by date
        trades = json.load(gzip.open(fnameprefix+'_trades.json.gz', 'rb'))
        orders = json.load(gzip.open(fnameprefix+'_orders.json.gz', 'rb'))
        # load into dict keyed by time
        self.d = {}
        for dt in trades.keys():
            self.d[dt] = {
                'trades': trades[dt], 
                'book': orders[dt]['book'],
                'orders': orders[dt]['orders']
            }
        # sort time axis keys
        self.taxis = list(trades.keys())
        self.taxis.sort()
        # if shuffletime, then get a randomly permuted index array
        if shuffletime:
            self.shufflemap = np.random.permutation(len(self.taxis))

    def getitem_bydt(self, dt):
        '''return all features at single time dt
        '''
        out = self.d[dt]
        # flatten all in out to numpy matrix
        outm = list(out.values())
        outm = sum([list(d.values()) for d in outm], [])
        return np.array(outm)

    def on_epoch_end(self):
        '''if shuffletime, update self.shufflemap
        '''
        if shuffletime:
            self.shufflemap = np.random.permutation(len(self.taxis))

    def toArray(self):
        '''return numpy array of data arranged by time in axis=0
        '''
        outresl = [self.getitem_bydt(dt) for dt in self.taxis]
        outresl = [l.reshape((1,) + l.shape) for l in outresl]
        return np.concatenate(outresl, axis=0)

    def __len__(self):
        return len(self.taxis)

    def __getitem__(self, idx):
        idxread = idx
        if self.shuffletime:
            idxread = self.shufflemap[idx]
        return self.getitem_bydt(self.taxis[idx])