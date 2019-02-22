import os
import sys
import gzip
import pickle

import numpy as np
import matplotlib.pyplot as plt
from sklearn import preprocessing

#from TXLoader import TXLoader
exec(open('TXLoader.py').read())


# load extracted features
xm = TXLoader(jobname = 'short', symbol = 'TD').getxm(byday=True)
ndays, ntime, ncov = xm.shape
print ('xm.shape:', xm.shape)
print (xm[0, ...])


# scaling stuff
scaler = preprocessing.MinMaxScaler((-1, 1))
scaler.fit(xm[0, :, :])
xmnormd = np.concatenate([scaler.transform(xm[ddex, :, :]).reshape((1,)+xm.shape[1:]) for ddex in range(ndays)], axis=0)
xmnormd[np.isnan(xmnormd)] = 0

for ddex in range(ndays):
    plt.imshow(xmnormd[ddex, :, :])
    plt.title('day:%s' % (ddex))
    plt.show()