import os
import sys
import gzip
import pickle

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.patches as patches

from tensorflow.keras.layers import Lambda, Input, Dense
from tensorflow.keras.models import Model
from tensorflow.keras.losses import mse
from tensorflow.keras import backend as K
from tensorflow.keras.optimizers import Adam

from sklearn import preprocessing
from sklearn.manifold import TSNE
from sklearn.mixture import GaussianMixture
from sklearn.model_selection import GridSearchCV

from TXLoader import TXLoader


## -----------------------------------------------------------------------------
## setup, use %pyspark
## -----------------------------------------------------------------------------
symbol = 'TD'
datelenname = '1mo'
timelenname = '1h'
tsunit = 'MINUTE'
dotraining = True

txlobj = TXLoader(datelenname=datelenname, timelenname=timelenname, tsunit=tsunit, symbol=symbol)
xm = txlobj.getxm()
xnames = txlobj.getcovnames()

ndays, ntime, ncov = xm.shape
ndaystest = 1
print ('xm.shape:', xm.shape)

# scaler
scaler = preprocessing.QuantileTransformer(random_state=0)
scaler.fit(np.concatenate([xm[i, :, :] for i in range(ndays)], axis=0))


## -----------------------------------------------------------------------------
## vizualization
## -----------------------------------------------------------------------------
i = 0
for xnametmp in xnames:
    print ('i:%d\t%s' % (i, xnametmp))
    i += 1

covtypeindices = (48, 70, 84)
covtypeindiceslen = np.diff((0,)+covtypeindices)
print (covtypeindiceslen)
# 48, 22, 14

def viz_covariates(xms):
    ntime, ncov = xms.shape
    fig, ax = plt.subplots(1, figsize=(ntime/10, ncov/10))
    ax.imshow(np.transpose(xms), aspect=ntime/ncov)
    
    # box for orders features
    ax.add_patch(patches.Rectangle((-.5, -.5), ntime+.5, covtypeindiceslen[0], 
        linewidth=2, edgecolor=(1,0,0,1), facecolor='none'))
    # box for book features
    ax.add_patch(patches.Rectangle((-.5, covtypeindices[0]-.5), ntime+.5, covtypeindiceslen[1], 
        linewidth=2, edgecolor=(1,0,0,1), facecolor='none'))
    # box for trades features
    ax.add_patch(patches.Rectangle((-.5, covtypeindices[1]-.5), ntime+.5, covtypeindiceslen[2], 
        linewidth=2, edgecolor=(1,0,0,1), facecolor='none'))
    # box for additional features
    ax.add_patch(patches.Rectangle((-.5, covtypeindices[2]-.5), ntime+.5, 99, 
        linewidth=2, edgecolor=(1,0,0,1), facecolor='none'))

    ax.set_xlabel('time')
    ax.set_ylabel('covariates')
    plt.show()

# visualize transformed covariates
for i in range(1):
    viz_covariates(scaler.transform(xm[i, :, :]))


## -----------------------------------------------------------------------------
## model
## -----------------------------------------------------------------------------
exec(open('rep_vae.py').read())

# find weights if dotraining else load weights
# datelenname=datelenname, timelenname=timelenname, tsunit=tsunit, symbol=symbol
weightsfname = 'vae_dl:%s_tl:%s_tsunit:%s_SYM:%s.h5' % (datelenname, timelenname, tsunit, symbol)
if dotraining:
    # train on all stocks
    xtrain = np.concatenate([scaler.transform(xm[i, :, :]) for i in range(ndays-ndaystest)], axis=0)
    xtrain[np.isnan(xtrain)] = 0
    print (xtrain.shape)
    xtest = np.concatenate([scaler.transform(xm[i, :, :]) for i in range(ndays-ndaystest, ndays)], axis=0)
    xtest[np.isnan(xtest)] = 0
    print (xtest.shape)
    history = vae.fit(xtrain, epochs=5000, batch_size=256, verbose=2, validation_data=(xtest, None))
    vae.save_weights(weightsfname)
else:
    vae.load_weights(weightsfname)

# plot training history if dotraining
if dotraining:
    plt.figure(figsize=(6, 6))
    plt.plot(history.history['loss'], color='green', label='training_loss')
    plt.plot(history.history['val_loss'], color='red', label='validation_loss')
    plt.xlabel('iters')
    plt.ylabel('loss')
    plt.legend()
    plt.show()


## -----------------------------------------------------------------------------
## latent space
## -----------------------------------------------------------------------------
xall = np.concatenate([scaler.transform(xm[i, :, :]) for i in range(ndays)], axis=0)
xall[np.isnan(xall)] = 0
zmeanall = encoder.predict(xall)[0]

# fit GMM
param_grid = {'n_components': np.arange(1, latent_dim)}
cv = GridSearchCV(GaussianMixture(random_state=0, covariance_type='full', param_grid=param_grid, cv=10)
cv.fit(zmeanall)
gmmbest = cv.best_estimator_
print (cv.best_params_)
gmmncomps = cv.best_params_['n_components']

# fit TSNE
zmeantsneall = TSNE(n_components=2).fit_transform(zmeanall)
zmeanlabel = gmmbest.predict(zmeanall)
print (np.unique(zmeanlabel, return_counts=True))


## -----------------------------------------------------------------------------
## visualize TSNE mapping and GMM clusters
## -----------------------------------------------------------------------------
plt.figure(figsize = (6, 6))

for classdex in range(gmmncomps):
    isclass = zmeanlabel == classdex
    plt.scatter(zmeantsneall[isclass, 0], zmeantsneall[isclass, 1], s=2, label='class: %s' % classdex)

plt.title('TSNE(zmean)')
plt.legend()
plt.show()


## -----------------------------------------------------------------------------
## cluster histograms
## -----------------------------------------------------------------------------
nquantiles = 4
q = np.linspace(0, 1, num=nquantiles+1)
print (q)
gmmvizm = np.zeros((ncov, gmmncomps, nquantiles))

for covdex in range(ncov):
    # quantiles for covariate: covdex
    bins = q
    print ('covdex: %s\n%s' % (covdex, bins))
    for classdex in range(gmmncomps):
        # use marginal quantiles as bins 
        xalltemp = xall[zmeanlabel == classdex, covdex]
        bincounts = plt.hist(xalltemp, bins=bins)[0]/len(xalltemp)
        gmmvizm[covdex, classdex, :] = bincounts

# plot cluster histograms
fig, ax = plt.subplots(ncols=gmmncomps, sharey=True, figsize=((gmmncomps/4)*6, 6), dpi=300)
ax[0].set_ylabel('covariates')

for gmmdex in range(gmmncomps):
    ax[gmmdex].imshow(gmmvizm[:, gmmdex, :], aspect=(gmmncomps*nquantiles)/ncov)
    # box for orders features
    ax[gmmdex].add_patch(patches.Rectangle((-.5, -.5), nquantiles+.5, covtypeindiceslen[0], 
        linewidth=2, edgecolor=(1,0,0,1), facecolor='none'))
    # box for book features
    ax[gmmdex].add_patch(patches.Rectangle((-.5, covtypeindices[0]-.5), nquantiles+.5, covtypeindiceslen[1], 
        linewidth=2, edgecolor=(1,0,0,1), facecolor='none'))
    # box for trades features
    ax[gmmdex].add_patch(patches.Rectangle((-.5, covtypeindices[1]-.5), nquantiles+.5, covtypeindiceslen[2], 
        linewidth=2, edgecolor=(1,0,0,1), facecolor='none'))
    # box for additional features
    ax[gmmdex].add_patch(patches.Rectangle((-.5, covtypeindices[2]-.5), nquantiles+.5, 99, 
        linewidth=2, edgecolor=(1,0,0,1), facecolor='none'))

    ax[gmmdex].set_title('class: %s' % (gmmdex))
    ax[gmmdex].set_xlabel('quantiles')
    ax[gmmdex].set_xticks(ticks=[])
plt.show()