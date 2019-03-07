import os
import sys
import gzip
import pickle

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.patches as patches

from sklearn import preprocessing

#from TXLoader import TXLoader
exec(open(os.getcwd() + '/TXLoader.py').read())

from tensorflow.keras.layers import Lambda, Input, Dense
from tensorflow.keras.models import Model
from tensorflow.keras.losses import mse
from tensorflow.keras import backend as K
from tensorflow.keras.optimizers import Adam
#from tensorflow.python.client import device_lib
#print(device_lib.list_local_devices())


# ------------------------------------------------------------------------------
# define VAE model
# ------------------------------------------------------------------------------
input_shape = (ncov, )
interm_dim = int(ncov/2)+1
latent_dim = int(interm_dim/2)+1
print ((ncov, interm_dim, latent_dim))

## -----------------------------------------------------------------------------
## build encoder model. 
## inputs -> x -> [z_mean, z_log_var]
## -----------------------------------------------------------------------------
inputs = Input(shape=input_shape, name='encoder_input')
x = Dense(interm_dim, activation='tanh')(inputs)
z_mean = Dense(latent_dim, name='z_mean')(x)
z_log_var = Dense(latent_dim, name='z_log_var')(x)

# use reparameterization trick to push the sampling out as input.
# gradients flow to [z_mean, z_log_var] through reparametrization.
# note that "output_shape" isn't necessary with the TensorFlow backend.
def sampling(args):
    z_mean, z_log_var = args
    batch = K.shape(z_mean)[0]
    dim = K.int_shape(z_mean)[1]
    # by default, random_normal has mean = 0 and std = 1.0
    epsilon = K.random_normal(shape=(batch, dim))
    return z_mean + K.exp(0.5 * z_log_var) * epsilon

# instantiate encoder model
z = Lambda(sampling, output_shape=(latent_dim,), name='z')([z_mean, z_log_var])
encoder = Model(inputs, [z_mean, z_log_var, z], name='encoder')
encoder.summary()

## -----------------------------------------------------------------------------
## build decoder model
## [z_sampled -> x -> ouputs]
## -----------------------------------------------------------------------------
latent_inputs = Input(shape=(latent_dim,), name='z_sampling')
x = Dense(interm_dim, activation='tanh')(latent_inputs)
outputs = Dense(ncov, activation='tanh')(x)

# instantiate decoder model
decoder = Model(latent_inputs, outputs, name='decoder')
decoder.summary()

## -----------------------------------------------------------------------------
## instantiate VAE model
## -----------------------------------------------------------------------------
outputs = decoder(encoder(inputs)[2])
vae = Model(inputs, outputs, name='vae')

# VAE loss = mse_loss or xent_loss + kl_loss
# losses defined in terms of [inputs, outputs], which point to keras layers
reconstruction_loss = ncov*mse(inputs, outputs)
kl_loss = 1 + z_log_var - K.square(z_mean) - K.exp(z_log_var)
kl_loss = K.sum(kl_loss, axis=-1)
kl_loss *= -0.5

vae_loss = K.mean(reconstruction_loss + kl_loss)
vae.add_loss(vae_loss)
vae.compile(optimizer=Adam(lr=0.00001))
vae.summary()


if __name__ == '__main__':
    
    symbol = 'TD'
    jobname = '1mo-1h'
    dotraining = True

    xm = TXLoader(jobname=jobname, symbol=symbol).getxm()
    xm[np.isnan(xm)] = 0
    nday, ntime, ncov = xm.shape
    print ('xm.shape:', xm.shape)

    # scale xm
    scaler = preprocessing.MinMaxScaler((-1, 1))
    scaler.fit(np.concatenate([xm[i, ...] for i in range(nday)], axis=0))
    xm = np.concatenate([scaler.transform(xm[i, ...]).reshape((1, ntime, ncov)) for i in range(nday)], axis=0)
    for i in range(5):
        viz_covariates(xm[i, :, :])

    xmflat = np.concatenate([xm[i, ...] for i in range(nday)], axis=0)
    ntest = 60
    xtrain, xtest = xmflat[:-ntest, :], xmflat[-ntest:, :]

    # find weights if dotraining else load weights
    weightsfname = 'vae_%s_SYM:%s.h5' % (jobname, symbol)
    if dotraining:
        history = vae.fit(xtrain, epochs=5000, batch_size=64, verbose=2, validation_data=(xtest, None))
        vae.save_weights(weightsfname)
    else:
        vae.load_weights(weightsfname)

    # plot training history if dotraining
    if dotraining:
        plt.plot(history.history['loss'], color='green', label='training_loss')
        plt.plot(history.history['val_loss'], color='red', label='validation_loss')
        plt.xlabel('iters')
        plt.ylabel('loss')
        plt.legend()
        plt.show()