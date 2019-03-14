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


# ------------------------------------------------------------------------------
# define VAE model
# https://github.com/keras-team/keras/blob/master/examples/variational_autoencoder.py
# ------------------------------------------------------------------------------
input_shape = (ncov, )
interm_dim = np.power(2, max(0, int(np.log(ncov)/np.log(2))))
latent_dim = np.power(2, max(0, int(np.log(interm_dim)/np.log(2))-1))
print ((ncov, interm_dim, latent_dim))


## -----------------------------------------------------------------------------
## build encoder model. 
## inputs -> x -> z
## -----------------------------------------------------------------------------
inputs = Input(shape=input_shape, name='encoder_input')
x = Dense(interm_dim, activation='tanh')(inputs)
z = Dense(latent_dim, name='encz')(x)

# instantiate encoder model
encoder = Model(inputs, z, name='encoder')
encoder.summary()


## -----------------------------------------------------------------------------
## build decoder model
## [z -> x -> ouputs]
## -----------------------------------------------------------------------------
latent_inputs = Input(shape=(latent_dim,), name='decoder_input')
x = Dense(interm_dim, activation='tanh')(latent_inputs)
outputs = Dense(ncov, activation='tanh')(x)

# instantiate decoder model
decoder = Model(latent_inputs, outputs, name='decoder')
decoder.summary()


## -----------------------------------------------------------------------------
## instantiate VAE model
## -----------------------------------------------------------------------------
outputs = decoder(encoder(inputs))
ae = Model(inputs, outputs, name='ae')

# AE loss = mse_loss
# losses defined in terms of [inputs, outputs], which point to keras layers
reconstruction_loss = ncov*mse(inputs, outputs)

ae_loss = K.mean(reconstruction_loss)
ae.add_loss(ae_loss)
ae.compile(optimizer=Adam(lr=0.00001))
ae.summary()