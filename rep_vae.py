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