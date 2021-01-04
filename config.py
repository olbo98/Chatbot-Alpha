from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import torch
from torch.jit import script, trace
import torch.nn as nn
from torch import optim
import torch.nn.functional as F
import csv
import random
import os
import unicodedata
import codecs
from io import open
import itertools
import math

# Train or/and evaluate
train = False
evaluate = True

##
## Corpus settings
##
corpus_name = "test"
filename = "sentence_pairs.txt"
MAX_LENGTH = 10 # Maximum sentence length to be included in sentence pairs dict.
MIN_COUNT = 3 # Minimum appearances in vocab else deleted

##
## Model settings
##
model_name = 'cb_model'
attn_model = 'dot'
#attn_model = 'general'
#attn_model = 'concat'
hidden_size = 500
encoder_n_layers = 2
decoder_n_layers = 2
dropout = 0.1
batch_size = 64

##
## Training settings
##
save_dir = os.path.join("data", "save") # Where to save trained model
checkpoint_iter = 4000

pretrained = True # Checkpoint to load from; set to None if starting from scratch
gputocpu = False # Is pretrained on gpu and going to be loaded on cpu
if not pretrained:
    loadFilename = None
else:
    loadFilename = os.path.join(save_dir, model_name, corpus_name,
                               '{}-{}_{}'.format(encoder_n_layers, decoder_n_layers, hidden_size),
                               '{}_checkpoint.tar'.format(checkpoint_iter))

# Configure training/optimization
clip = 50.0
teacher_forcing_ratio = 1.0
learning_rate = 0.0001
decoder_learning_ratio = 5.0
n_iteration = 4000
print_every = 1
save_every = 500

# Use cuda if available
USE_CUDA = torch.cuda.is_available()
device = torch.device("cuda" if USE_CUDA and not gputocpu else "cpu")
