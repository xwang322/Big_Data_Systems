import os
import sys
import numpy as np
import mxnet as mx
import logging
import tensorflow as tf

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

data = mx.symbol.Variable('data')
fc1  = mx.symbol.FullyConnected(data = data, num_hidden=1)
act1 = mx.symbol.Activation(data = fc1, act_type='sigmoid') 
mlp  = mx.symbol.SoftmaxOutput(data = act1, name = 'softmax')

datapath_train = "criteo_25.mx"
datapath_test = "criteo_5.mx"
dataset_train = mx.nd.load(datapath_train)
dataset_test = mx.nd.load(datapath_test)

batch_size = 1
devices = [mx.cpu(0)]
train = mx.io.NDArrayIter(data=dataset_train['features'], label=dataset_train['labels'], batch_size=batch_size, shuffle=False)
test = mx.io.NDArrayIter(data=dataset_test['features'], label=dataset_test['labels'], batch_size=batch_size, shuffle=False)
print train.provide_label
model = mx.model.FeedForward(ctx=devices, symbol=mlp, num_epoch=2, learning_rate=0.1)
model.fit(X=train, eval_metric=['accuracy'], batch_end_callback=mx.callback.Speedometer(1,10)) 

print "score"
print 'Accuracy:', model.score(test)*100, '%'




