import os
import sys
import numpy as np
import mxnet as mx
import logging
import tensorflow as tf
import gc

num_features = 33762578
g = tf.Graph()
with g.as_default():

    filename_queue = tf.train.string_input_producer([
        "/home/ubuntu/criteo-tfr/tfrecords00",
    ], num_epochs=None)
    reader = tf.TFRecordReader()
    _, serialized_example = reader.read(filename_queue)
    features = tf.parse_single_example(serialized_example,
                                       features={
                                        'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                        'index' : tf.VarLenFeature(dtype=tf.int64),
                                        'value' : tf.VarLenFeature(dtype=tf.float32),})
    label = features['label']
    index = features['index']
    value = features['value']
    dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index), [num_features,], tf.sparse_tensor_to_dense(value))
    sess = tf.Session()
    sess.run(tf.initialize_all_variables())
    tf.train.start_queue_runners(sess=sess)

    num_train_points = 25
    num_test_points = 5 
    dataset_train = np.ndarray(shape=(num_train_points,num_features),dtype=float)
    dataset_train_labels = np.ndarray(shape=(num_train_points,))
    dataset_test = np.ndarray(shape=(num_test_points,num_features),dtype=float)
    dataset_test_labels = np.ndarray(shape=(num_test_points,))

    for i in range(0, num_train_points):
        dataset_train[i,:] = sess.run(dense_feature)
        dataset_train_labels[i] = sess.run(label)
        print "point: {0}, label: {1}".format(i,dataset_train_labels[i])
        gc.collect()	

    for i in range(0, num_test_points):
        dataset_test[i,:] = sess.run(dense_feature)
        dataset_test_labels[i] = sess.run(label)
        print "point: {0}, label: {1}".format(i,dataset_test_labels[i])
        gc.collect()
		

data = mx.symbol.Variable('data')
fc1  = mx.symbol.FullyConnected(data = data, num_hidden=1)
act1 = mx.symbol.Activation(data = fc1, act_type='sigmoid') 
mlp  = mx.symbol.SoftmaxOutput(data = act1, name = 'softmax')

batch_size = 1
train = mx.io.NDArrayIter(data=dataset_train, label=dataset_train_labels, batch_size=batch_size, shuffle=False)
test = mx.io.NDArrayIter(data=dataset_test, label=dataset_test_labels, batch_size=batch_size, shuffle=False)

devices = [mx.cpu(0)]
kv = mx.kvstore.create('local')
head = '%(asctime)-15s Node[' + str(kv.rank) + '] %(message)s'
logging.basicConfig(level=logging.DEBUG, format=head)
model = mx.model.FeedForward(ctx=devices, symbol=mlp, num_epoch=2, learning_rate=0.1)

eval_metrics = ['accuracy']
batch_end_callback = []
batch_end_callback.append(mx.callback.Speedometer(1, 10))
model.fit(X=train, kvstore=kv, eval_metric=eval_metrics, batch_end_callback=batch_end_callback) 

print "score"
print 'Accuracy:', model.score(X=test)*100, '%'




