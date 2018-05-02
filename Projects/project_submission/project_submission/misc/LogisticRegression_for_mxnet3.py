import os
import sys
import numpy as np
import mxnet as mx
import logging
import tensorflow as tf
import gc

num_features = 33762578
def TFRecordIter():
    print 'here it is'
    g = tf.Graph()
    with g.as_default():
        filename_queue = tf.train.string_input_producer([
            "/home/ubuntu/criteo-tfr/tfrecords00",
        ], num_epochs=None)
        reader = tf.TFRecordReader()
        _, serialized_example = reader.read(filename_queue)
        features = tf.parse_single_example(serialized_example, features={
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

        num_points = 1
        dataset = np.ndarray(shape=(num_points,num_features),dtype=float)
        dataset_labels = np.ndarray(shape=(num_points,))

        dataset[0,:] = sess.run(dense_feature)
        dataset_labels[0] = sess.run(label)
        if dataset_labels[0] == -1.0:
            dataset_labels[0] = 0
        print dataset
        print "label: {0}".format(dataset_labels[0])
        gc.collect()	 
        return dataset, dataset_labels
        sess.close()		
				
data = mx.symbol.Variable('data')
fc1  = mx.symbol.FullyConnected(data = data, num_hidden=1)
act1 = mx.symbol.Activation(data = fc1, act_type='sigmoid') 
mlp  = mx.symbol.SoftmaxOutput(data = act1, name = 'softmax')

batch_size = 1
num_iteration_train = 10
for i in range(num_iteration_train):
    print "this is the {0} iteration for train".format(i+1)
    returnlist = TFRecordIter()
    train = mx.io.NDArrayIter(data=returnlist[0], label=returnlist[1], batch_size=batch_size, shuffle=False)

num_iteration_test = 5    
for i in range(num_iteration_test):
    print "this is the {0} iteration for test".format(i+1)
    returnlist = TFRecordIter()
    test = mx.io.NDArrayIter(data=returnlist[0], label=returnlist[1], batch_size=batch_size, shuffle=False)

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




