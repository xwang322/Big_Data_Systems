import os
import sys
import numpy as np
import mxnet as mx
import logging
import tensorflow as tf
import gc

data = mx.symbol.Variable('data')
fc1  = mx.symbol.FullyConnected(data = data, num_hidden=1)
act1 = mx.symbol.Activation(data = fc1, act_type='sigmoid') 
mlp  = mx.symbol.LogisticRegressionOutput(data = act1, name = 'softmax')

num_features = 33762578
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

    num_points = 1
    dataset = np.ndarray(shape=(num_points,num_features),dtype=float)
    dataset_labels = np.ndarray(shape=(num_points,))

    batch_size = 1
    num_iteration_train = 100
    coord = tf.train.Coordinator()
    with tf.Session("grpc://vm-38-1:2222") as sess:
        sess.run(tf.initialize_all_variables())
        threads = tf.train.start_queue_runners(sess=sess, coord=coord)		
        for i in range(num_iteration_train):
            if coord.should_stop():
                break
            print "this is the {0} iteration for train".format(i+1)
            dataset[0,:] = sess.run(dense_feature)
            dataset_labels[0] = sess.run(label)
            if dataset_labels[0] == -1.0:
                dataset_labels[0] = 0
            print dataset
            print "label: {0}".format(dataset_labels[0])
            train = mx.io.NDArrayIter(data=dataset, label=dataset_labels, batch_size=batch_size, shuffle=False)
					
            devices = [mx.cpu(0)]
            kv = mx.kvstore.create('local')
            head = '%(asctime)-15s Node[' + str(kv.rank) + '] %(message)s'
            logging.basicConfig(level=logging.DEBUG, format=head)
            model = mx.model.FeedForward(ctx=devices, symbol=mlp, num_epoch=2, learning_rate=0.1)
            eval_metrics = ['accuracy']
            batch_end_callback = []
            batch_end_callback.append(mx.callback.Speedometer(1, 10))
            model.fit(X=train, kvstore=kv, eval_metric=eval_metrics, batch_end_callback=batch_end_callback)
        coord.request_stop()
        coord.join(threads)
    sess.close()

    num_iteration_test = 10
    coord_test = tf.train.Coordinator()	
    with tf.Session("grpc://vm-38-1:2222") as sess_test:
        sess_test.run(tf.initialize_all_variables())
        threads_test = tf.train.start_queue_runners(sess=sess_test, coord=coord_test)		
        for i in range(num_iteration_test):
            if coord_test.should_stop():
                break
            print "this is the {0} iteration for test".format(i+1)
            dataset[0,:] = sess_test.run(dense_feature)
            dataset_labels[0] = sess_test.run(label)
            if dataset_labels[0] == -1.0:
                dataset_labels[0] = 0
            print dataset
            print "label: {0}".format(dataset_labels[0])
            test = mx.io.NDArrayIter(data=dataset, label=dataset_labels, batch_size=batch_size, shuffle=False)
            print "score"
            print 'Accuracy:', model.score(X=test)*100, '%'
        coord_test.request_stop()
        coord_test.join(threads_test)
    sess_test.close()
		
gc.collect()	  






