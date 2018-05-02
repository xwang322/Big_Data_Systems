import tensorflow as tf
import mxnet as mx
import gc
import numpy as np

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
    # batch processing
    '''
    min_after_dequeue = 100
    batch_size = 100
    capacity = min_after_dequeue + 3 * batch_size
    example_batch = tf.train.shuffle_batch([dense_feature], batch_size=batch_size, capacity=capacity, min_after_dequeue=min_after_dequeue)
    print example_batch
    '''
    sess = tf.Session()
    sess.run(tf.initialize_all_variables())
    tf.train.start_queue_runners(sess=sess)

    num_train_points = 25
    num_test_points = 5
    np_train = np.ndarray(shape=(num_train_points,num_features),dtype=float)
    np_train_labels = np.ndarray(shape=(num_train_points,))
    np_test = np.ndarray(shape=(num_test_points,num_features),dtype=float)
    np_test_labels = np.ndarray(shape=(num_test_points,))

    for i in range(0, num_train_points):
        np_train[i,:] = sess.run(dense_feature)
        np_train_labels[i,] = sess.run(label)
        print "point: {0}, label: {1}".format(i,np_train_labels[i])
        print np_train[i]
        print type(np_train)
        print np_train[0][33762577]
        gc.collect()

    for i in range(0, num_test_points):
        np_test[i,:] = sess.run(dense_feature)
        np_test_labels[i,] = sess.run(label)
        print "point: {0}, label: {1}".format(i,np_test_labels[i])
        gc.collect()
    print np_train.shape
    print np_train_labels.shape

    mx.nd.save("criteo_{0}.mx".format(num_train_points), {'labels' : mx.nd.array(np_train_labels), 'features' : mx.nd.array(np_train)})
    mx.nd.save("criteo_{0}.mx".format(num_test_points), {'labels' : mx.nd.array(np_test_labels), 'features' : mx.nd.array(np_test)}) 



