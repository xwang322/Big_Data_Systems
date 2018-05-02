import tensorflow as tf
num_features = 33762578
eta = 0.1
g = tf.Graph()
with g.as_default():
    w = tf.Variable(tf.random_uniform([num_features, ], minval=-1.0, maxval=1.0), name = "one_worker")

    filename_queue = tf.train.string_input_producer([
        "/home/ubuntu/criteo-tfr/tfrecords20",
	"/home/ubuntu/criteo-tfr/tfrecords21",
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

    simple_weight = tf.gather(params=w, indices=index.values)
    shaped_weight = tf.reshape(simple_weight, [tf.size(simple_weight), 1])
    shaped_value = tf.reshape(value.values, [tf.size(value.values), 1])
    intermediate = tf.sigmoid(tf.to_float(label) * tf.matmul(tf.matrix_transpose(shaped_weight), shaped_value))
    local_gradient = tf.mul(-eta, tf.mul(tf.mul(tf.to_float(label), (intermediate-1)), value.values))
    shaped_index = tf.reshape(index.values, tf.shape(index.indices))
    shaped_update = tf.reshape(local_gradient, tf.shape(value.values))
    weight_update = tf.SparseTensor(indices=shaped_index, values=shaped_update, shape=[num_features, ])
    aggregator = tf.sparse_to_dense(weight_update.indices, [num_features, ], weight_update.values)
    assign_op = w.assign_add(aggregator) 

    sess = tf.Session()
    sess.run(tf.initialize_all_variables())
    tf.train.start_queue_runners(sess=sess)
    initial = sess.run(w)
    print initial	
    for i in range(3000):
       new = sess.run(assign_op)
    print w.eval(session=sess)
	
    filename_queue_test = tf.train.string_input_producer([
	"/home/ubuntu/criteo-tfr/tfrecords22",		
    ], num_epochs=None)	
    reader = tf.TFRecordReader()
    _, serialized_example_test = reader.read(filename_queue_test)
    features_test = tf.parse_single_example(serialized_example_test, features={
                                        'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                        'index' : tf.VarLenFeature(dtype=tf.int64),
                                        'value' : tf.VarLenFeature(dtype=tf.float32),})
    label_test = features_test['label']
    index_test = features_test['index']
    value_test = features_test['value']
    dense_feature_test = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index_test), [num_features,], tf.sparse_tensor_to_dense(value_test))
    sess_test = tf.Session()	
    sess_test.run(tf.initialize_all_variables())
    tf.train.start_queue_runners(sess_test)

    shaped_label_test = tf.reshape(label_test, [1])
    shaped_matrix_test = tf.reshape(dense_feature_test, [num_features, 1])
    judge = tf.matmul(tf.matrix_transpose(tf.reshape(w, [num_features, 1])), shaped_matrix_test)
	
    error = 0
    print "calculating error"
    for i in range(100):
        result = sess_test.run(judge)
        new_label_test = sess_test.run(shaped_label_test)
        print new_label_test[0], result[0][0]
        if result[0][0] <= 0 and new_label_test[0] == 1:
            error += 1
        elif result[0][0] >= 0 and new_label_test[0] == -1:
            error += 1
        else:
            pass
    print(i+1, error)			
