import tensorflow as tf
num_features = 33762578
eta = 0.01
g = tf.Graph()
with g.as_default():
    w = tf.Variable(tf.random_uniform([num_features, 1]), name = "one_worker")
    filename_queue = tf.train.string_input_producer([
        "./data/criteo-tfr-tiny/tfrecords00",
		"./data/criteo-tfr-tiny/tfrecords01",
		"./data/criteo-tfr-tiny/tfrecords02",
		"./data/criteo-tfr-tiny/tfrecords03",
		"./data/criteo-tfr-tiny/tfrecords04",
		"./data/criteo-tfr-tiny/tfrecords05",
		"./data/criteo-tfr-tiny/tfrecords06",
		"./data/criteo-tfr-tiny/tfrecords07",
		"./data/criteo-tfr-tiny/tfrecords08",
		"./data/criteo-tfr-tiny/tfrecords09",
		"./data/criteo-tfr-tiny/tfrecords10",
		"./data/criteo-tfr-tiny/tfrecords11",
		"./data/criteo-tfr-tiny/tfrecords12",
		"./data/criteo-tfr-tiny/tfrecords13",
		"./data/criteo-tfr-tiny/tfrecords14",
		"./data/criteo-tfr-tiny/tfrecords15",
		"./data/criteo-tfr-tiny/tfrecords16",
		"./data/criteo-tfr-tiny/tfrecords17",
		"./data/criteo-tfr-tiny/tfrecords18",
		"./data/criteo-tfr-tiny/tfrecords19",
		"./data/criteo-tfr-tiny/tfrecords20",
		"./data/criteo-tfr-tiny/tfrecords21",		
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

    shaped_label = tf.reshape(label, [ 1])
    new_label = sess.run(shaped_label)
    shaped_matrix = tf.reshape(dense_feature, [num_features, 1])

    intermediate = tf.sigmoid(tf.matmul(tf.matrix_transpose(w), shaped_matrix)[0][0] * float(new_label[0]))
    update = new_label[0] * (intermediate-1) * shaped_matrix
    assign_op = w.assign_add(-eta * update)

    initial = sess.run(w)
    print initial	
    for i in range(200):
        weight = sess.run(assign_op)
    print weight
	
    filename_queue_test = tf.train.string_input_producer([
		"./data/criteo-tfr-tiny/tfrecords22",		
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
    judge = tf.matmul(tf.matrix_transpose(weight), shaped_matrix_test)[0][0]
    
    error = 0
    print "calculating error"
    for i in range(10):
        new_label_test = sess_test.run(shaped_label_test)
        result = sess_test.run(judge)
        if result <= 0 and new_label_test[0] > 0:
            error += 1
        elif result >= 0 and new_label_test[0] < 0:
            error += 1
        else:
            pass
    print(i+1, error)			
