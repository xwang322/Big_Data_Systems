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

    shaped_label = tf.reshape(label,[1])
    new_label = sess.run(shaped_label)
    shaped_matrix = tf.reshape(dense_feature, [num_features, 1])

    intermediate = tf.sigmoid(tf.matmul(tf.matrix_transpose(w), shaped_matrix)[0][0] * float(new_label[0]))
    update = new_label[0] * (intermediate-1) * shaped_matrix
    assign_op = w.assign_add(-eta * update)

    initial = sess.run(w)
    print initial
    for i in range(20):
        result = sess.run(assign_op)
    print result
 
