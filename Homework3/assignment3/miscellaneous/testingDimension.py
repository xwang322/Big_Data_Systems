import tensorflow as tf

num_features = 33762578
g = tf.Graph()

with g.as_default():
    filename_queue = tf.train.string_input_producer([
        "./data/criteo-tfr-tiny/tfrecords00",
        ],
        num_epochs=None
    )


    reader = tf.TFRecordReader()
    _, serialized_example = reader.read(filename_queue)
    features = tf.parse_single_example(serialized_example,
                                       features={
                                        'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                        'index' : tf.VarLenFeature(dtype=tf.int64),
                                        'value' : tf.VarLenFeature(dtype=tf.float32),
                                       }
                                      )

    label = features['label']
    index = features['index']
    value = features['value']

    size_indices = tf.size(index.values)
    size_values = tf.size(value.values)

    # as usual we create a session.
    sess = tf.Session()
    sess.run(tf.initialize_all_variables())
    sess.run(tf.initialize_local_variables())

    tf.train.start_queue_runners(sess=sess)
    for i in range(0, 10):
        a,b = sess.run([size_indices, size_values])
        if a != b:
            print "error", i , a, b
        else:
            print i, a, b
