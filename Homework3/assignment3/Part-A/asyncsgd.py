import tensorflow as tf
import os

tf.app.flags.DEFINE_integer("task_index", 0, "Index of the worker task")
FLAGS = tf.app.flags.FLAGS

num_features = 33762578
eta = 0.01
g = tf.Graph()

with g.as_default():
    with tf.device("/job:worker/task:0"):
        w = tf.Variable(tf.random_uniform([num_features, ], minval=-1.0, maxval=1.0), name = "async_mode")

    with tf.device("/job:worker/task:%d" % FLAGS.task_index):
        if (FLAGS.task_index < 2):
            filename_queue = tf.train.string_input_producer([
                "/home/ubuntu/criteo-tfr/tfrecords0%d" % (FLAGS.task_index * 5 + 0),
                "/home/ubuntu/criteo-tfr/tfrecords0%d" % (FLAGS.task_index * 5 + 1),
                "/home/ubuntu/criteo-tfr/tfrecords0%d" % (FLAGS.task_index * 5 + 2),
                "/home/ubuntu/criteo-tfr/tfrecords0%d" % (FLAGS.task_index * 5 + 3),
                "/home/ubuntu/criteo-tfr/tfrecords0%d" % (FLAGS.task_index * 5 + 4),
            ], num_epochs=None)
        elif (FLAGS.task_index < 4 and FLAGS.task_index >= 2):
            filename_queue = tf.train.string_input_producer([
                "/home/ubuntu/criteo-tfr/tfrecords%d" % (FLAGS.task_index * 5 + 0),
                "/home/ubuntu/criteo-tfr/tfrecords%d" % (FLAGS.task_index * 5 + 1),
                "/home/ubuntu/criteo-tfr/tfrecords%d" % (FLAGS.task_index * 5 + 2),
                "/home/ubuntu/criteo-tfr/tfrecords%d" % (FLAGS.task_index * 5 + 3),
                "/home/ubuntu/criteo-tfr/tfrecords%d" % (FLAGS.task_index * 5 + 4),
            ], num_epochs=None)
        else:
            filename_queue = tf.train.string_input_producer([
                "/home/ubuntu/criteo-tfr/tfrecords%d" % (FLAGS.task_index * 5 + 0),
                "/home/ubuntu/criteo-tfr/tfrecords%d" % (FLAGS.task_index * 5 + 1),
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
        shaped_update = tf.reshape(local_gradient, tf.shape(index.values))
        weight_update = tf.SparseTensor(indices=shaped_index, values=shaped_update, shape=[num_features ,])		

    with tf.device("/job:worker/task:0"):
        assign_op = tf.assign_add(w, tf.sparse_to_dense(weight_update.indices, [num_features, ], weight_update.values))
   
    coord = tf.train.Coordinator()   
    with tf.Session("grpc://vm-38-%d:2222" % (FLAGS.task_index+1)) as sess:
        if FLAGS.task_index == 0:
            sess.run(tf.initialize_all_variables())
        threads = tf.train.start_queue_runners(sess=sess, coord=coord)
        print w.eval()
        for i in range(0, 100000):
            if coord.should_stop():
                break
            sess.run(assign_op)
            print i
        print w.eval()
        coord.request_stop()
        coord.join(threads)


    with tf.device("/job:worker/task:0"):		
        filename_queue_test = tf.train.string_input_producer([
		    "/home/ubuntu/criteo-tfr/tfrecords23",		
        ], num_epochs=None)			
        _, serialized_example_test = reader.read(filename_queue_test)
        features_test = tf.parse_single_example(serialized_example_test, features={
                                            'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                            'index' : tf.VarLenFeature(dtype=tf.int64),
                                            'value' : tf.VarLenFeature(dtype=tf.float32),})	
        label_test = features_test['label']
        index_test = features_test['index']
        value_test = features_test['value']
        dense_feature_test = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index_test), [num_features,], tf.sparse_tensor_to_dense(value_test))
        shaped_label_test = tf.reshape(label_test, [1])
        shaped_matrix_test = tf.reshape(dense_feature_test, [num_features, 1])
        judge = tf.matmul(tf.matrix_transpose(tf.reshape(w, [num_features, 1])), shaped_matrix_test)

    print "calculating error"
    coord_test = tf.train.Coordinator()			
    with tf.Session("grpc://vm-38-1:2222") as sess_test:
        error = 0
        sess_test.run(tf.initialize_all_variables())
        threads_test = tf.train.start_queue_runners(sess=sess_test, coord=coord_test)
        for i in range(0, 10000): # this is the number of lines in testing file
            if coord_test.should_stop():
                break
            new_label_test = sess_test.run(shaped_label_test)
            result = sess_test.run(judge)
            if result[0][0] <= 0 and new_label_test[0] == 1:
                error += 1
            elif result[0][0] > 0 and new_label_test[0] == -1:
                error += 1
            else:
                pass
            if (i % 1000 == 0):
                print(i+1, error)
        coord_test.request_stop()
        coord_test.join(threads_test)
        print(i+1, error)

