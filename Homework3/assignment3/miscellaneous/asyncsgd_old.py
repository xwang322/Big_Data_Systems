import tensorflow as tf
import os

tf.app.flags.DEFINE_integer("task_index", 0, "Index of the worker task")
FLAGS = tf.app.flags.FLAGS

num_features = 33762578
eta = 0.01
g = tf.Graph()

with g.as_default():
    with tf.device("/job:worker/task:0"):
        w = tf.Variable(tf.random_uniform([num_features, 1]), name = "one_worker")

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
			
        dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index), [num_features,], tf.sparse_tensor_to_dense(value))    
        shaped_label = tf.reshape(label,[1])
        shaped_matrix = tf.reshape(dense_feature, [num_features, 1])
        intermediate = tf.sigmoid(tf.mul(tf.matmul(tf.matrix_transpose(w), shaped_matrix), tf.to_float(shaped_label)))
        local_gradient = tf.to_float(shaped_label)[0] * (intermediate-1) * shaped_matrix		
			
    with tf.device("/job:worker/task:0"):
        assign_op = w.assign_add(tf.mul(local_gradient, -eta))
   
    coord = tf.train.Coordinator()   
    with tf.Session("grpc://vm-38-%d:2222" % (FLAGS.task_index+1)) as sess:
        if FLAGS.task_index == 0:
            sess.run(tf.initialize_all_variables())
        threads = tf.train.start_queue_runners(sess=sess, coord=coord)
        print w.eval()
        for i in range(0, 2000):
            if coord.should_stop():
                break
            sess.run(assign_op)
            print i
        print w.eval()
        coord.request_stop()
        coord.join(threads)

    with tf.device("/job:worker/task:0"):		
        filename_queue_test = tf.train.string_input_producer([
		    "/home/ubuntu/criteo-tfr/tfrecords22",		
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
        judge = tf.matmul(tf.matrix_transpose(w), shaped_matrix_test)[0][0]

    print "calculating error"
    coord_test = tf.train.Coordinator()			
    with tf.Session("grpc://vm-38-1:2222") as sess_test:
        error = 0
        sess_test.run(tf.initialize_all_variables())
        threads_test = tf.train.start_queue_runners(sess=sess_test, coord=coord_test)
        for i in range(0, 1840617): # this is the number of lines in testing file
            if coord_test.should_stop():
                break
            new_label_test = sess_test.run(shaped_label_test)
            result = sess_test.run(judge)
            if result <= 0 and new_label_test[0] > 0:
                error += 1
            elif result > 0 and new_label_test[0] < 0:
                error += 1
            else:
                pass
            if (i % 100000 == 0):
                print(i+1, error)
        coord_test.request_stop()
        coord_test.join(threads_test)
        print(i+1, error)	
