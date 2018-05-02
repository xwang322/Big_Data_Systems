import tensorflow as tf
import os

num_features = 33762578
eta = 0.01
g = tf.Graph()

with g.as_default():
    with tf.device("/job:worker/task:0"):
        w = tf.Variable(tf.random_uniform([num_features, ], minval=-1.0, maxval=1.0), name = "sync_mode")

    gradients = []		
    for i in range(0, 5):
        with tf.device("/job:worker/task:%d" %i):
            if (i < 2):
                filename_queue = tf.train.string_input_producer([
                    "/home/ubuntu/criteo-tfr/tfrecords0%d" % (i * 5 + 0),
                    "/home/ubuntu/criteo-tfr/tfrecords0%d" % (i * 5 + 1),
                    "/home/ubuntu/criteo-tfr/tfrecords0%d" % (i * 5 + 2),
                    "/home/ubuntu/criteo-tfr/tfrecords0%d" % (i * 5 + 3),
                    "/home/ubuntu/criteo-tfr/tfrecords0%d" % (i * 5 + 4),
                ], num_epochs=None)
            elif (i < 4 and i >= 2):
                filename_queue = tf.train.string_input_producer([
                    "/home/ubuntu/criteo-tfr/tfrecords%d" % (i * 5 + 0),
                    "/home/ubuntu/criteo-tfr/tfrecords%d" % (i * 5 + 1),
                    "/home/ubuntu/criteo-tfr/tfrecords%d" % (i * 5 + 2),
                    "/home/ubuntu/criteo-tfr/tfrecords%d" % (i * 5 + 3),
                    "/home/ubuntu/criteo-tfr/tfrecords%d" % (i * 5 + 4),
                ], num_epochs=None)
            else:
                filename_queue = tf.train.string_input_producer([
                    "/home/ubuntu/criteo-tfr/tfrecords%d" % (i * 5 + 0),
                    "/home/ubuntu/criteo-tfr/tfrecords%d" % (i * 5 + 1),
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
            dense_label = tf.reshape(label,[1])			
            min_after_dequeue = 100
            batch_size = 100
            capacity = min_after_dequeue + 3 * batch_size
            features_batch = tf.train.shuffle_batch([dense_feature], batch_size=batch_size, capacity=capacity, min_after_dequeue=min_after_dequeue)
            labels_batch = tf.train.shuffle_batch([dense_label], batch_size=batch_size, capacity=capacity, min_after_dequeue=min_after_dequeue)
            matrix_batch = tf.reshape(features_batch, [batch_size, num_features, 1])
            intermediate_batch = tf.sigmoid(tf.mul(tf.matmul(tf.matrix_transpose(w), matrix_batch), tf.to_float(labels_batch)))
            update_batch = tf.to_float(labels_batch)[0] * (intermediate_batch-1) * matrix_batch
            gradients.append(-eta * update_batch)
			
    with tf.device("/job:worker/task:0"):
        for each in gradients:
            aggregator = tf.add_n(gradients)
            assign_op = tf.assign_add(w, aggregator)

    coord = tf.train.Coordinator()		
    with tf.Session("grpc://vm-38-1:2222") as sess:
        sess.run(tf.initialize_all_variables())
        threads = tf.train.start_queue_runners(sess=sess, coord=coord)
        print w.eval()
        for i in range(0, 10000):
            if coord.should_stop():
                break
            sess.run(assign_op)
            print i
        coord.request_stop()
        coord.join(threads)

    with tf.device("/job:worker/task:0"):		
        filename_queue_test = tf.train.string_input_producer([
		    "/home/ubuntu/criteo-tfr/tfrecords23",		
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
        shaped_label_test = tf.reshape(label_test, [1])
        shaped_matrix_test = tf.reshape(dense_feature_test, [num_features, 1])
        judge = tf.matmul(tf.matrix_transpose(tf.reshape(w, [num_features, 1])), shaped_matrix_test)

    print "calculating error"
    coord_test = tf.train.Coordinator()			
    with tf.Session("grpc://vm-38-1:2222") as sess_test:
        error = 0
        sess_test.run(tf.initialize_all_variables())
        threads_test = tf.train.start_queue_runners(sess=sess_test, coord=coord_test)
        for i in range(0, 10000):
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














											
