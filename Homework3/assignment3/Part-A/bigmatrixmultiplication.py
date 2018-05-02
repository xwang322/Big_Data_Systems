import tensorflow as tf
import os

N = 100000
d = 10
M = int(N/d)

def get_block_name(i,j):
    return "sub-matrix-"+str(i)+"-"+str(j)

def get_intermediate_trace_name(i,j):
    return "inter-"+str(i)+"-"+str(j)

g = tf.Graph()
with g.as_default():
    tf.logging.set_verbosity(tf.logging.DEBUG)
    tf.set_random_seed(1)

    matrices = {}
    for i in range(d):
        for j in range(d):
            matrix_name = get_block_name(i,j)
            matrices[matrix_name] = tf.random_uniform([M, M], name = matrix_name)

    intermediate_traces = {}
    for i in range(5):
        with tf.device("/job:worker/task:%d" % i):
            for j in range(2*i, 2*i+2):
                for k in range(d):
                    A = matrices[get_block_name(j, k)]
                    B = matrices[get_block_name(k, j)]
                    intermediate_traces[get_intermediate_trace_name(j, k)] = tf.trace(tf.matmul(A, B))
    with tf.device("/job:worker/task:0"):
        retval = tf.add_n(intermediate_traces.values())
    config = tf.ConfigProto(log_device_placement=True)
    with tf.Session("grpc://vm-38-1:2222", config=config) as sess:
        result = sess.run(retval)
        sess.close()
    print "Trace of the big matrix is =", result
