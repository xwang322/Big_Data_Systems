import mxnet as mx
from mxnet import io
import tensorflow as tf
import numpy as np
import logging


class CriteoTFRecordIter(io.DataIter):
    def __init__(self, data=None, label=None, batch_size=1, shuffle=False, last_batch_handle='pad'):
        # pylint: disable=W0201
        super(CriteoTFRecordIter, self).__init__()
        self.num_data = 2000
        self.batch_size = 1
        self.cursor = 0
        self.num_features = 33762578
        self.g = tf.Graph()
        with self.g.as_default():
            input_data = list()
            if isinstance(data, str):
                input_data.append(data)
            filename_queue = tf.train.string_input_producer(input_data, num_epochs=None)
            reader = tf.TFRecordReader()
            _, serialized_example = reader.read(filename_queue)
            features = tf.parse_single_example(serialized_example,
                                            features={
                                                'label': tf.FixedLenFeature([1], dtype=tf.int64),
                                                'index' : tf.VarLenFeature(dtype=tf.int64),
                                                'value' : tf.VarLenFeature(dtype=tf.float32),
                                            }
                                            )
            self.label = features['label']
            index = features['index']
            value = features['value']
            self.dense_feature = tf.sparse_to_dense(tf.sparse_tensor_to_dense(index),
                                        [self.num_features,],
                                        tf.sparse_tensor_to_dense(value))
            self.sess = tf.Session()
            self.sess.run(tf.initialize_all_variables())
            tf.train.start_queue_runners(sess=self.sess)

    @property
    def provide_data(self):
        """The name and shape of data provided by this iterator"""
        output =  self.sess.run(self.dense_feature)
        output = np.transpose(output.reshape(len(output), 1))
        return [('data', output.shape)]

    @property
    def provide_label(self):
        """The name and shape of label provided by this iterator"""
        l = self.sess.run(self.label)
        return [('softmax_label', l.shape)]

    def hard_reset(self):
        """Igore roll over data and set to start"""
        self.cursor = 0

    def reset(self):
        self.cursor = 0

    def iter_next(self):
        self.cursor += self.batch_size
        return self.cursor < self.num_data

    def next(self):
        if self.iter_next():
            return io.DataBatch(data=self.getdata(), label=self.getlabel(), \
                    pad=self.getpad(), index=None)
        else:
            raise StopIteration

    def _getdata(self):
        """Load data from underlying arrays, internal use only"""
        output =  self.sess.run(self.dense_feature)
        output = np.transpose(output.reshape(len(output), 1))
        self.cursor += 1
        mx_o = mx.nd.array(output)
        return [mx_o]

    def _getlabel(self):
        o = list()
        l = self.sess.run(self.label)
        if l[0] == -1.0:
            l[0] = 0
        mx_l = mx.nd.array(l)
        return [mx_l]

    def getdata(self):
        return self._getdata()

    def getlabel(self):
        return self._getlabel()

    def getpad(self):
        return None


if __name__ == "__main__":
    train = CriteoTFRecordIter('/home/ubuntu/criteo-tfr/tfrecords00')
    test = CriteoTFRecordIter('/home/ubuntu/criteo-tfr/tfrecords04')
    
    batch_size = 1
    data = mx.symbol.Variable('data')
    fc1  = mx.symbol.FullyConnected(data = data, num_hidden=1)
    act1 = mx.symbol.Activation(data = fc1, act_type="relu") 
    mlp  = mx.symbol.SVMOutput(data = act1, name = 'softmax')
   
    kv = mx.kvstore.create('local')
    head = '%(asctime)-15s Node[' + str(kv.rank) + '] %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=head)
    devices = [mx.cpu(0)]
    model = mx.model.FeedForward(ctx=devices, symbol=mlp, num_epoch=1, learning_rate=0.01)

    eval_metrics = ['accuracy']
    batch_end_callback = []
    batch_end_callback.append(mx.callback.Speedometer(1, 10))


    model.fit(X                = train,
            kvstore            = kv,
            eval_metric        = eval_metrics,
            batch_end_callback = batch_end_callback)

    model.fit(X=train)
    print "score"
    print model.score(X=test)
