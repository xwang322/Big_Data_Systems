#!/bin/bash
../../mxnet/tools/launch.py -n 5 --launcher ssh -H hosts python LogisticRegression_for_tf_dist.py  --kv-store dist_sync 







