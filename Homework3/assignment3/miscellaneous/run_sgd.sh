#!/bin/bash
source tfdefs.sh
terminate_cluster
start_cluster startserver.py
# start multiple clients
python asyncsgd.py
