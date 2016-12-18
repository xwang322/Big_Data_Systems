import subprocess
import os
from os import listdir
from os.path import isfile, join
my_path = 'C:\Academy\CS 838\Fall 2016\Homework1\data\mr_history-20160927T050343Z\mr_history'
onlyfiles = [f for f in listdir(my_path) if isfile(join(my_path, f))]
newpath = []
for each in onlyfiles:
    newpath.append(my_path + each)
for each in newpath:
    print(each)
    os.system('python jhist.py')