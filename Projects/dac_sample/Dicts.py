import re
import numpy as np

sets = [set() for i in range(26)]
dicts = [dict() for i in range(26)]
lines = [line.rstrip('\n') for line in open('dac_sample.txt')]
label = []
matrix = []
for l in lines:
    vector = l.split('\t')
    for i in range(14,40):
        if vector[i] != '':
            sets[i-14].add(vector[i])
        else:
            vector[i] = 99999
for i in range(26):
    length = len(sets[i])
    for j in range(length):
        dicts[i].setdefault(list(sets[i])[j], j)

fout = open('Dict_for_sample.txt','w')
for each in dicts:
    fout.write(str(each)+'\n')
fout.close()
