from sklearn.naive_bayes import GaussianNB
from sklearn.cross_validation import cross_val_score
import numpy as np
import re
import ast

dicts = [dict() for i in range(26)]
dicts_lines = [line.rstrip('\n') for line in open('Dict_for_sample.txt')]
for i in range(26):
    dicts[i] = ast.literal_eval(dicts_lines[i])

label = []
matrix = []
lines = [line.rstrip('\n') for line in open('dac_sample_train.txt')]
for l in lines:
    vector = l.split('\t')
    label.append(vector[0])
    int_array = []
    str_array = []
    for i in range(1,14):
        if vector[i] != '':
            int_array.append(int(vector[i]))
        else:
            int_array.append(99999)
    for i in range(14,40):
        lookup = dicts[i-14].get(vector[i])	
        if lookup == None:
            str_array.append(99999)
        elif lookup != '':
            str_array.append(lookup)
    array = int_array + str_array
    x = np.array(array)
    y = x.astype(np.int)
    matrix.append(y)
a = np.array(label)
b = a.astype(np.int)
label = b	

label_test = []
matrix_test = []
lines_test = [line.rstrip('\n') for line in open('dac_sample_test.txt')]
for l in lines_test:
    vector_test = l.split('\t')
    label_test.append(vector_test[0])
    int_array_test = []
    str_array_test = []
    for i in range(1,14):
        if vector_test[i] != '':
            int_array_test.append(int(vector_test[i]))
        else:
            int_array_test.append(99999)
    for i in range(14,40):
        lookup_test = dicts[i-14].get(vector_test[i])	
        if lookup_test == None:
            str_array_test.append(99999)
        elif lookup_test != '':
            str_array_test.append(lookup_test)
    array_test = int_array_test + str_array_test
    x_test = np.array(array_test)
    y_test = x_test.astype(np.int)
    matrix_test.append(y_test)
a_test = np.array(label_test)
b_test = a_test.astype(np.int)
label_test = b_test

clf = GaussianNB()
clf = clf.fit(matrix, label)
scores = cross_val_score(clf, matrix, label)
print(scores.mean())
scores_test = cross_val_score(clf, matrix_test, label_test)
print('GaussianNB:')
print(scores_test.mean())
print('predicted classes:')
print(clf.predict(matrix_test))