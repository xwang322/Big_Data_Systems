import csv
import os

directory = 'split-dataset'
UserList = set()
UserActionList = {}
for filename in os.listdir(directory):
    if filename.endswith('.csv'):
        with open(os.path.join(directory, filename)) as f:
            for lines in f:
                items = lines.split(',')
                UserList.add(items[0])
                UserList.add(items[1])

with open('CS-838-Assignment2-PartB-Question3-UserList.csv', 'w') as csvfile:
    contents = csv.writer(csvfile, lineterminator = '\n')
    for each in UserList:
        contents.writerow([each])
        






	
