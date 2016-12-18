import json, sys
import re

with open("C:\Academy\CS 838\Fall 2016\Homework1\data\mr_history-20160927T050343Z\mr_stats\stats92.txt",'r') as f:
    timestep = 9999999999999
    r = 0
    for line in f:
        r += 1
        items = line.split(',')
        for item in items:
            if 'start time' in item:
                start_number = re.findall(r'[\d|]+', item)
                temp = ''.join(start_number)
                if int(temp) < timestep:
                    timestep = int(temp)
    print(timestep, r)
    timelist = [[0 for i in range(2)] for j in range(r)]
    r = 0
f.close()
with open("C:\Academy\CS 838\Fall 2016\Homework1\data\mr_history-20160927T050343Z\mr_stats\stats92.txt",'r') as f:
    for line in f:
        items = line.split(',')
        for item in items:
            if 'start time' in item:
                start_number = re.findall(r'[\d|]+', item)
                temp = ''.join(start_number)
                timelist[r][0] = int(temp) - timestep
            if 'finish time' in item:
                finish_number = re.findall(r'[\d|]+', item)
                temp = ''.join(finish_number)
                timelist[r][1] = int(temp) - timestep
        r += 1
    print(timelist)		
f.close()
