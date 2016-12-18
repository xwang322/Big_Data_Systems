import json, sys
import os

with open('C:\Academy\CS 838\Fall 2016\Homework1\data\mr_history-20160927T050343Z\mr_history\job_1474751300160_0092-1474774981814-ubuntu-select++substr_28r_reason_desc_2C1_2C20_29+as+...100_28Stage-1474774997638-1-1-SUCCEEDED-default-1474774986460.jhist','r') as f:
    job_start = []
    job_finish = []
    for line in f:
        struct = {}
        try:
            dataform = str(line).strip("'<>() ").replace('\'', '\"')
            struct = json.loads(dataform)
        except:
            continue
        if struct is not None:
            for key, value in struct.items():
                if key.lower() == 'type' and value.lower() == 'task_started':
                    temp = []
                    for key1, value1 in struct['event']['org.apache.hadoop.mapreduce.jobhistory.TaskStarted'].items():
                        if key1.lower() == 'tasktype':
                            temp.append(value1.lower())
                        if key1.lower() == 'taskid':
                            temp.append(value1.lower())
                        if key1.lower() == 'starttime':
                            temp.append('start time: ' + str(value1))						
                    job_start.append(temp)              
                if key.lower() == 'type' and value.lower() == 'task_finished':
                    temp = []
                    for key1, value1 in struct['event']['org.apache.hadoop.mapreduce.jobhistory.TaskFinished'].items():
                        if key1.lower() == 'tasktype':
                            temp.append(value1.lower())
                        if key1.lower() == 'taskid':
                            temp.append(value1.lower())
                        if key1.lower() == 'finishtime':
                            temp.append('finish time: ' + str(value1))							
                    job_finish.append(temp)
    for each in job_start:
        for every in each:
            if 'task' in str(every):
                find_out = every
                for items in job_finish:
                    for item in items:
                        if str(item) == find_out:
                            index = job_finish.index(items)
                            break
                for add_item in job_finish[index]:
                    if 'finish time' in add_item:
                        each.append(add_item)
f.close()

z = open("C:\Academy\CS 838\Fall 2016\Homework1\data\mr_history-20160927T050343Z\mr_stats\stats92.txt",'w')
for each in job_start:
    print(each, file = z)
z.close()
   	
          