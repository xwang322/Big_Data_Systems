from subprocess import call
read_1 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0, 'vm-38-5':0}
write_1 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0, 'vm-38-5':0}
read_2 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0, 'vm-38-5':0}
write_2 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0, 'vm-38-5':0}
call(["pdsh -w ubuntu@vm-38-[1-5] 'cat /proc/diskstats' > disk.log"], shell=True)
with open('disk.log') as f:
    for line in f:
        words = line.split(" ")
        words = filter(None, words)
        if words[3] == "vda":
            read_1[words[0][:-1]] += long(words[6])
            write_1[words[0][:-1]] += long(words[10])
call(["./spark-submit CS-838-Assignment2-PartA-Question3.py web-BerkStan.txt 10"], shell=True)
call(["pdsh -w ubuntu@vm-38-[1-5] 'cat /proc/diskstats' > disk.log"], shell=True)
with open('disk.log') as f:
    for line in f:
        words = line.split(" ")
        words = filter(None, words)
        if words[3] == "vda":
            read_2[words[0][:-1]] += long(words[6])
            write_2[words[0][:-1]] += long(words[10])
read_2['vm-38-1'] = read_2['vm-38-1'] - read_1['vm-38-1']
read_2['vm-38-2'] = read_2['vm-38-2'] - read_1['vm-38-2']
read_2['vm-38-3'] = read_2['vm-38-3'] - read_1['vm-38-3']
read_2['vm-38-4'] = read_2['vm-38-4'] - read_1['vm-38-4']
read_2['vm-38-5'] = read_2['vm-38-5'] - read_1['vm-38-5']
write_2['vm-38-1'] = write_2['vm-38-1'] - write_1['vm-38-1']
write_2['vm-38-2'] = write_2['vm-38-2'] - write_1['vm-38-2']
write_2['vm-38-3'] = write_2['vm-38-3'] - write_1['vm-38-3']
write_2['vm-38-4'] = write_2['vm-38-4'] - write_1['vm-38-4']
write_2['vm-38-5'] = write_2['vm-38-5'] - write_1['vm-38-5']
print 'read: ', read_2
print 'write: ', write_2
