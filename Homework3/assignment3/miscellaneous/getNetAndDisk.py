from subprocess import call

receive_1 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0,'vm-38-5': 0}
transmit_1 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0,'vm-38-5': 0}

receive_2 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0,'vm-38-5': 0}
transmit_2 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0,'vm-38-5': 0}

read_1 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0, 'vm-38-5':0}
write_1 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0, 'vm-38-5':0}

read_2 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0, 'vm-38-5':0}
write_2 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0, 'vm-38-5':0}


call(["pdsh -w ubuntu@vm-38-[1-5] 'cat /proc/net/dev' > net.log"], shell=True)
call(["pdsh -w ubuntu@vm-38-[1-5] 'cat /proc/diskstats' > disk.log"], shell=True)

with open('net.log') as f:
    for line in f:
        words = line.split(":")
        if words[1].strip() == "eth0":
            vm = words[0].strip()
            num = words[2].split(" ")
            num = filter(None, num)
            receive = num[0].strip()
            transmit = num[8].strip()
            # print(vm + " receives " + receive + ", transmit " + transmit)
            receive_1[vm] = long(receive)
            transmit_1[vm] = long(transmit)
            # print (vm + " receive "), receive_1[vm]
            # print (vm + " transmit "), transmit_1[vm]

with open('disk.log') as f:
    for line in f:
        words = line.split(" ")
        words = filter(None, words)
        if words[3] == "vda":
            read_1[words[0][:-1]] += long(words[6])
            write_1[words[0][:-1]] += long(words[10])

####################
call(["./run_sgd.sh"], shell=True)
####################


call(["pdsh -w ubuntu@vm-38-[1-5] 'cat /proc/net/dev' > net.log"], shell=True)
call(["pdsh -w ubuntu@vm-38-[1-5] 'cat /proc/diskstats' > disk.log"], shell=True)

with open('net.log') as f:
    for line in f:
        words = line.split(":")
        if words[1].strip() == "eth0":
            vm = words[0].strip()
            num = words[2].split(" ")
            num = filter(None, num)
            receive = num[0].strip()
            transmit = num[8].strip()
            # print(vm + " receives " + receive + ", transmit " + transmit)
            receive_2[vm] = long(receive)
            transmit_2[vm] = long(transmit)
            # print (vm + " receive " ), receive_2[vm]
            # print (vm + " transmit "), transmit_2[vm]

with open('disk.log') as f:
    for line in f:
        words = line.split(" ")
        words = filter(None, words)
        if words[3] == "vda":
            read_2[words[0][:-1]] += long(words[6])
            write_2[words[0][:-1]] += long(words[10])


receive_2['vm-38-1'] = receive_2['vm-38-1'] - receive_1['vm-38-1']
receive_2['vm-38-2'] = receive_2['vm-38-2'] - receive_1['vm-38-2']
receive_2['vm-38-3'] = receive_2['vm-38-3'] - receive_1['vm-38-3']
receive_2['vm-38-4'] = receive_2['vm-38-4'] - receive_1['vm-38-4']
receive_2['vm-38-5'] = receive_2['vm-38-5'] - receive_1['vm-38-5']
transmit_2['vm-38-1'] = transmit_2['vm-38-1'] - transmit_1['vm-38-1']
transmit_2['vm-38-2'] = transmit_2['vm-38-2'] - transmit_1['vm-38-2']
transmit_2['vm-38-3'] = transmit_2['vm-38-3'] - transmit_1['vm-38-3']
transmit_2['vm-38-4'] = transmit_2['vm-38-4'] - transmit_1['vm-38-4']
transmit_2['vm-38-5'] = transmit_2['vm-38-5'] - transmit_1['vm-38-5']

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

print 'receive: ', receive_2
print 'transmit: ', transmit_2

print 'disk read: ', read_2
print 'disk write: ', write_2
