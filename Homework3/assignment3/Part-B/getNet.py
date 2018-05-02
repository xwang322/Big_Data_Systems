from subprocess import call

receive_1 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0,'vm-38-5': 0}
transmit_1 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0,'vm-38-5': 0}

call(["pdsh -w ubuntu@vm-38-[1-5] 'cat /proc/net/dev' > net.log"], shell=True)

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
        # words = line.split(" ")
        # words = filter(None, words)
        # if words[3] == "sda" or words[3] == "sdb" or words[3] == "sdc":
        #     receive_1[words[0][:-1]] += long(words[6])
        #     transmit_1[words[0][:-1]] += long(words[10])

receive_2 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0,'vm-38-5': 0}
transmit_2 = {'vm-38-1': 0,'vm-38-2': 0,'vm-38-3': 0,'vm-38-4': 0,'vm-38-5': 0}

#########
call(["./run.sh"], shell=True)
print "running..."
#########

call(["pdsh -w ubuntu@vm-38-[1-5] 'cat /proc/net/dev' > net.log"], shell=True)

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

print 'receive: ', receive_2
print 'transmit: ', transmit_2
