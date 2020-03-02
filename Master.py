import multiprocessing
import zmq
import pandas as pd
import sys
import time

'''
Configuration phase of the lookup table
'''
data = {
    'status' : [],
    'ipv4' : [],
    'port' : [],
    'TTL' : [] # time to live i.e. after period of time if the data keeper didn't respond this TTL would be changed
    #'file' : []
}

dkNum = int(sys.argv[1])
    #making M indices for the data keepers
    #data['index'] = list(range(1,m+1))

    #for loop to intialize the data in the look up table
for i in range(dkNum):
    data['status'].append("alive")
    data['ipv4'].append("tcp://127.0.0.1:")
    data['port'].append("5556")
    data['TTL'].append(0)

lookUpTable = pd.DataFrame(data=data)
print(lookUpTable)
print(type(lookUpTable))


def subPubTracker():

    #define variable counter to make checks on the data keepers who is alive and who is dead
    counter = 0
    #defining the port of the pub sub connection
    port = "5556"

    #intiating context
    context = zmq.Context()
    sockt = context.socket(zmq.SUB)
    sockt.bind("tcp://127.0.0.1:%s" % port)
    sockt.subscribe(topic="") # topic is empty string so the master accepts any string

    while True:
        
        '''
        to be modified on making lookup table but it is a temp logic
        '''
        
        dkNum = sockt.recv_string() # data keeper number
        #printing the data keeper number and check if it's not alive or not
        if(int(dkNum) == counter):
            print("Data Keeper # "+dkNum+" is a live")
        else:
            print("Data Keeper # "+dkNum+" isn't a live i.e. it is dead :( ")
        # delay for 1 second
        counter += 1
        counter %= dkNum
        time.sleep(1)

    '''
    @TODO
    in download phase , store the file name in lower case , use in operator to search in the file list
    '''

subPubProcess = multiprocessing.Process(target=subPubTracker)

subPubProcess.start()

subPubProcess.join()