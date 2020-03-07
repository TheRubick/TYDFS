import multiprocessing
import zmq
import pandas as pd
import sys
import time
import numpy as np
'''
Configuration phase of the lookup table
'''
#intiating the shared memory
status = multiprocessing.Manager().list()
ipv4 = multiprocessing.Manager().list()
port = multiprocessing.Manager().list()
filePath = multiprocessing.Manager().list()

#intiating the lock for the critical section
lutLock = multiprocessing.Lock()

dkNum = int(sys.argv[1])
    #making M indices for the data keepers
    #data['index'] = list(range(1,m+1))

    #for loop to intialize the data in the look up table
for i in range(dkNum):
    status.append("alive")
    ipv4.append("tcp://127.0.0.1:")
    port.append("5556")
    filePath.append("--") # no need to fill the dkNum first rows of the lookup table as there is no files yet 


def watchDogFunc(status,lutLock):

    #defining the port of the pub sub connection
    port = "5556"

    #intiating context
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind("tcp://127.0.0.1:%s" % port)
    socket.subscribe(topic="") # topic is empty string so the master accepts any string

    #intiate whoSent array to be used on making checks on the alive data keepers
    whoSent = np.zeros((dkNum)) 
    
    while True:

        start = time.monotonic()
        #check the alive
        for i in range(dkNum):
            num = socket.recv_string()
            print("dkNUM # "+num)
            whoSent[int(num)] = 1
            lutLock.acquire()
            status[i] = "alive"
            lutLock.release()
        
        #check the dead
        for i in range(dkNum):
            if(whoSent[i] == 0):
                print("dkNUM # "+str(i)+" didn't send")
                lutLock.acquire()
                status[i] = "dead"
                lutLock.release()
        
        #reset the whoSent list 
        whoSent = np.zeros(dkNum)
        
        end = time.monotonic()
        print("time taken = "+str(end - start))
        #wait for the remaing of the 1 second
        if((end-start) <= 1):
            time.sleep(1-(end-start))
        print("----------------------------------------------------------------")
        
            
    

'''
@TODO
in download phase , store the file name in lower case , use in operator to search in the file list
'''

#watch Dog process is used to keep tracking the alive messages from the data keepers
watchDog = multiprocessing.Process(target=watchDogFunc,args=(status,lutLock))

watchDog.start()

watchDog.join()