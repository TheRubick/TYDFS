import multiprocessing
import zmq
import pandas as pd
import sys
import time
import numpy as np
'''
Configuration phase of the lookup table
'''
#intiating the data of the look up table
dataLut = {
    'status'  : [], # status = alive or not alive
    'dkID'  : [], # data keeper ID = ipaddress:port
    'fileName'  : [], # file Name
    'filePath'  : [], # file Path
    'userID' : []

}

#intiating the processes the look up table
processLut = {
    'dkNum' : [], # data keeper number
    'status'  : [], # status = busy or idle
    'dkID'  : [], # data keeper ID = ipaddress:port
    'userID' : []

}

#intiating the lock for the critical section
lutLock = multiprocessing.Lock()

#intialize the dkNUm with the number of data keepers
dkNum = int(sys.argv[1])

#intialize the number of processes for each data keeper
dkProcessNum = int(sys.argv[2])

#intaite the port number of the first master process
trackerPort = "6000"

#for loop to intialize the data in the look up table
for i in range(dkNum):
    dataLut['status'].append("alive")
    #@TODO should be configured from conf.sh
    dataLut['dkID'].append("tcp://127.0.0.1:6000")
    dataLut['fileName'].append("--")
    dataLut['filePath'].append("--")
    dataLut['userID'].append("--")
    for j in range(dkProcessNum):
        processLut['dkNum'].append(i)
        processLut['status'].append("idle")
        processLut['dkID'].append("tcp://127.0.0.1:"+str(6000+j+1))
        processLut['userID'].append("--")

def watchDogFunc(sharedLUT,sharedProcess,lutLock):

    #intiating context
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind("tcp://127.0.0.1:%s" % trackerPort)
    socket.subscribe(topic="") # topic is empty string so the master accepts any string

    #intiate chance to live array to be used on making checks on the alive data keepers , maximum number of chance is 2
    CTL = np.zeros((dkNum)) 
    #intialize the set the would contain all the numbers of the data keepers
    dkSet = set()
    for i in range(dkNum):
        dkSet.add(i)
    
    #intialize the alive data keepers set
    aliveSet = set()
    #intialize the flag to intiate the start time "new period"
    startPeriod = True
    #make the start time variable
    global startTime
    #intialize temp dictionary to hold the new data frame
    global tempdf
    
    while True:

        if(startPeriod):
            #make the flag of start period with false
            startPeriod = False
            #intialize the startTime
            startTime = time.monotonic()
            #intialize temp dictionary to hold the new data frame
            tempdf = sharedLUT.df

        #recieve the num of the data keeper
        num = socket.recv_string()
        tempdf['status'][int(num)] = "alive"
        #in case if the data keeper was dead then waked up again
        CTL[int(num)] = 0
        #add the num of this data keeper to the aliveSet
        aliveSet.add(int(num))

        #get the current time
        currentTime = time.monotonic()
        #if the time between the start and current time is greater than 0.7 then check on the alive/dead data keepers
        if(currentTime - startTime > 0.7):
            #intialize the notAlive set
            notAlive = dkSet.difference(aliveSet)
            print(len(aliveSet))
            #loop on this set
            while(len(notAlive) != 0):
                deadDk = notAlive.pop()
                CTL[deadDk] = CTL[deadDk] + 1
                #if the chance of the keeper = 2 means it didn't send to the master after 2 seconds , so it will be dead
                if(CTL[deadDk] == 2): 
                    tempdf['status'][deadDk] = "dead"

            #make the flag of start period with true
            startPeriod = True
            #change the LUT shared between the processes
            lutLock.acquire()
            sharedLUT.df = tempdf
            print(sharedLUT.df)
            lutLock.release()

            #reset the aliveSet
            aliveSet = set()

            #end time would be used to sleep the remaining of the 1 second i.e. sleep remaining of the period
            endTime = time.monotonic()        
            print(endTime-startTime)
            #wait for the remaing of the 1 second
            if((endTime-startTime) <= 1):
                time.sleep(1-(endTime-startTime))
            
            print("----------------------------------------------------------------")
            
def MasterTracker(portNum,sharedLUT,sharedProcess,lutLock):
    #configuring the context of the socket
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://127.0.0.1:%s" % str(portNum))
    #dummy
    print(str(portNum)+"sdfsdfsdfsdf")
    while True:
        #recieving data from the client
        clientData = socket.recv_json()
        print("tracker port # "+str(portNum)+" recieved from client # = "+str(clientData['id']))
        #@TODO should be modified
        if(clientData['opType'] == "Upload"):
            for dk in range(dkNum):
                if(sharedLUT.df['status'][dk] == "alive"):
                    #sleep 2 seconds to make sure if it is still alive
                    time.sleep(2)
                    #check if the machine is still alive
                    if(sharedLUT.df['status'][dk] == "alive"):
                        #check on all the ports of the machine 
                        for port in range(dkProcessNum):
                            #on finding the 1st idle port , assign it to the client
                            if(sharedProcess.df['status'][port+dk] == "idle"):
                                #first change it to busy port
                                print(sharedProcess.df)
                                tempdf = sharedProcess.df
                                tempdf['status'][port+dk] = "busy"
                                tempdf['userID'][port+dk] = clientData['id']
                                lutLock.acquire()
                                sharedProcess.df = tempdf
                                lutLock.release()
                                print(sharedProcess.df)
                                #then send this ip:port to the client and break
                                socket.send_string(sharedProcess.df['dkID'][port])
                                break

        #@TODO should be modified
        elif(clientData['opType'] == "Download"):
            #dummy dkID
            socket.send_string("tcp://127.0.0.1:6000")
        else:
            socket.send_string("bad request !! , please check the parameters u have entered")
        time.sleep(1)
    

'''
@TODO
in download phase , store the file name in lower case , use in operator to search in the file list
'''
#make the lookUpTable data frame then assign it to the shared Memory "sharedLUT" 
sharedLUT = multiprocessing.Manager().Namespace()
sharedLUT.df = pd.DataFrame(dataLut)

#make table for checking the process status
sharedProcess = multiprocessing.Manager().Namespace()
sharedProcess.df = pd.DataFrame(processLut)

#@TODO remember to configure the number of the rest processes
#intiate the rest of the processes
masterProcesses = []
#watch Dog process is used to keep tracking the alive messages from the data keepers
watchDog = multiprocessing.Process(target=watchDogFunc,args=(sharedLUT,sharedProcess,lutLock,))

masterProcesses.append(watchDog)

masterProcesses[0].start()


#check if the number of the dk's processes is same as that of the master or not
for i in range(2):
    trackerPort = int(trackerPort) + 1
    masterProcesses.append(multiprocessing.Process(target=MasterTracker,args=(trackerPort,sharedLUT,sharedProcess,lutLock,)))
    masterProcesses[i+1].start()

for i in range(3):
    masterProcesses[i].join()
