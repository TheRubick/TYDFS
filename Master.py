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

#num of replicates
numOfReplicates = int(sys.argv[3])

#masterIP
masterIP = "127.0.0.244"

#intaite the port number of the first master process
trackerPort = "6000"

#for loop to intialize the data in the look up table
for i in range(dkNum):
    dataLut['status'].append("alive")
    #@TODO should be configured from conf.sh
    
    dkAddress = "tcp://127.0.0."+str(i+1)+":6000"
    dataLut['dkID'].append(dkAddress)
    dataLut['fileName'].append("--")
    dataLut['filePath'].append("--")
    dataLut['userID'].append("--")
    for j in range(dkProcessNum):
        processLut['dkNum'].append(i)
        processLut['status'].append("idle")
        #should be modified on configuration
        dkProcessIP = "tcp://127.0.0."+str(i+1)+":"+str(6020+j)
        processLut['dkID'].append(dkProcessIP)
        processLut['userID'].append("--")

def watchDogFunc(sharedLUT,sharedProcess,lutLock):

    #intiating context
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind("tcp://"+masterIP+":%s" % trackerPort)
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
                    #replicate the files that are in the dead machine
                    processOfDeadDk = sharedProcess.df[sharedProcess.df['dkNum'] == deadDk]
                    print(sharedProcess.df)
                    print(processOfDeadDk)
                    #for each one of those processes , if it stores a file ? then replicate it
                    fileToReplicate = []
                    for deadProcess in processOfDeadDk['dkID']:
                        for dks in range(sharedLUT.df.shape[0]):    
                            if(deadProcess == sharedLUT.df['dkID'][dks] and sharedLUT.df['fileName'][dks] != '--'):
                                fileToReplicate.append(sharedLUT.df['fileName'][dks])
                    print(fileToReplicate)
                    for fileTR in fileToReplicate:
                        Replicates("replicateDead",fileTR,sharedLUT,sharedProcess,"")

            #make the flag of start period with true
            startPeriod = True
            #change the LUT shared between the processes
            #print("-------sharedLUT before modify")
            #print(sharedLUT.df)
            #print("tempdf")
            #print(tempdf)
            lutLock.acquire()
            tempdf2 = sharedLUT.df
            #print("-------tempdf2----------")
            #print(tempdf2)
            for i in range(dkNum):
                #print(tempdf2['status'][i]+" "+tempdf2['dkID'][i]+" "+tempdf2['fileName'][i]+" "+tempdf2['filePath'][i]+" "+tempdf2['userID'][i])
                tempdf2['status'][i] = tempdf['status'][i]
                tempdf2['dkID'][i] = tempdf['dkID'][i]
                tempdf2['fileName'][i] = tempdf['fileName'][i]
                tempdf2['filePath'][i] = tempdf['filePath'][i]
                tempdf2['userID'][i] = tempdf['userID'][i]
                #print(tempdf2['status'][i]+" "+tempdf2['dkID'][i]+" "+tempdf2['fileName'][i]+" "+tempdf2['filePath'][i]+" "+tempdf2['userID'][i])

            #print("tempdf2 after modify")
            #print(tempdf2)
            sharedLUT.df = tempdf2
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

def getAvailablePortOfDataKeeperUpload(sharedLUT,sharedProcess,lutLock):
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
                            lutLock.acquire()
                            sharedProcess.df = tempdf
                            lutLock.release()
                            print(sharedProcess.df)
                            #then send this ip:port to the client and break
                            return (sharedProcess.df['dkID'][port])


def get_Dks_having_file_download(filename, sharedLUT,sharedProcess,lutLock):
    dic = sharedLUT.df
    dic2 = {}
    dic3 = sharedProcess.df
    print(dic)
    print("-------jjjjj-----------------")
    for i in dic ["fileName"]:
        print(i)
    print("------------------------------------------------")
    for i in range(sharedLUT.df.shape[0]):
        if dic ["fileName"][i] == filename and dic["fileName"][i] != "--":
            if dic ["status"][i] == "alive":
                for j in range(sharedProcess.df.shape[0]):
                    if(dic ["dkID"][i] == dic3["dkID"][j] and dic3["status"][j] == "idle"):
                        lutLock.acquire()
                        tempdf = sharedProcess.df
                        tempdf["status"][j] = "busy"
                        sharedProcess.df = tempdf
                        lutLock.release()
                        dic2[dic ["dkID"][i] ]= dic["filePath"][i]
    return dic2

def MasterTracker(portNum,sharedLUT,sharedProcess,lutLock):
    #configuring the context of the socket
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://"+masterIP+":%s" % str(portNum))

    while True: 
        clientData = socket.recv_pyobj()
        print(clientData["requestType"])
        if clientData["requestType"]=="upload":
            dataKeeperPort=getAvailablePortOfDataKeeperUpload(sharedLUT,sharedProcess,lutLock)
            print(dataKeeperPort)
            socket.send_pyobj(dataKeeperPort)
        #@TODO should be modified
        elif(clientData["requestType"] == "download"):
            dks = get_Dks_having_file_download(clientData["arg"], sharedLUT,sharedProcess,lutLock)
            #print(dks)
            #dks['requestType'] = "download"
            #print(dks)
            socket.send_pyobj(dks)
            print("master replied to client ")
        else:
            socket.send_string("bad request !! , please check the parameters u have entered")
        time.sleep(1)

'''
Replicas Phase
'''

def NotifyMachineDataTransfer(opType,sourceMachine, machineToCopy, nameOfFile, replicateSocket):#fffhfghfhgffhgfhg

    #in case of uploading file
    if(opType == "upload"):
        data = {
            "nameOfFile" : nameOfFile,
            "machineToCopy" : machineToCopy
        }
        replicateSocket.send_pyobj(data)
    #if some one is dead and we want to replicate its' files
    else:
        data = {
            "requestType" : "replicate",
            "nameOfFile" : nameOfFile,
            "machineToCopy" : machineToCopy,
            "type" : "src",
            "srcMachine" : sourceMachine
        }
        #intialize a socket
        socket = zmq.Context().socket(zmq.PAIR)
        socket.connect(sourceMachine)
        socket.close()


def getInstanceCount(nameOfFile,sharedLUT):
    
    alive = sharedLUT.df[sharedLUT.df.status == "alive"]
    alive = alive[alive.fileName == "nameOfFile"]
    count = alive.fileName.count()
    return count

def getSourceMachine(nameOfFile,sharedProcess,sharedLUT):
    alive = sharedLUT.df[sharedLUT.df.status == 'alive']
    sub_df = alive[alive.fileName == nameOfFile]  # sub dataframe that contains only rows for that file
    idleProcesses = sharedProcess.df[sharedProcess.df.status == "idle"]
    x = set(sub_df.dkID)
    y = set(idleProcesses.dkID)
    z = x and y
    return z.pop()


def selectMachineToCopyTo(nameOfFile,sharedProcess,sharedLUT):
    
    alive = sharedLUT.df[sharedLUT.df.status == 'alive']
    sub_df = alive[alive.fileName != nameOfFile]  # sub dataframe that contains only rows for that file
    idleProcesses = sharedProcess.df[sharedProcess.df.status == "idle"]
    x = set(sub_df.dkID)
    y = set(idleProcesses.dkID)
    z = x and y
    return z.pop()

def Replicates(opType,fileName,sharedLUT,sharedProcess,replicateSocket):
    instanceCount = getInstanceCount(fileName,sharedLUT)         # how many the file exsist in alive 
    machinesnumOfReplicatesNeeded = numOfReplicates - instanceCount       # how many copies needed
    if(numOfReplicatesNeeded > 0) and (instanceCount != 0):
        sourceMachine = getSourceMachine(fileName,sharedProcess,sharedLUT)  # for now it is the num of the macine only
        for x in range(0,numOfReplicatesNeeded):
            machineToCopy = selectMachineToCopyTo(fileName,sharedProcess.sharedLUT)
            NotifyMachineDataTransfer(opType,sourceMachine, machineToCopy, fileName,replicateSocket)      # send to the two machines to transfare the file


def subNotifications(sharedLUT,sharedProcess,lutLock):
    
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind("tcp://127.0.0.1:6100")
    socket.subscribe(topic="") 
    while True:
        dic = socket.recv_pyobj()
        if dic["requestType"]=="notificationUpload":
                #update look up table
                df2 = pd.DataFrame({"status":"--", 
                                    'dkID':[dic["dataKeeperport"]],
                                    'fileName':[dic["filename"]],
                                    'filePath':[dic["filepath"]],
                                    'userID':[str(dic["clientId"])]}) 
                print(sharedLUT)
                #change the look up table
                lutLock.acquire()
                tempdf = sharedLUT.df
                print("--------sfdsdfsfsdfdsfsdfds----------------")
                print(tempdf)
                tempdf = tempdf.append(df2,ignore_index=True)
                print(tempdf)
                print("--------sfdsdfsfsdfdsfsdfds99999999999999999999999----------------")
                sharedLUT.df=tempdf
                lutLock.release()

                print(sharedLUT)
                
                #change the process status from busy to idle
                
                tempdf = sharedProcess.df
                for p in range(dkProcessNum):
                    if(tempdf['dkID'][p] == df2['dkID'][p]):
                        
                        print("------------dobby dobby----------------------")
                        print(tempdf['dkID'][p])
                        print(tempdf['status'][p])
                        print(sharedProcess.df)
                        print("------------dobby dobby----------------------")
                        tempdf['status'][p] = "idle"
                        lutLock.acquire()        
                        sharedProcess.df=tempdf
                        lutLock.release()
                        print("------------sobby sobby----------------------")
                        print(sharedProcess.df)
                        print("------------sobby sobby----------------------")
                        #replicate this file , first make another socket to connect to the datakeeper port
                        replicateSocket = zmq.Context().socket(zmq.PAIR)
                        #modify this case
                        replicateSocket.bind("tcp://127.0.0.1:"+str(6200+tempdf['dkNum'][p]))
                        Replicates("upload",dic["filename"],sharedLUT,sharedProcess,replicateSocket)
                        replicateSocket.close()
                        #check in this break
                        break
                        
                    

        elif dic["requestType"]=="notificationDownload" or dic["requestType"]=="notificationReplicaUpload":
            tempdf = sharedProcess.df
            for p in range(dkProcessNum*dkNum):
                if(tempdf['dkID'][p] == df2['dkID'][p]):  
                    print("------------dobby dobby----------------------")
                    print(tempdf['dkID'][p])
                    print(tempdf['status'][p])
                    print(sharedProcess.df)
                    print("------------dobby dobby----------------------")
                    tempdf['status'][p] = "idle"
                    lutLock.acquire()
                    sharedProcess.df=tempdf
                    lutLock.release()
                    print("------------sobby sobby----------------------")
                    print(sharedProcess.df)
                    print("------------sobby sobby----------------------")
                    #check in this break
                    break




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

#watch Dog process is used to keep tracking the alive messages from the data keepers
subNotificationsProcess = multiprocessing.Process(target=subNotifications,args=(sharedLUT,sharedProcess,lutLock,))

masterProcesses.append(subNotificationsProcess)

masterProcesses[1].start()

trackerPort = 6000

#check if the number of the dk's processes is same as that of the master or not
for i in range(dkProcessNum):
    trackerPort = trackerPort + 1
    masterProcesses.append(multiprocessing.Process(target=MasterTracker,args=(trackerPort,sharedLUT,sharedProcess,lutLock,)))
    masterProcesses[i+2].start()

for i in range(dkProcessNum+2):
    masterProcesses[i].join()
