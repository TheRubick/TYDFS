import zmq
import time
import sys
import random
import pandas as pd
import multiprocessing

#ht3rf el port mn el look up table
def getAvailablePortOfDataKeeperUpload(dataKeepersPortsNum,sharedLUT):
    dic=sharedLUT.to_dict()
    listOfAlive=[]
    for i in range(dataKeepersPortsNum):
        if dic['status'][i]=="alive":
            listOfAlive.append(i)
    print(dic['status'])
    index=random.choice(listOfAlive)
    print(index)
    return dic['dkID'][index]
            


def masterProcess(port,dataKeepersNum,sharedLUT):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://127.0.0.1:%s" % port)

    while True: 
        dic = socket.recv_pyobj()
        print(dic["requestType"])
        if dic["requestType"]=="upload":
            dataKeeperPort=getAvailablePortOfDataKeeperUpload(dataKeepersNum,sharedLUT)
            print(dataKeeperPort)
            socket.send_pyobj(dataKeeperPort)
        if dic["requestType"]=="download":
            continue
        


def subNotifications(port,sharedLUT):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind("tcp://127.0.0.1:%s" % port)
    socket.subscribe(topic="") 
    while True:
        dic = socket.recv_pyobj()
        if dic["requestType"]=="notificationUpload":
                #update look up table
                df2 = pd.DataFrame({"status":"alive", 
                                    'dkID':[dic["dataKeeperport"]],
                                    'fileName':[dic["filename"]],
                                    'filePath':[dic["filepath"]],
                                    'userID':[str(dic["clientId"])]}) 
                sharedLUT=sharedLUT.append(df2)
                print(sharedLUT)
                # send successful to the client




#////////////////////////////////////////////////////////////////////////////////////
dataLut = {
    'status'  : [], # status = alive or not alive
    'dkID'  : [], # data keeper ID = ipaddress:port
    'fileName'  : [], # file Name
    'filePath'  : [], # file Path
    'userID' : []

}


dataLut['status'].append("alive")
dataLut['dkID'].append("tcp://127.0.0.1:6000")
dataLut['fileName'].append("--")
dataLut['filePath'].append("--")
dataLut['userID'].append("--")  
    
dataLut['status'].append("alive")
dataLut['dkID'].append("tcp://127.0.0.1:6001")
dataLut['fileName'].append("--")
dataLut['filePath'].append("--")
dataLut['userID'].append("--")  


sharedLUT = pd.DataFrame(dataLut)
  
port =  sys.argv[1]
masterProcess(port,2,sharedLUT)

#subNotifications(port,sharedLUT)

