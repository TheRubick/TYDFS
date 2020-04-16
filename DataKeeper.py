import multiprocessing
import zmq
import pandas as pd
import sys
import time
import numpy as np
import os

dkNum = int(sys.argv[1])
processNum = int(sys.argv[2])
masterIP = "127.0.0.244"
def aliveSender():

    #defining the port of the pub sub connection
    port = "6000"

    #intiating context
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.connect("tcp://"+masterIP+":%s" % port)
    
    while True:
        socket.send_string(str(dkNum))
        time.sleep(1)    
            
def dataKeeper(port):
    context = zmq.Context()
    socket2 = context.socket(zmq.PUB)
    socket2.connect ("tcp://"+masterIP+":6100")

    while True:
        socket = context.socket(zmq.PAIR)
        dkProcessIP = "127.0.0."+str(dkNum+1)+":"+str(port)
        socket.bind("tcp://"+dkProcessIP)
        print("before recieve")
        dic = socket.recv_pyobj()
        print("after recieve .........")
        #print(dic)
        print(dic["requestType"])
        if dic["requestType"]=="upload":
            video=dic["video"]
            #hn3ml save ll video fi path mo3ian
            filepath=dic["filename"]
            f = open(filepath, "ab")
            f.write(video)
            f.close()
            #notify masterrrrr   
            dic["requestType"]="notificationUpload"
            dic["filepath"]=filepath
            socket2.send_pyobj(dic) 

            #replicate phase
            socketReplicateDK = zmq.Context().socket(zmq.PAIR)
            socketReplicateDK.connect("tcp://127.0.0.244:"+str(6200+dkNum))
            recvData = socketReplicateDK.recv_pyobj()
            #make condition such that replication operation isn't needed
            print("dk has recved the socket and is going to replicate")
            destData = {
                "fileName" : recvData["nameOfFile"]+"replicateVideo",
                "requestType" : "replicate",
                "video" : dic["video"],
                "type" : "dst",
                "dkSrcIP" : dkProcessIP,
                "clientId" : "--"
            }
            print(recvData["machineToCopy"])
            socket.connect(recvData["machineToCopy"])
            socket.send_pyobj(destData)
            print("data have been sent")
            #confirm to the Master that the file has been sent to the dest DK
            #dic["requestType"] = "notificationReplicaUpload"
            #socket2.send_pyobj(dic) 
            socketReplicateDK.close()

        elif dic["requestType"]=="download":
            curFile = dic["filepath"]
            size = os.stat(curFile).st_size
            target = open(curFile, 'rb')
            file = target.read(size)
            msg = [file, size]
            if file:
                socket.send_pyobj(msg)
                print ("file sent")
                target.close()
                time.sleep(1)
                dic = {
                    "requestType" : "notificationDownload"
                    }
                socket2.send_pyobj(dic)

        elif dic["requestType"]=="replicate":
            
            nameOfFile = dic["nameOfFile"]
            print("here we goooooooooooooooooooooooooooooooooooooooooooooooooooooooooo")
            if dic["type"] == "src":                      # from function NotifyMachineDataTransfer in master
                socket2 = context.socket(zmq.PAIR)
                f = open(nameOfFile, "rb")
                video=f.read()
                dic2 = {
                    "video" : video,
                    "src" : dic["srcMachine"]
                    }
                socket2.bind(dic["machineToCopy"])
                print ('will send now')
                socket2.send_pyobj(dic2)
                print ('sent')
                f.close()

            elif dic["type"] == "dst":
                print ('will receve now')
                print ('will receve now')
                print ('will receve now')
                print ('will receve now')
                print ('will receve now')
                context = zmq.Context()
                socket3 = context.socket(zmq.PAIR)
                print(dic["dkSrcIP"])
                socket3.connect(dic["dkSrcIP"])
                
                dic2 = socket3.recv_pyobj()
                #print (dic2)
                print ('receved')
                video=dic2["video"]
                f = open(dic2["fileName"], "ab")
                f.write(video)
                f.close()
                dic["requestType"] = "notificationUpload"
                dic["filepath"] = dic2["fileName"]
                dic["fileName"] = dic2["fileName"]
                dic["clientId"] = dic2["clientId"]
                socket3.close()

                socket2.send_pyobj(dic) 

    socket.close()
    time.sleep(1) 
    print ("blaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")        

#watch Dog process is used to keep tracking the alive messages from the data keepers
aliveProcess = multiprocessing.Process(target=aliveSender,)

aliveProcess.start()

#intiate the rest of the processes of the datakeeper
dkProcesses = []
dkProcesses.append(aliveProcess)

port = 6020
for i in range(processNum):
    dkProcesses.append(multiprocessing.Process(target=dataKeeper,args=(str(port),)))
    dkProcesses[i+1].start()
    port = port + 1


aliveProcess.join()

for i in range(processNum+1):
    dkProcesses[i].join()
