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
    socket2 = context.socket(zmq.PUSH)
    socket2.connect ("tcp://"+masterIP+":6100")
    socket = context.socket(zmq.PAIR)
    dkProcessIP = "127.0.0."+str(dkNum+1)+":"+str(port)
    socket.bind("tcp://"+dkProcessIP)
    while True:
        
        print("before recieve")
        dic = socket.recv_pyobj()
        print("data process "+dkProcessIP+" has recieved .........")
        #print(dic)
        print("request type = "+dic["requestType"])
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
                "filename" : "replicateVideo"+recvData["nameOfFile"],
                "requestType" : "replicate",
                "video" : dic["video"],
                "type" : "dst",
                "dkSrcIP" : dkProcessIP,
                "clientId" : "--"
            }
            print("machine to send = "+recvData["machineToCopy"])
            socketReplicateInformer = zmq.Context().socket(zmq.PAIR)
            socketReplicateInformer.connect(recvData["machineToCopy"])
            socketReplicateInformer.send_pyobj(destData)
            print("data have been sent")
            #confirm to the Master that the file has been sent to the dest DK
            #dic["requestType"] = "notificationReplicaUpload"
            #socket2.send_pyobj(dic)
            print("machine to send = "+recvData["machineToCopy"])
            socketReplicateSender = zmq.Context().socket(zmq.PAIR)
            socketReplicateSender.connect(recvData["machineToCopy"])
            socketReplicateSender.send_pyobj(destData)
            
            socketReplicateInformer.close() 
            socketReplicateSender.close() 
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
            
            #nameOfFile = dic["nameOfFile"]
            #print("here we goooooooooooooooooooooooooooooooooooooooooooooooooooooooooo")
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
                #context = zmq.Context()
                #socket3 = context.socket(zmq.PAIR)
                #print(dic["dkSrcIP"])
                #socket3.connect("tcp://"+dic["dkSrcIP"])
                
                #dic2 = socket3.recv_pyobj()
                #print (dic2)
                print ('receved')
                video=dic["video"]
                f = open(dic["filename"], "ab")
                f.write(video)
                f.close()
                dicSend = {
                    "requestType" : "notificationUpload",
                    "filepath" : dic["filename"],
                    "filename" : dic["filename"],
                    "clientId" : dic["clientId"],
                    "dataKeeperport" : "tcp://"+str(dkProcessIP)
                }
                print(dicSend["filename"])
                #socket3.close()
                print("inform the master that replicate is done")
                context3 = zmq.Context()
                socket23 = context3.socket(zmq.PUSH)
                socket23.connect ("tcp://"+masterIP+":6100")
                socket23.send_pyobj(dicSend)
        else:
            print("bad request")

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
