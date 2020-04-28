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
filepath="dk"+str(dkNum)+"Dir"
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
    
    while True:
        print("data keeper"+str(dkNum)+" reciever")
        context = zmq.Context()
        socket2 = context.socket(zmq.PUSH)
        socket2.connect ("tcp://"+masterIP+":6100")
        
        socket = context.socket(zmq.PAIR)
        dkProcessIP = "127.0.0."+str(dkNum+1)+":"+str(port)
        socket.bind("tcp://"+dkProcessIP)
       
        dic = socket.recv_pyobj()
        print("data process "+dkProcessIP+" has recieved .........")
        #print(dic)
        print("request type = "+dic["requestType"])
        if dic["requestType"]=="upload":
            video=dic["video"]
            #hn3ml save ll video fi path mo3ian
            

            completeName = os.path.join(filepath,dic["filename"])
            
            f = open(completeName, "wb")
            f.write(video)
            f.close()
            #notify masterrrrr   
            dic["requestType"]="notificationUpload"
            dic["filepath"]=filepath
            dic["isReplicate"] = False
            dic["dkNum"] = "--"
            socket2.send_pyobj(dic)
            
            socket.close()
            socket2.close() 

        elif dic["requestType"]=="download":
            curFile = filepath+"/"+dic["filepath"]
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
                    "requestType" : "notificationDownload",
                    "isReplicate" : False,
                    "dataKeeperport" : "tcp://"+dkProcessIP,
                    'fileName':"--",
                    'filePath':"--",
                    }
                dic["dkNum"] = "--"
                socket2.send_pyobj(dic)

            socket.close()
            socket2.close() 

        elif dic["requestType"]=="replicate":
            
            if dic["type"] == "src":                      # from function NotifyMachineDataTransfer in master
                
                print("dk has recved the socket and is going to replicate")
                
                f = open(filepath+"/"+dic["nameOfFile"], "rb")
                
                videoToBeSend = f.read()
                
                destData = {
                    "filename" : dic["nameOfFile"],
                    "requestType" : "replicate",
                    "video" : videoToBeSend,
                    "type" : "dst",
                    "dkSrcIP" : dkProcessIP,
                    "clientId" : "--"
                }
                print("machine to send = "+dic["machineToCopy"])
                socketReplicateSender = zmq.Context().socket(zmq.PAIR)
                socketReplicateSender.connect(dic["machineToCopy"])
                socketReplicateSender.send_pyobj(destData)
                print("data have been sent")
                #send to master that data have been replicated
                socket.send_string("")
                socketReplicateSender.close()
                socket.close()
                socket2.close() 

            elif dic["type"] == "dst":
                #print ('will receve now')
                #print ('receved')
                video=dic["video"]

                completeName = os.path.join(filepath,dic["filename"])
            
                f = open(completeName, "wb")
                f.write(video)
                f.close()

                dicSend = {
                    "requestType" : "notificationUpload",
                    "filepath" : filepath,
                    "filename" : dic["filename"],
                    "clientId" : dic["clientId"],
                    "dataKeeperport" : "tcp://"+str(dkProcessIP),
                    "isReplicate" : True,
                    "dkNum" : "--"
                }
                #print(dicSend["filename"])
                #socket3.close()
                print("inform the master that replicate is done")
                socket2.send_pyobj(dicSend)
                socket.close()
                socket2.close() 
            
        
        else:
            print("bad request")
            socket.close()
            socket2.close() 
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
