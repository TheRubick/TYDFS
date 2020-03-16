import multiprocessing
import zmq
import pandas as pd
import sys
import time
import numpy as np
import os

dkNum = int(sys.argv[1])
processNum = int(sys.argv[2])

def aliveSender():

    #defining the port of the pub sub connection
    port = "6000"

    #intiating context
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.connect("tcp://127.0.0.1:%s" % port)
    
    while True:
        socket.send_string(str(dkNum))
        time.sleep(1)    
            
def dataKeeper(port):
    context = zmq.Context()
    socket2 = context.socket(zmq.PUB)
    socket2.connect ("tcp://127.0.0.1:6100")

    while True:
        socket = context.socket(zmq.PAIR)
        socket.bind("tcp://127.0.0.1:%s" % port)
        print("before recieve")
        dic = socket.recv_pyobj()
        print("after recieve .........")
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

        socket.close()
        time.sleep(1) 
        print ("blaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")        

#watch Dog process is used to keep tracking the alive messages from the data keepers
aliveProcess = multiprocessing.Process(target=aliveSender,)

aliveProcess.start()

#intiate the rest of the processes of the datakeeper
dkProcesses = []
dkProcesses.append(aliveProcess)

port = 6020 + dkNum
for i in range(processNum):
    port = port + 1
    dkProcesses.append(multiprocessing.Process(target=dataKeeper,args=(str(port),)))
    dkProcesses[i+1].start()


aliveProcess.join()

for i in range(processNum+1):
    dkProcesses[i].join()
