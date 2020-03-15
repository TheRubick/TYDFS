import multiprocessing
import zmq
import pandas as pd
import sys
import time
import numpy as np

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
            
def doSomeStaff(trackerPort):
    while True:
        pass

#watch Dog process is used to keep tracking the alive messages from the data keepers
aliveProcess = multiprocessing.Process(target=aliveSender,)

aliveProcess.start()

#intiate the rest of the processes of the datakeeper
dkProcesses = []
dkProcesses.append(aliveProcess)

trackerPort = 6000
for i in range(processNum):
    trackerPort = int(trackerPort) + 1
    dkProcesses.append(multiprocessing.Process(target=doSomeStaff,args=(trackerPort,)))
    dkProcesses[i+1].start()


aliveProcess.join()

for i in range(processNum+1):
    dkProcesses[i].join()
