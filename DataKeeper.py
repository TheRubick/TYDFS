import multiprocessing
import zmq
import pandas as pd
import sys
import time
import numpy as np

num = int(sys.argv[1])

def aliveSender():

    #defining the port of the pub sub connection
    port = "6000"

    #intiating context
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.connect("tcp://127.0.0.1:%s" % port)
    
    while True:
        socket.send_string(str(num))
        time.sleep(1)    
            
    

#watch Dog process is used to keep tracking the alive messages from the data keepers
aliveProcess = multiprocessing.Process(target=aliveSender,)

aliveProcess.start()

aliveProcess.join()