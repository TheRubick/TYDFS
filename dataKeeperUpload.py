import zmq
import random
import sys
import time




def dataKeeper(port):
    context = zmq.Context()
    

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
            socket2 = context.socket(zmq.REQ)
            socket2.connect ("tcp://127.0.0.1:%s" % dic["masterPort"])
            dic["requestType"]="notificationUpload"
            dic["filepath"]=filepath
            socket2.send_pyobj(dic)
            done = socket2.recv_pyobj()
            
            print(done)
        socket.close()
        print ("blaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        
        
    
port =  sys.argv[1]
dataKeeper(port)
    
    
