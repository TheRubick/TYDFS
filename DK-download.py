import zmq
import cv2
import os
import time
import pandas as pd
import sys

def dk_main( ip_as_pair, port_as_pair, ip_as_publisher, port_as_publisher ):
    context = zmq.Context()
    socket1 = context.socket(zmq.PAIR)
    socket1.bind("tcp://{}:{}".format(ip_as_pair, port_as_pair))
    socket2 = context.socket(zmq.PUB)
    socket2.bind("tcp://127.0.0.1:6100")
    i = 0
    while True:
        request = socket1.recv_pyobj()# request for downlaod or upload
        if  request["type"] == "download":
            curFile = request["path"]
            size = os.stat(curFile).st_size
            target = open(curFile, 'rb')
            file = target.read(size)
            msg = [file, size]
            if file:
                socket1.send_pyobj(msg)
                print ("file sent")
                target.close()
                time.sleep(1)
                dic = {
                    "requestType" : "notificationDownload"
                    }
                socket2.send_pyobj(dic)
                '''
                topic = 'availability' 
                socket2.send_string(topic, zmq.SNDMORE)
                socket2.send_string('available')
                print("dk announced availability")
                '''

ip1 =  sys.argv[1]
port1 = sys.argv[2]
ip2 = sys.argv[3]
port2 = sys.argv[4]
dk_main(ip1, port1,ip2, port2)