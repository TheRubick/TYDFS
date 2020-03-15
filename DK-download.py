import zmq
import cv2
import os
import time

def dk_main(context, ip_as_pair, port_as_pair, ip_as_publisher, port_as_publisher ):
    socket1 = context.socket(zmq.PAIR)
    socket1.bind("tcp://{}:{}".format(ip_as_pair, port_as_pair))
    socket2 = context.socket(zmq.PUB)
    socket2.bind("tcp://{}:{}".format(ip_as_publisher, port_as_publisher))
    i = 0
    while True:

        request = socket.recv_pyobj()# request for downlaod or upload
        if  request["type"] == "download":
            curFile = request["path"]
            target = open(curFile, 'rb')
            file = target.read(size)
            msg = [file, size]
            if file:
                socket1.send_pyobj(msg)
                print ("file sent")
                time.sleep(1)
                topic = 'availability' 
                socket2.send_string(topic, zmq.SNDMORE)
                socket2.send_string('available')
    
