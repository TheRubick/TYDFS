import zmq
import random
import sys
import time


port =  sys.argv[1]

context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.connect("tcp://127.0.0.1:%s" % port)


# receve from master 
while True:
	dic = socket.recv_pyobj()
	if dic["requestType"]=="replicate":
		socket2 = context.socket(zmq.PAIR)
		portForReplication = dic["port"]
		nameOfFile = dic["nameOfFile"]
		print (type(nameOfFile))
		if dic["type"] == "src":
			f = open(nameOfFile, "rb")
			video=f.read()
			dic2 = {"video":video}
			socket2.bind("tcp://127.0.0.1:%s" % portForReplication)
			socket2.send_pyobj(dic2)
			f.close()

		elif dic["type"] == "dst":
			socket.connect("tcp://127.0.0.1:%s" % portForReplication)
			dic2 = socket.recv_pyobj()
			video=dic2["video"]
			f = open(nameOfFile, "ab")
			f.write(video)
			f.close()



