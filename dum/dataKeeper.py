import zmq
import sys
import time


port =  sys.argv[1] #idle port

context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.connect("tcp://127.0.0.1:%s" % port)


# receve from master 
while True:
	dic = socket.recv_pyobj()
	if dic["requestType"]=="replicate":
		
		
		nameOfFile = dic["nameOfFile"]
		if dic["type"] == "src":
			socket2 = context.socket(zmq.PUSH)
			f = open(nameOfFile, "rb")
			video=f.read()
			dic2 = {"video":video}
			socket2.bind("tcp://127.0.0.1:%s" % portForReplication)
			print ('will send now')
			socket2.send_pyobj(dic2)
			print ('sent')
			#ass = fass
			f.close()

		elif dic["type"] == "dst":
			socket2 = context.socket(zmq.PULL)
			socket2.connect("tcp://127.0.0.1:%s" % portForReplication)
			print ('will receve now')
			dic2 = socket2.recv_pyobj()
			print (dic2)
			print ('receved')
			video=dic2["video"]
			f = open(nameOfFile, "ab")
			f.write(video)
			f.close()



