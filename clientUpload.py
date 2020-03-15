import zmq
import sys
import random
import time
#read port numbers
port =  sys.argv[1]
port1 =  sys.argv[2]


client_id = random.randrange(1,10005)

context = zmq.Context()

socket = context.socket(zmq.REQ)

socket.connect ("tcp://127.0.0.1:%s" % port)
socket.connect ("tcp://127.0.0.1:%s" % port1)


listOfPorts=[]
listOfPorts.append(port)
listOfPorts.append(port1)






f = open("/home/nourahmed/miniProjectOs/fox.mp4", "rb")
video=f.read()

f2 = open("/home/nourahmed/miniProjectOs/arnb.mp4", "rb")
video2=f2.read()

dic={"requestType":"upload" }
dic2={"requestType":"upload" ,"video":video,"clientId":client_id,"masterPort":random.choice(listOfPorts),"filename":"fox1.mp4"}

dic3={"requestType":"upload" }
dic4={"requestType":"upload" ,"video":video2,"clientId":client_id,"masterPort":random.choice(listOfPorts),"filename":"arnb1.mp4"}


socket.send_pyobj(dic)
dataKeeperPort = socket.recv_pyobj()
print(dataKeeperPort)
dic2["dataKeeperport"]=dataKeeperPort
#here the client will connect to the data keeper to send the file to it
socket2 = context.socket(zmq.PAIR)
socket2.connect(dataKeeperPort)
socket2.send_pyobj(dic2)

time.sleep(3)
socket2.close()

socket.send_pyobj(dic3)
dataKeeperPort = socket.recv_pyobj()
print(dataKeeperPort)
dic4["dataKeeperport"]=dataKeeperPort
#here the client will connect to the data keeper to send the file to it
socket2 = context.socket(zmq.PAIR)
socket2.connect(dataKeeperPort)
socket2.send_pyobj(dic4)


time.sleep(20)
