import os
import sys
import zmq
import random

#intiate the client with specific id
clientID = int(sys.argv[1])
#assign the num of ports that the client would deal with "configurable"
numMasterPorts = int(sys.argv[2]) - 1 # - 1 as we wouldn't include the watchDog process
#file name
fileName = str(sys.argv[3])
#kind of operation download/upload i.e. 'd' would indicate download operation while 'u' would indicate upload operation
operationType = str(sys.argv[4])

#intiating socket context
context = zmq.Context()
socket = context.socket(zmq.REQ)

#intialize the port number of the Master to be connected to
portNum = random.randint(6001,6000+numMasterPorts)
socket.connect("tcp://127.0.0.1:%s" % str(portNum))
print("Sending request ")

#intialize data dictionary which have the file name and the operation type
data = {
    'id' : clientID,
    'fileName' : fileName,
    'opType' : operationType
}
#sendin the request
#@TODO try to make sure its is sent successfully from one real device to another
socket.send_json(data)

#@TODO should be modified
#  Get the dkID which is the ip:port
dkID = socket.recv_string()
print("data keeper would be "+dkID)

#intiate client data dictionary
clientData = {
    'id' : clientID,
    'fileName' : fileName
}





