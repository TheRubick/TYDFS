import os
import sys
import zmq
import random
import time

#intiate the client with specific id
clientID = int(sys.argv[1])
#assign the num of ports that the client would deal with "configurable"
numMasterPorts = int(sys.argv[2])
#file name
fileName = str(sys.argv[3])
#kind of operation download/upload i.e. 'd' would indicate download operation while 'u' would indicate upload operation
operationType = str(sys.argv[4])

#intiating socket context
context = zmq.Context()
socket = context.socket(zmq.REQ)

#intialize the port number of the Master to be connected to
portNum = random.randint(6001,6000+numMasterPorts)
socket.connect("tcp://127.0.0.244:%s" % str(portNum))
print("Sending request ")

if(operationType == "upload"):

    f = open(fileName, "rb")
    video=f.read()
    #intialize data dictionary which have the file name and the operation type
    dic={"requestType":"upload" }
    dic2={"requestType":"upload" ,"video":video,"clientId":clientID,"filename":fileName}


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

elif(operationType == "download"):
    path_to_download_to = "clientFolder/"
    request = {"requestType":"download", "arg" : fileName} # send the master the type of service i need plus the file i need to download in case of download
    socket.send_pyobj(request) #send my request to the master, dic of {"req type":"---", "file_name":"---"}
    reply = socket.recv_pyobj() # get my response from the master, dic of  ips having my file and the corresponding port of each ip puls the path where the file is stored at this ip, i.e {"ip :[port, map], ---}
    #ip-port = ''
    #path  = ''
    print("revieved from master")
    new_size = 1
    rec_size = -1
    check = True
    while(check):
        for  i in reply: # i is the ip and reply is the lst of both port and path
            path = reply[i]
            if rec_size == new_size:
                check = False
                break
            socket.close()
            socket = context.socket(zmq.PAIR)
            socket.connect(i)
            request = {"requestType":"download", "filepath" : path}
            socket.send_pyobj(request) # request for the file is sent
            start = time.time()
            if time.time() - start == 3:
                continue  
            print('Req to dk: %s is sent '%(i))
            msgg = socket.recv_pyobj() # file is recieved and to be saved
            print('Rep is recieved from dk : %s '%(i))
            destfile = path_to_download_to+fileName
            f = open(destfile, 'wb')
            f.write(msgg[0])
            rec_size = msgg[1]
            new_size = os.stat(destfile).st_size
            f.close()
            time.sleep(1)
            socket.close()
            print('client is finished')





