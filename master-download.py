import zmq
import time
import pandas as pd
import sys
def update_available_table(ip, port, sharedLUT):
    pass
def update_alive_table(ip, port,sharedLUT):
    pass

def update_available_alive(ip, port, sharedLUT):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://%s:%s"%(ip,port))
    socket.setsockopt(zmq.SUBSCRIBE, b'availability')
    while True:
        topic = socket.recv_string()
        msg = socket.recv_string()
        update_available_table(ip, port, sharedLUT)
        print("avialbale updated in master")

def update_alive_dk( ip, port, sharedLUT):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:port")
    socket.setsockopt(zmq.SUBSCRIBE, b'alive')
    while True:
        topic = socket.recv_string()
        msg = socket.recv_string()
        update_alive_table(ip, port, sharedLUT)
        

def get_Dks_having_file_download(filename, sharedLUT):
    dic=sharedLUT.to_dict()
    dic2 = {}
    for i in dic ["fileName"]:
        if dic ["fileName"][i]== filename:
            if dic ["status"][i] == "alive": 
                dic2[dic ["dkID"][i] ]= dic["filePath"][i]
    return dic2
          

def main_master(ip, port, sharedLUT):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    print('started getting requests from clients')
    socket.bind('tcp://%s:%s'%(ip,port))
    i = 0
    while True:
        request = socket.recv_pyobj()
        if request["type"] == "download":
            dks = get_Dks_having_file_download(request["arg"], sharedLUT)
            socket.send_pyobj(dks)
            print('master replied to client %s'%i)
        i+=1    
dataLut = {
    'status'  : [], # status = alive or not alive
    'dkID'  : [], # data keeper ID = ipaddress:port
    'fileName'  : [], # file Name
    'filePath'  : [], # file Path
    'userID' : []

}


dataLut['status'].append("alive")
dataLut['dkID'].append("tcp://127.0.0.1:6002")
dataLut['fileName'].append("newtest1.mp4")
dataLut['filePath'].append("/home/hager/Desktop/labsfinal/lab5/newtest1.mp4")
dataLut['userID'].append("--")  
    
dataLut['status'].append("dead")
dataLut['dkID'].append("tcp://127.0.0.1:6001")
dataLut['fileName'].append("--")
dataLut['filePath'].append("--")
dataLut['userID'].append("--")  
sharedLUT = pd.DataFrame(dataLut)
  
ip =  sys.argv[1]
port = sys.argv[2]
ip2 = sys.argv[3]
port2 = sys.argv[4]
#update_available_alive(ip2,port2,sharedLUT)
main_master(ip, port, sharedLUT)
