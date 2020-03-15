import zmq
import time
def update_available_table(ip, port, sharedLUT):
    pass
def update_alive_table(ip, port,sharedLUT):
    pass

def update_available_alive(context, ip, port, sharedLUT):
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:port")
    socket.setsockopt(zmq.SUBSCRIBE, b'availability')
    while True:
        topic = socket1.recv_string()
        msg = socket1.recv_string()
        update_available_table(ip, port, sharedLUT)

def update_alive_dk(context, ip, port, sharedLUT):
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:port")
    socket.setsockopt(zmq.SUBSCRIBE, b'alive')
    while True:
        topic = socket.recv_string()
        msg = socket.recv_string()
        update_alive_table(ip, port, sharedLUT)

def get_Dks_having_file_download(filename, sharedLUT, sharedLUT):
    dic=sharedLUT.to_dict()
    dic2 = {}
    for i in dic ["fileName"]:
        if dic ["fileName"][i]== "rec.mp4":
            if dic ["status"][i] == "alive":
                dic2[dic ["dkID"][i] ]= dic["filePath"][i]
    return dic2      

def main_master(ip, port, sharedLUT):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    print('started getting requests from clients')
    socket.bind("tcp://ip:port")
    i = 0
    while True:
        
        request = socket.recv_pyobj()
        if request["type"] == "download":
            dks = get_Dks_having_file_download(request["arg"], sharedLUT)
            socket.send_pyobj(dks)
            print('master replied to client {}'.format(i))
        i+=1    
