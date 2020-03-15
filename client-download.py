def client_download_main(master_ip, master_port, path_to_download_to):

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://{}:{}".format(master_ip, master_port)) #connect to the master
    request = {"type":"download", "arg" : "hager"} # send the master the type of service i need plus the file i need to download in case of download
    socket.send_pyobj(request) #send my request to the master, dic of {"req type":"---", "file_name":"---"}
    reply = socket.recv_pyobj() # get my response from the master, dic of  ips having my file and the corresponding port of each ip puls the path where the file is stored at this ip, i.e {"ip :[port, map], ---}
    ip-port = ''
    path  = ''
    new_size = 1
    rec_size = -1
    check = True
    while(check):
        for  i in reply: # i is the ip and reply is the lst of both port and path
            ip-port = i 
            path = reply[i]
            if rec_size == new_size:
                check = False
                break;
            socket.close()
            socket = context.socket(zmq.PAIR)
            socket.connect('tcp://{}'.format(ip-port))
            request = {"type":"download", "path" : path}
            socket.send_pyobj(request) # request for the file is sent
            start = time.time()
            if time.time() - start == 3:
                continue  
            print('Req to dk: {} is sent '.format{ip-port})
            msgg = socket.recv_pyobj() # file is recieved and to be saved
            print('Rep is recieved from dk : {} '.format{ip-port})
            destfile = path_to_download_to
            f = open(destfile, 'wb')
            f.write(msgg[0])
            rec_size = msgg[1]
            new_size = os.stat(destfile).st_size
            f.close()
            time.sleep(1)
            socket.close()
            print('client is finished')
