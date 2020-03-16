import pandas as pd
import zmq
import sys
import time

dataKeeper = ['dataKeeper1', 'dataKeeper2', 'dataKeeper3']      #delete
numOfReplicates = 2
#file = []                          #delete
file = ['file1.mp4', 'file2.mp4', 'file3.mp4', 'file4.mp4']  
#file = ['file1.txt', 'file2.txt', 'file3.txt', 'file4.txt']  
#dum look up table
# lookUpTable = {'dkID':[1, 3, 1, 1, 1, 2],
# 		'fileName':['file1.mp4', 'file1.mp4', 'file2.mp4', 'file3.mp4', 'file4.mp4', 'file4.mp4'],
# 		'filePath':['dataKeeper1', 'dataKeeper3', 'dataKeeper1', 'dataKeeper1', 'dataKeeper1', 'dataKeeper2'],
# 		'status':['yes', 'yes', 'yes', 'yes', 'yes', 'yes']}


lookUpTable = {'dkID':[1, 1, 1, 1, 2],
		'fileName':['file1.mp4', 'file2.mp4', 'file3.mp4', 'file4.mp4', 'file4.mp4'],
		'filePath':['dataKeeper1', 'dataKeeper1', 'dataKeeper1', 'dataKeeper1', 'dataKeeper2'],
		'status':['yes', 'yes', 'yes', 'yes', 'yes']}


# lookUpTable = {'dkID':[1, 1, 1, 1, 2],
# 		'fileName':['file1.txt', 'file2.txt', 'file3.txt', 'file4.txt', 'file4.txt'],
# 		'filePath':['dataKeeper1', 'dataKeeper1', 'dataKeeper1', 'dataKeeper1', 'dataKeeper2'],
# 		'status':['yes', 'yes', 'yes', 'yes', 'yes']}
df = pd.DataFrame(lookUpTable)          # convert look up table to dataframe





def getInstanceCount(nameOfFile):     # return how many copies of the file in the look up table
	alive = df[df.status == 'alive']    # new data frame of alive only
	cont = alive[alive.fileName == nameOfFile].fileName.count()   # get the count
	print ("count is : " + str(cont))
	return cont

def getSourceMachine(nameOfFile,sharedLUT):#dgfhgfhhgfh
	alive = sharedLUT.df[sharedLUT.df['status'] == 'alive']
	sub_df = alive[alive['fileName'] == nameOfFile]  # sub dataframe that contains only rows for that file
	data_list = sub_df.values.tolist()  # convert the dataframe to list
	print (data_list)
	srcMachine = data_list[0][0]    #get the first machine
	#path = data_list[0][2]      #get the path in the first machine
	#srcList = [srcMachine, path]     #store them in list and return it
	#return srcList
	print ("source machine is : " + str(srcMachine))
	return srcMachine           # for now returns only the num of the machine;';';';'
	#return the port function with it

def selectMachineToCopyTo(nameOfFile):#dgdgddgfgdf
	alive = df[df.status == 'alive']
	sub_df = alive[alive.fileName == nameOfFile]         #get the dataframe where the file in;';'';';;';';'
	dk_list = alive.dkID.values.tolist()          # will change to look at the first rows only  # convert to list
	#search for the machines where the file is not in
	machineNodes = sub_df.dkID.tolist()
	x = set(dk_list)
	y = set(machineNodes)
	z = x.difference(y)
	print (x)
	print (y)
	print (z)
	if z:
		selectedMacine = z.pop()
		print ("dst macine is : " + str(selectedMacine))
		return selectedMacine    #for now it returns the machine num only
	#return the port function with it


def NotifyMachineDataTransfer(sourceMachine, machineToCopy, nameOfFile):#fffhfghfhgffhgfhg
	port = 5556         # selecting port for the two machines to contact with  # will change to see which is available
	srcPort = 6002          # port to send to the src the req type  # will change to see which is available
	dstPort = 6001          # port to send to the dst the req type  # will change to see which is availablel';ll;l;l;ll;l;l;
	dic={"requestType":"replicate", "type":'src', "nameOfFile":nameOfFile, "port":port}
	dic2={"requestType":"replicate", "type":'dst', "nameOfFile":nameOfFile, "port":port}
	dk = 'dataKeeper'
	#srcMachine = dk + str(sourceMachine)
	dstMachine = dk + str(machineToCopy)

	# knowing the src and dist and thier ports :-
	context = zmq.Context()
	context2 = zmq.Context()
	socket = context.socket(zmq.PAIR)
	socket2 = context2.socket(zmq.PAIR)
	#sending to the src machine to send the file on port
	socket2.bind("tcp://127.0.0.1:%s" % srcPort)
	socket2.send_pyobj(dic)
	#sending to the dst machine to receve from src the file
	socket.bind("tcp://127.0.0.1:%s" % dstPort)
	socket.send_pyobj(dic2)

	df2 = {
	'dkID':machineToCopy,
	'fileName':nameOfFile,
	'filePath':dstMachine,
	'status':'yes'
	}


	#df = df.append(df2, ignore_index = True)
	time.sleep(0.5)  # to make sure the file had been copied





def Replicates():      
	
	while True:             # iguess this will not loop forever here and we will but it outside the function
		#time.sleep(5) 
		for k in range(len(file)):
			instanceCount = getInstanceCount(file[k])         # how many the file exsist in alive machines
			numOfReplicatesNeeded = numOfReplicates - instanceCount       # how many copies needed
			#print ("nim ....................." + numOfReplicatesNeeded)
			if (numOfReplicatesNeeded > 0) and (instanceCount != 0):
				sourceMachine = getSourceMachine(file[k])  # for now it is the num of the macine only
				for x in range(0,numOfReplicatesNeeded):
					machineToCopy = selectMachineToCopyTo(file[k])        #select machine depending on alive and not containing this file already
					NotifyMachineDataTransfer(sourceMachine, machineToCopy, file[k])      # send to the two machines to transfare the file

				


				



Replicates()
	








