import multiprocessing
import os
import time
from tdemodules.labeler import * 
from tdemodules.entity_builder import *
from tdemodules.util.queue_empty import *
from tdemodules.util.queue_empty import *
from tdemodules.child_monitor import *
from tdemodules.missed_syns_test import *
from tdemodules.missed_syns import *
from datetime import datetime
import pdb
import json

class myProcess (multiprocessing.Process):
    def __init__(self, procId, name, classInstance, filename, genOutput):
        self.filename = "output"+str(procId)+".txt"
	self.genOutput = genOutput
	multiprocessing.Process.__init__(self)
	self.daemon = True
        self.procId = procId
        self.name = name
        self.classInstance = classInstance
	self.start_time1 =str(datetime.now())


    def run(self):
	self.start_time = time.time()
	print( "\n ")
        print "Starting " + str(self.pid)
       	print run_in_parallel(self.pid,self.name, self.classInstance,self.filename, self.genOutput)
        print "Exiting " + str(self.pid)

def run_in_parallel(procId, procName, instance, fname, genOutput):
	print "Executing Process "+str(procId)+ " ->"+procName
	
	if type(instance) == str:
		pdb.set_trace()

	dataz = '['+json.dumps({'Process ID ':str(procId), 'Module Name ':procName, 'Start time ':str(datetime.now())})+']'
	#print("data to send", dataz)


        instance.start()
	return dataz


if __name__ =='__main__':
	#qAgent_routes_obj = ConnectionVariables()
	labeler_obj = Labeler()
	eBuilder_obj = EntityBuilder(config='entity_builder')
	qEmpty_obj = QueueEmptier(config='parent_queue_empty')
	childMonitor_obj = ChildMonitor()
	missedsyns_obj = MissedSyns(config='missed_synonyms')
	#missedsynsTest_obj = MissedSynsTest(config='missed_synonyms_test')

	global filenameAll 
	filenameAll = "allOutput.txt"
	fl = open(filenameAll,"w")
	myProcesses = []
	myProcesses.append(myProcess(1, "1st Labeler", labeler_obj, "",fl))
	myProcesses.append(myProcess(2, "1st EntityBuilder", eBuilder_obj, "",fl))
	myProcesses.append(myProcess(3, "Queue Empty",qEmpty_obj, "", fl))
	myProcesses.append(myProcess(4, "Child Monitor", childMonitor_obj, "",fl))
	myProcesses.append(myProcess(5, "Missed Synonyms",missedsyns_obj, "",fl)  )                  


	for proc in myProcesses:
		proc.start()
	proc_list = []
	for proc in myProcesses:
		proc_dict = {}
		proc_dict['pid'] = proc.pid
		proc_dict['name'] = proc.name
		proc_dict['start_time'] = proc.start_time1
		print(proc_dict)
		proc_list.append(proc_dict)

	fl.write(json.dumps(proc_list, indent=5, sort_keys=True))
	fl.close()
	for proc in myProcesses:
		proc.join()


	time.sleep(1)
	print("Exiting main")