import json
import os
import yaml
from Tkinter import *
import psutil


openFile_json = open("allOutput.txt")
data = yaml.load(openFile_json)


entire_msg = ""

def is_process_running(process_id, process_name,entire_msg):
    try:
        proc = psutil.Process(process_id) 
        proc_status = proc.status()

	if(proc.status == "stopped"):
		entire_msg = entire_msg +"\n" + process_name +" is NOT running"
	elif(proc_status == "zombie"):
		entire_msg = entire_msg +"\n" + process_name +" is NOT running"
	elif(proc_status == "sleeping"):
		entire_msg = entire_msg +"\n" + process_name +" is running"
	else:
 		entire_msg = entire_msg +"\n" + process_name +" is running"

    except psutil.NoSuchProcess:
        entire_msg = entire_msg+"\n"+ process_name + " is NOT running"

    return entire_msg
	

for d in data:
	print(d["pid"])
	entire_msg = is_process_running(d["pid"], d["name"], entire_msg)

# print(entire_msg)
master = Tk()
whatever_you_do = entire_msg
msg = Message(master, text = whatever_you_do)
msg.config(bg='lightgreen', font=('times', 24, 'italic'))
msg.pack( )
mainloop( )