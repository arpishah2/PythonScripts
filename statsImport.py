from collections import OrderedDict
import operator
import sys, os,pdb
from os import path
from os import listdir
from os.path import isfile, join
import datetime
from datetime import date, time, timedelta
import time,pymongo
import logging
import re
import json
from pymongo import Connection
from pymongo.errors import ConnectionFailure
from bson.code import Code
from prettytable import PrettyTable
from sets import Set
import argparse
# argparse is a complete argument processing library

def main():


############################################### Adding Command line arguments ###################################################################
	parser = argparse.ArgumentParser()
	parser.add_argument('-et', help ="To speciy an entity type")
	parser.add_argument('-s', help ="To specify a source. Type Mongo / S3 . Note: Default source is Mongo")
	parser.add_argument('-db', help = "To specify database. Note: default is t1")
	parser.add_argument('-c', help ="To specify a collection.  Note: default is raw")
	parser.add_argument('-ls', help = "To specific link of source")
	parser.add_argument('-ds', help = "To specify domain of the source")
	parser.add_argument('-sdt', help = "To specify start date")
	parser.add_argument('-edt', help = "To specify end date")
	parser.add_argument('-cr', help = "To specify credentials")
	args = parser.parse_args()
	
	if args.et:
		args.et

############################################## Make a connection ################################################################################
        try:
                c = Connection(host="localhost", port=27017)
                print "Connected successfully"
        except ConnectionFailure, e:
                sys.stderr.write("Could not connect to MongoDB: %s" % e)
                sys.exit(1)

	
	#Get a Database handle to T1
	t1DBH = c["t1"]
	
    	# Demonstrate the db.connection property to retrieve a reference to the
	# Connection object should it go out of scope. In most cases, keeping a
	# reference to the Database object for the lifetime of your program should
	# be sufficient.
	
	assert t1DBH.connection == c
	print "Successfully set up a database handle to t1"

# setup collection to use
	t1CH = t1DBH["raw"]

############################################################## Input from user ######################################################################

# Get the different Entity types that we have in Mongo T1/Raw collection
# This can be taken as default if user input is not provided
	
	entLi =  t1CH.distinct("entityType")
	entList = json.dumps(entLi) # removes unicode represenattion u
	#print entList

# Take user input
	
	x = PrettyTable(["Parameter to be used","Value"])
	x.align["Parameter to be used"] = "l" # Left align first value
	x.padding_width = 1
	 

# User input - Location Source(S3/Mongo)
	
	if args.s:
		locSource = args.s
	else :
		locSource = "Mongo" 
	x.add_row(["Location Source",locSource])
	
	if locSource != "S3" and locSource != "Mongo" :
		print " Invalid input. Please try again"
		sys.exit(0)


# if the location source is mongo, then get db and collection from user

	if locSource == "Mongo":
		if args.db:
			dbName = args.db
		else :
			dbName = "t1"

		if args.c:
			collName = args.c
		else: 
			collName = "raw"
		
		x.add_row(["MongoDB",dbName])
		x.add_row(["Mongo collection",collName])
		t1DB = dbName
		t1CollName = collName


# User input source 
	
	if args.ls:
		source = args.ls
	elif args.ds:
		source = args.ds
		regex_source = ".*"+args.ds+".*"
	else:
		source = "."
		regex_source = ".*.*"

	x.add_row(["Source",source])

# User input - Entity Type

	if args.et:
		entType = args.et
	else:
		entType = entLi
	x.add_row(["Entity Type",entType])
	print entType

# User Input - Date Range (From and to)

	if args.sdt:
		dateFrom = args.sdt
	if args.edt:
		dateTo = args.edt
	#x.add_row("Date Range - From", dateFrom])


# Access Credential

	if args.cr:
		accCred = args.cr
	#accCred = raw_input("Please enter the access credentials: ")
	#x.add_row(["Access credentials", accCred])	
	
# Print the user input
	print x
	
##################################################################### Methods ###########################################################################
	
# get count of documents as per entity type
	def countOfEntityType(entType):
		queryByEntityType = {'entityType': entType}
                res = t1CH.find( queryByEntityType )
                entitiesCount = res.count()
                return entitiesCount
	
# get total no of documents in a collection
	def countTotalDocuments():
		queryTotalDocCount = t1CH.count()
		print "total no of entities in the collection is : ", queryTotalDocCount


# get count of attributes for different entity types
	def countAttrPerEntityType():
		reducer = Code( """ function(current,result){ var k; for(k in current){	if(k === 'entityType') continue; result.attributes[k] = (result.attributes[k] || 0) + 1;}}""")
		resultAttrCount = t1CH.group(key = {"entityType":1},condition = {},reduce = reducer,initial = { "attributes": {} })
		#print json.dumps(resultAttrCount,sort_keys=False,indent=4, separators=(',', ': '))
		print json.dumps(orderListOfJson(resultAttrCount),sort_keys = 4,indent =4, separators = (',',':'))

# report T1 attribute counts for a given entity type from a given harvest source
	def countAttrPerEntityTypePerHarvestSource(source,entityType):
		reducer1 = Code(""" function(current,result){ var k; for(k in current){if(k === 'entityType') continue; result.attributes[k] = (result.attributes[k] || 0) + 1;}}""")	
		if(regex_source):
			resultAttrCount1 = t1CH.group(key = {"entityType":1},condition = {'source': {"$regex" : regex_source}, 'entityType': entityType},reduce = reducer1,initial = { "attributes": {} })
		else :
			resultAttrCount1 = t1CH.group(key = {"entityType":1},condition = {'source':source, 'entityType':entityType},reduce = reducer1,initial = { "attributes": {} })
		#print resultAttrCount1
		print json.dumps(orderListOfJson(resultAttrCount1),sort_keys = 4,indent =4, separators = (',',':'))



	if locSource == "Mongo":
		print ""

# Method to convert an array of JSON/BSON objects in an order by value and then by key(if value is same)
	def orderListOfJson(listInp):
		new_list = []
		for obj in listInp:
			toOrder = obj["attributes"]
			toOrder = toOrder.items()
			toOrder.sort(cmp=cmpOrdering)
			# uncomment the 2 lines below -  to get an obj
			#toOrder = OrderedDict(toOrder)
			#toOrder = json.dumps(toOrder)
			obj['attributes'] = toOrder
			new_list.append(obj)
		return new_list

# Method for comaprision to order Json by key and value
	def cmpOrdering(x,y):
		if x[1] == y[1]: 
			if x[0] > y[0] : return 1
			elif x[0] < y[0] : return -1
			else:  return 1
		if x[1] > y[1]: return -1
		if x[1] < y[1]: return 1

#################################################################### Execution calls ########################################################################		

# check if entity type is a list or not
	#print type(entType)
	
	if type(entType) is list:
		print "Ent Type is a list"
		for eT in entType:
			print "\n \n \n "
			print eT, countOfEntityType(eT)
	else:
		print "Entity type is: ", entType
		print "\n\n Count of Ent type : ", countOfEntityType(entType)

	print "\n\n\n Count of Attribute per entity type : \n", countAttrPerEntityType()
	print "\n\n\n  : \n", countTotalDocuments()

	if args.ls or args.ds:
		if args.et: 
			print "\n\n\n Attribute for the given harvest souce and entity type: ", countAttrPerEntityTypePerHarvestSource(source,entType)
		else :
			"Please specify the ent Type"
		
	#l = t1CH.find({"source" : {"$regex" : regex_source}})
	#for m in l: print m,"\n"

if __name__ == "__main__":
    main()