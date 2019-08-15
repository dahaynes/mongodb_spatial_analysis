# -*- coding: utf-8 -*-
"""
Created on Wed Aug 14 20:19:53 2019

@author: david
"""




import fiona, os, csv
from pymongo import MongoClient, GEOSPHERE, HASHED
from pymongo.errors import WriteError
from collections import OrderedDict
import timeit, json, subprocess




def CreateMongoConnection(host='localhost', port=27017):
    """
    host=['localhost:27017']
    """
    connString ="%s:%s" % (host, port)
    print(connString)
    connection = MongoClient(connString) #host

        
    return(connection)

def CreateMongoCollection(theCon, collectionName ):
    """
    Input:
        Mongo Dabase Connection
        Collection Name
    """
    if collectionName in theCon.list_collection_names():
        print("Found Existing collection with same name %s \n Dropping old collection" % (collectionName))
        theCon.drop_collection(collectionName)
    
    
def CreateSpatialIndex(theCollection, geoIndex=GEOSPHERE):
    """
    This function create the Spatial Index  
    """
    print("Creating Geospatial Index: GEOSPHERE is default" )
    theCollection.create_index([('geom', geoIndex)])

def CreatGeoHashedIndex(theCollection, hashKey):
    """
    Creates a hashed index on a distributed dataset
    db.random10m_points_hashed.ensureIndex({HASH_2: "hashed"})
    """
    print("Creating Hashed Index on %s" % (hashKey))
    theCollection.create_index([(hashKey, HASHED)])
    
    
def MongoDBPrep(collectionName, mongoPort, mongoDatabase="research", shardkey=None):
    """
    Function for loading a shapefile
    """
    mCon = CreateMongoConnection(host='localhost', port=mongoPort)
    databaseName = str(mongoDatabase)
    mDB = mCon[databaseName]
    #Not sure if this is beigng used
    # shapeDir, shapeFileName = os.path.split(inFile)
    
    CreateMongoCollection(mDB, collectionName )
    mCollection  = mDB[collectionName]
    print("Created Collection")
    if shardkey:
        timeit.time.sleep(30)
        ShardCollection(mCon, databaseName, collectionName, shardkey, )

    return mDB, mCollection

def ShardCollection(mongoCon, databaseName, collectionName, shardkey, ):
    """

    """
    #print("Creating Hashed Index on %s" % (shardkey))
    #CreatGeoHashedIndex(mongoCollection, shardkey)
    databaseMongoCollectionName = "%s.%s" % ("research", collectionName)
    print("Sharding collection by key: %s" % (shardkey))
    #mongoDB.admin.c
    #This command is still failing
    adminDB = mongoCon['admin']
    print("*********** HERE ************", databaseName)
    adminDB.command('enableSharding', databaseName)
    #print(adminDB.command('listCommands'))
    print("""{%s: "%s"}, key: {%s: "hashed"} """ % ("shardCollection", databaseMongoCollectionName, shardkey ))

    try:
        print("Attempting to shard")
        adminDB.command({"shardCollection": databaseMongoCollectionName, "key":{shardkey: "hashed"}})
    except:
        timeit.time.sleep(10)
        adminDB.command({"shardCollection": databaseMongoCollectionName, "key":{shardkey: "hashed"}})

    # adminDB.command({"shardCollection": databaseMongoCollectionName, "key":{shardkey: "hashed"}})
    #usemongoDB.admin.command('shardCollection', databaseMongoCollectionName, key={shardkey: "hashed"})
    del adminDB   


def LoadJSON(mongoHost, mongoDB,mongoPort, mongoCollectionName, jsonFilePath):
    """
    mongoimport --db research --collection counties44 
    --file c:\git\mongodb_spatial_analysis\datasets\small_points4.json --jsonArray
    """
    
    mongoImportCommand = """mongoimport --host {} --db {} --port {} --collection {} --file {} --jsonArray""".format(mongoHost, mongoDB,mongoPort, mongoCollectionName, jsonFilePath)
    print(mongoImportCommand)
    p = subprocess.Popen(mongoImportCommand, shell=True)
    p.wait()
    out, err = p.communicate()


def WriteFile(filePath, theDictionary):
    """
    This function writes out the dictionary as csv
    """
    
    thekeys = list(theDictionary.keys())
    

    fields = thekeys #list(theDictionary[thekeys[0]].keys())
    theWriter = csv.DictWriter(filePath, fieldnames=fields)
    theWriter.writeheader()
    theWriter.writerow(theDictionary)

def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "Module for loading geometry data into MongoDB")    


    #Required Parameters for Databases
    parser.add_argument("--host", required=True, type=str, help="host location of the mongos instance", dest="host", default="localhost")   
    parser.add_argument("-p", required=True, type=int, help="port number of mongo", dest="port")   
    parser.add_argument("-d", required=True, type=str, help="Name of database", dest="db")

    parser.add_argument("-c", required=True, type=str, help="Name of MongoDB collection", dest="collectionName")
    parser.add_argument("-f", required=False, type=str, help="Field Name for sharded collection", dest="shardKey", default=None)

    parser.add_argument("-o", required=False, type=argparse.FileType('w'), help="The file path of the out csv", dest="csv", default=None)

    parser.add_argument("--json", required=True, type=str, help="Input file path for the json", dest="json") 

    return parser




if __name__ == '__main__':
    args, unknown = argument_parser().parse_known_args()
    print(args)
    #Creating connection
    mongoDB, mongoCollection = MongoDBPrep(args.collectionName, args.port, mongoDatabase=args.db, shardkey=args.shardKey)
    print("Loading Dataset")
    start = timeit.default_timer()      
    
    LoadJSON(args.host,args.db,args.port, args.collectionName, args.json)
             
    stopLoad = timeit.default_timer()      
    CreateSpatialIndex(mongoCollection)
    stopSpatialIndex = timeit.default_timer()      
    
    #Timing Dictionary
    times = OrderedDict([ ("connectionInfo", "Wrangler"), ("platform", "MongoDB"), ("dataset", args.collectionName), ("Loading_time", (stopLoad-start)-30), ("index_time", stopSpatialIndex-stopLoad) ])

    #If the shardkey exists, then we create the geoHash
    if args.shardKey:
        print("Distributing collection {}".format(mongoCollection))
        print("Creating Hashed Index on %s" % (args.shardKey))   
        CreatGeoHashedIndex(mongoCollection, args.shardKey) 
        stopGeoHash = timeit.default_timer()      
        d = {"distributed_hash_time": stopGeoHash-stopSpatialIndex}
        times.update(d)

    stop = timeit.default_timer()      
    print("All Processes have been completed: {:.2f} seconds".format(stop-start))
     
    if args.csv:
        WriteFile(args.csv, times)

    print("Finished") 
