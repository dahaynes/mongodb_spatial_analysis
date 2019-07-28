# -*- coding: utf-8 -*-
"""

This script loads shapefiles and csv(WKT) into MongoDB
"""



# from osgeo import ogr
# import geopandas as gpd
# import psycopg2, shapely
import fiona, os, csv
from shapely.geometry import *
from shapely.wkt import dumps, loads
from pymongo import MongoClient, GEOSPHERE, HASHED
from pymongo.errors import WriteError
from collections import OrderedDict
import timeit
csv.field_size_limit(100000000)


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


def CreateMongoPoint(theCoordinates):
    """
    
    { type: "Point", coordinates: [ 40, 5 ] }
    
    """
    return(list(theCoordinates))

def CreateMongoLine(coordinateList):
    """
    { type: "LineString", coordinates: [ [ 40, 5 ], [ 41, 6 ] ] }
    """

    return( [list(c) for c in coordinateList ])


def CreateMongoPolygon(theFeature):
    """
    This is how Mongo wants a polygon
    {
     type : "Polygon",
     coordinates : [
             [ [ 0 , 0 ] , [ 3 , 6 ] , [ 6 , 1 ] , [ 0 , 0 ] ], #exterior ring
             [ [ 2 , 2 ] , [ 3 , 3 ] , [ 4 , 2 ] , [ 2 , 2 ] ]  #interior ring
        ]
     }
    """
    
    arrayCoordinates = []
    dataset = [ list(pointpair) for pointpair in theFeature.exterior.coords ]
        #ringCoordinates = [list(pointPair) for ring in theFeaturePoints for pointPair in ring]            
    arrayCoordinates.append(dataset)
    #Adding any interior polygons    
    if list(theFeature.interiors):        
        for interiorRings in list(theFeature.interiors):
            rings = [ list(pointPair) for pointPair in interiorRings.coords ]
            arrayCoordinates.append(rings)
            
    return(arrayCoordinates)

def progress(count, total, status=''):
    """
    Stolen from 
    https://gist.github.com/vladignatyev/06860ec2040cb497f0f3
    """
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.1 * count / float(total), 1)
    bar = '=' * filled_len + '-' * (bar_len - filled_len)

    print('[%s] %s%s ...%s\r' % (bar, percents, '%', status))


def ReadCSV(mongoCollection, inFilePath, geomField="geom_text", delimiterChar=";"):
    """
    Function for reading in a large CSV
    geomField must be WKT
    """
    print("Reading CSV path %s and Loading records" % (inFilePath))
    badgeom = []
    with open(inFilePath, 'r', newline="\n") as fin:
        csvIn = csv.DictReader(fin, delimiter=delimiterChar)
        geoDocuments = []
        counter = 0
        for  rec in csvIn:
            theGeom = loads(rec[geomField])
            counter += 1
            del rec[geomField]
            # print(theGeom.type, rec[geomField])
            if theGeom.type == "Point":
                ### Loaded type is tuple. Convert to list and take the first item ###
                coordinates = CreateMongoPoint(list(theGeom.coords)[0] ) 
                # print(mongoCoordinates)
                if coordinates:
                    geoDocuments.append(CreateMongoGeoDocument(theGeom.type, coordinates, rec) )
                    #CreateMongoGeospatialDocument( mongoCollection, theGeom.type, coordinates, rec )
            elif theGeom.type == "LineString":
                print("Not finished")
                break
            elif theGeom.type == "MultiLineString":
                coordinates = ValidateMultiLineString(theGeom)
                # print(coordinates)
                CreateMongoGeospatialDocument( mongoCollection, theGeom.type, coordinates, rec)
            elif theGeom.type == "Polygon":
                coordinates = CreateMongoPolygon(theGeom)
                CreateMongoGeospatialDocument( mongoCollection, theGeom.type, coordinates, rec)

            elif theGeom.type == "MultiPolygon":
                # print("Number of polygons: ",len(theGeom.geoms))
                coordinates = ValidateMultiPolygon(theGeom)
                if coordinates:
                    CreateMongoGeospatialDocument( mongoCollection, theGeom.type, coordinates, rec )
            
            if counter == 1000:
                # print("inserting")
                # print(geoDocuments)
                mongoR = mongoCollection.insert_many(geoDocuments)
                geoDocuments = []
                counter = 0
                    
        #Insert the remaining records
        if counter:
            mongoR = mongoCollection.insert_many(geoDocuments)

    print("Loaded %s records into %s " % (mongoCollection.count(), mongoCollection.name))
            
    
        

         
        
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
    adminDB.command('enableSharding', databaseName)
    #print(adminDB.command('listCommands'))
    print("""{%s: "%s"}, key: {%s: "hashed"} """ % ("shardCollection", databaseMongoCollectionName, shardkey ))
    adminDB.command({"shardCollection": databaseMongoCollectionName, "key":{shardkey: "hashed"}})
    #usemongoDB.admin.command('shardCollection', databaseMongoCollectionName, key={shardkey: "hashed"})
    del adminDB   


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
    
    if shardkey:
        timeit.time.sleep(30)
        ShardCollection(mCon, databaseName, collectionName, shardkey, )

    return mDB, mCollection

def CreateMongoGeospatialDocument(mongoCollection, geospatialType, mongoCoordinates, featureAttributes):
    """

    """
    try:
        mongoDocument = {'geom': {'type': geospatialType, 'coordinates': mongoCoordinates }  }
        mongoDocument.update(featureAttributes)
        mongoR = mongoCollection.insert_one(mongoDocument)
        return 1
    except:
        print("****Error Inserting", mongoDocument)

def CreateMongoGeoDocument(geospatialType,mongoCoordinates,featureAttributes):
    """

    """
    mongoDocument = {'geom': {'type': geospatialType, 'coordinates': mongoCoordinates }  }
    mongoDocument.update(featureAttributes) 

    return mongoDocument



def ValidateMultiLineString(feature):
    """

    """
    mongoCoordinates = []
    for aLine in feature.geoms:
        lineCoordinates = CreateMongoLine(list(aLine.coords))
        mongoCoordinates.append(lineCoordinates)

    return mongoCoordinates

def ValidateMultiPolygon(feature):
    """

    """
    mongoCoordinates = []
    for aPolygon in feature.geoms:
        if aPolygon.is_valid and aPolygon.exterior.is_closed and aPolygon.exterior.is_valid:   
            polygonCoordinates = CreateMongoPolygon(aPolygon)
            mongoCoordinates.append(polygonCoordinates)

    return mongoCoordinates

def ReadShapefile(mongoCollection, inFilePath):
    """
    
    """
    badGeoms = []
    mongoCoordinates = []
    created = 0
    with fiona.open(inFilePath, 'r', crs=4326) as theShp:
    
        for f, feature in enumerate(theShp):
            featureType = feature['geometry']['type']
            theFeaturePoints = feature['geometry']['coordinates']
            print(featureType)
    
            if featureType ==  "Polygon":            
                if len(feature['geometry']['coordinates']) == 1:
                    #Single ring polygons
                    theFeature = Polygon(theFeaturePoints[0])
                else:
                    #Single ring polygons with interior rings
                    interiorRings = [inR for inR in feature['geometry']['coordinates'][1:] ]                    
                    theFeature = Polygon(theFeaturePoints[0], holes=interiorRings )
                
                if theFeature.is_valid and theFeature.exterior.is_closed and theFeature.exterior.is_valid:    
                    mongoCoordinates = CreateMongoPolygon(theFeature)
                
                if mongoCoordinates: created = CreateMongoGeospatialDocument( mongoCollection, featureType, mongoCoordinates, feature['properties'] )
            
            elif featureType == "MultiPolygon":
                #This isn't the best way to make the shapely multipolygon
                listofPolygons  = [ Polygon(poly[0]) for poly in theFeaturePoints]
                theFeature = MultiPolygon(listofPolygons)
                mongoCoordinates = ValidateMultiPolygon(theFeature)

                        
                if mongoCoordinates: created = CreateMongoGeospatialDocument( mongoCollection, featureType, mongoCoordinates, feature['properties'] )
            
            elif featureType == "LineString":
                # { type: "LineString", coordinates: [ [ 40, 5 ], [ 41, 6 ] ] }
                mongoCoordinates = CreateMongoLine(theFeaturePoints)
                # print(mongoCoordinates)
                if mongoCoordinates: created = CreateMongoGeospatialDocument( mongoCollection, featureType, mongoCoordinates, feature['properties'] ) 

            elif featureType == "MultiLineString":
                    #  {
                    #   type: "MultiLineString",
                    #   coordinates: [
                    #      [ [ -73.96943, 40.78519 ], [ -73.96082, 40.78095 ] ],
                    #      [ [ -73.96415, 40.79229 ], [ -73.95544, 40.78854 ] ],
                    #      [ [ -73.97162, 40.78205 ], [ -73.96374, 40.77715 ] ],
                    #      [ [ -73.97880, 40.77247 ], [ -73.97036, 40.76811 ] ]
                    #   ]
                    # }
                break

            elif featureType == "Point":
                #{ type: "Point", coordinates: [ 40, 5 ] }
                mongoCoordinates = CreateMongoPoint(theFeaturePoints)
                # print(mongoCoordinates)
                if mongoCoordinates: created = CreateMongoGeospatialDocument( mongoCollection, featureType, CreateMongoPoint(theFeaturePoints), feature['properties'] )
                
                
            elif featureType == "MultiPoint":
                #{
                #  type: "MultiPoint",
                #  coordinates: [
                #     [ -73.9580, 40.8003 ],
                #     [ -73.9498, 40.7968 ],
                #     [ -73.9737, 40.7648 ],
                #     [ -73.9814, 40.7681 ]
                #  ]
                #}
                break
            
            

            if created:                
                if not len(theShp) % (f+1):
                    # print(f+1, mongoDocument)
                    progress(f+1, len(theShp), status='loading')
                    print(mongoCollection.count())
                #mongoCollection.create_index( [("geom",GEOSPHERE)])
                del mongoCoordinates, created
                    
                
            else:        
                print("Feature id %s is not valid" % (feature['id']))
                badGeoms.append(feature['id'])
    
    print("Loaded %s records into %s " % (mongoCollection.count(), mongoCollection.name))
                
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
    
    # group = parser.add_mutually_exclusive_group(required=True)
    # group.add_argument('--distributed',action='store_true', dest="dist")
    # group.add_argument('--reference',action='store_false', dest="dist")

    #Required Parameters for Databases
    parser.add_argument("-host", required=True, type=str, help="host location of the mongos instance", dest="host", default="localhost")   
    parser.add_argument("-p", required=True, type=int, help="port number of mongo", dest="port")   
    parser.add_argument("-d", required=True, type=str, help="Name of database", dest="db")

    parser.add_argument("-c", required=True, type=str, help="Name of MongoDB collection", dest="collectionName")
    parser.add_argument("-f", required=False, type=str, help="Field Name for sharded collection", dest="shardKey", default=None)

    parser.add_argument("-o", required=False, type=argparse.FileType('w'), help="The file path of the csv", dest="csv", default=None)

    subparser = parser.add_subparsers(help='sub-command help', dest="command")
    #Adding sub parsers requires the order of the arguments to be in a particular pattern
    #Big parser arguments first, sub parser arguments second
    shapefileParser = subparser.add_parser('shapefile')
    
    shapefileParser.add_argument("--shp", required=True, help="Input file path for the shapefile", dest="shapefilePath")    

    csvParser = subparser.add_parser('csv')
    csvParser.add_argument("--txt", required=True, type=str, help="Input file path for the csv", dest="inCSV") 
    csvParser.add_argument("--delimiter", required=True, type=str, help=""" "Delimiter for CSV default = ";" """, dest="delimiter")
    csvParser.add_argument("--geom", required=True, help="The field name for the geometry text", dest="geom") 
    # csvParser.add_argument("--keyvalue", required=True, action='append', type=lambda kv: kv.split("="), dest='keyvalues') 
    return parser
        
if __name__ == '__main__':
    args, unknown = argument_parser().parse_known_args()
    #print(args)
    #Creating connection
    mongoDB, mongoCollection = MongoDBPrep(args.collectionName, args.port, mongoDatabase=args.db, shardkey=args.shardKey)
    start = timeit.default_timer()      
    
    if args.command == 'shapefile':
        ReadShapefile(mongoCollection, args.shapefilePath)
        
    elif args.command == 'csv':
        ReadCSV(mongoCollection, args.inCSV, args.geom, args.delimiter)            
    
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
            
