# -*- coding: utf-8 -*-
"""

This script loads shapefiles into MongoDB
"""



#from osgeo import ogr
#import geopandas as gpd
#import psycopg2, shapely
import fiona, os
from shapely.geometry import *
from pymongo import MongoClient, GEOSPHERE, HASHED
from pymongo.errors import WriteError
import timeit





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
    
    
def CreateSpatialIndex(theCollection):
    """
    
    """
    
    theCollection.create_index([('geom', GEOSPHERE)])

def CreatGeoHashedIndex(theCollection, hashKey):
    """
    db.random10m_points_hashed.ensureIndex({HASH_2: "hashed"})
    """
    
    theCollection.create_index([(hashKey, HASHED)])

def CreateMonogPoint(theCoordinates):
    """
    
    { type: "Point", coordinates: [ 40, 5 ] }
    
    """
    return(list(theCoordinates))

def CreateMongoPolygon(theFeature, ):
    """
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


def LoadShapefile(inFile, collectionName, mongoPort, mongoDatabase="research", shardkey=None, srid=4326):
    """
    Function for loading a shapefile
    """
    mongoCon = CreateMongoConnection(host='localhost', port=mongoPort)
    mongoDB = mongoCon[mongoDatabase]
    shapeDir, shapeFileName = os.path.split(inFile)
    
    CreateMongoCollection(mongoDB, collectionName )
    mongoCollection  = mongoDB[collectionName]
    
    if shardkey:
        print("Creating Hashed Index on %s" % (shardkey))
        CreatGeoHashedIndex(mongoCollection, shardkey)
        databaseMongoCollectionName = "%s.%s" % ("research", mongoCollection)
        print("Sharding collection by key: %s" % (shardkey))
        #mongoDB.admin.c
        #This command is still failing
        adminDB = mongoCon.admin
        adminDB.command('enableSharding', mongoDatabase)
        #print(adminDB.command('listCommands'))
        adminDB.command({'shardCollection': databaseMongoCollectionName, 'key':{shardkey: "hashed"}})
        #usemongoDB.admin.command('shardCollection', databaseMongoCollectionName, key={shardkey: "hashed"})

    with fiona.open(fileIn, 'r', crs=srid) as theShp:
        
        
        mongoCollection  = mongoDB[collectionName]
        badGeoms = []
    
        for f, feature in enumerate(theShp):
            featureType = feature['geometry']['type']
            theFeaturePoints = feature['geometry']['coordinates']
    
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
                    insertData = True
            
            
            elif featureType == 'MultiPolygon':
                #This isn't the best way to make the shapely multipolygon
                listofPolygons  = [ Polygon(poly[0]) for poly in theFeaturePoints]
                theFeature = MultiPolygon(listofPolygons)
                
                mongoCoordinates = []
                for aPolygon in theFeature.geoms:
                    if aPolygon.is_valid and aPolygon.exterior.is_closed and aPolygon.exterior.is_valid:   
                        polygonCoordinates = CreateMongoPolygon(aPolygon)
                        mongoCoordinates.append(polygonCoordinates)
                        insertData = True
                        
            elif featureType == "Point":
                #{ type: "Point", coordinates: [ 40, 5 ] }
                mongoCoordinates = CreateMonogPoint(theFeaturePoints)
                insertData = True
                
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
                pass
            

            if insertData:
                
                mongoDocument = {'geom': {'type': featureType, 'coordinates': mongoCoordinates }  }
                mongoDocument.update(feature['properties'])
                
                mongoR = mongoCollection.insert_one(mongoDocument) #'properties': feature['properties'] feature['properties']['BLOCKID10']
                #print("Loaded %s %s of %s" % (featureType, feature['id'], len(theShp)))
                
                if len(theShp) %(f+1):
                    progress(f+1, len(theShp), status='loading')
                #mongoCollection.create_index( [("geom",GEOSPHERE)])
                del mongoCoordinates
                insertData = False
    
                
            else:        
                print("Feature id %s is not valid" % (feature['id']))
                badGeoms.append(feature['id'])
            
                
        
        print("Creating Spatial Index")
        CreateSpatialIndex(mongoCollection)
        
        if shardkey:
            print("Creating Hashed Index on %s" % (shardkey))
            CreatGeoHashedIndex(mongoCollection, shardkey)
            #"shardCollection may only be run against the admin database."
            #db.runCommand({shardCollection: "research.random10m_points_hashed",key:{HASH_2: "hashed"}})
        


def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "Module for loading geometry data into MongoDB")    
    
    parser.add_argument("-s", required=True, help="Input file path for the shapefile", dest="shapefilePath")    
    parser.add_argument("-p", required=True, type=int, help="port number of mongo", dest="port")   
    parser.add_argument("-c", required=False, type=str, help="Name of MongoDB collection", dest="collectionName")
    parser.add_argument("-f", required=False, type=str, help="Field Name for sharded collection", dest="shardKey")
    parser.add_argument("-d", required=False, type=str, help="Name of database", dest="db")

    return parser
        
if __name__ == '__main__':
    args = argument_parser().parse_args()
    start = timeit.default_timer()      
    
    fileIn = args.shapefilePath
    if not args.collectionName: 
        directory, shapeFileName = os.path.split(fileIn)
        collectionName = shapeFileName.split('.')[0]
        #LoadShapefile(fileIn, collectionName)
    else:
        collectionName = args.collectionName

    LoadShapefile(fileIn, collectionName, args.port, mongoDB=args.db, shardkey=args.shardKey)
           
            
    print("Finished")            
            
