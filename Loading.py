# -*- coding: utf-8 -*-
"""
Created on Thu Nov  8 12:23:09 2018

@author: dahaynes
"""

import pymongo
from pymongo import MongoClient
import fiona, os



def CreateMongoConnection(host='localhost', port=27017, db='research'):
    """
    
    """
    connection = MongoClient(host, port)
    theDB = connection[db]
        
    return(connection, theDB)




def LoadShapeFile(theShpPath, db, theCollection, SRID=4326):
    """

    """    
    with fiona.open(myShpPath, 'r', crs=SRID) as theShp:
        if theCollection in mongoDB.list_collection_names():
            print("Found Existing collection with same name %s \n Dropping old collection" % (theCollection))
            mongoDB.drop_collection(theCollection)
        
        print()
        mongoCollection  = db[theCollection]
        for f, feature in enumerate(theShp):
            featureType = feature['geometry']['type']
            print(featureType)
            if featureType == 'Polygon':
                featureCoordinates = feature['geometry']['coordinates']
                mongoDBCoordinates = [list(p) for polys in featureCoordinates for p in polys]
        #.replace("(","[").replace(")", "]")
    
                featureDict = {'type': featureType, 'coordinates': mongoDBCoordinates, 'id': feature['properties']['ID'], 'name': feature['properties']['NAME']}
                mongoCollection.insert_one(featureDict)
                print("Loaded feature %s of %s" % (f, len(theShp)))
                #return(mongoDBCoordinates, featureCoordinates)
                #break
            
            else:
                multiPolygon = []
                for polygon in feature['geometry']['coordinates']:
                    
                    mongoDBCoordinates = [list(p) for polys in polygon for p in polys]
                    multiPolygon.append(mongoDBCoordinates)
                
                #return(feature, multiPolygon, mongoDBCoordinates)
                
        mongoCollection.create_index([('coordinates', pymongo.GEO2D)])
            
        
    
        
            
myShpPath = r"C:\scidb\shapefiles\4326\counties.shp"
directory, shapeFileName = os.path.split(myShpPath)
collectionName = shapeFileName.split('.')[0]

con, mongoDB = CreateMongoConnection()
LoadShapeFile(myShpPath, mongoDB, collectionName)

#mongoDB.list_collection_names()
