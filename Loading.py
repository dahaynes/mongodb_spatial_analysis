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
        
        
        mongoCollection  = db[theCollection]
        
        for f, feature in enumerate(theShp):
            featureType = feature['geometry']['type']
            print(featureType, len(feature['geometry']['coordinates']))
            if featureType == 'Polygon':
                featureCoordinates = feature['geometry']['coordinates']
                mongoDBCoordinates = [list(p) for polys in featureCoordinates for p in polys]
        
                #fakeCoordinates = [[ [-96.50, 49.14 ], [-90.77, 48.46], [-91.00, 45.98], [-97.37, 46.00], [-96.50, 49.14 ] ]]
    
                
                mongoCollection.insert_one({'geom': {'type': featureType, 'coordinates': [mongoDBCoordinates] }} )
                print("Loaded feature %s of %s" % (f+1, len(theShp)))
                #break
                #return(mongoDBCoordinates, featureCoordinates)
                #break
            
            else:
                multiPolygon = []
                for polygon in feature['geometry']['coordinates']:
                    
                    mongoDBCoordinates = [list(p) for polys in polygon for p in polys]
                    multipart = [mongoDBCoordinates]
                    multiPolygon.append(multipart)
                
                mongoCollection.insert_one({'geom': {'type': featureType, 'coordinates': multiPolygon }} )
                print("Loaded feature %s of %s" % (f+1, len(theShp)))

                #return(feature, multiPolygon, mongoDBCoordinates)
                
        #mongoCollection.create_index([('coordinates', pymongo.GEO2D)])
        mongoCollection.create_index([('geom', pymongo.GEOSPHERE)])
        #mongoCollection.create_index([('coordinates', pymongo.GEOSPHERE)])
            
        
    
        
            
myShpPath = r"C:\scidb\us_states.shp" #r"C:\scidb\mn_county_boundaries.shp" #r"C:\scidb\shapefiles\4326\counties.shp"
directory, shapeFileName = os.path.split(myShpPath)
collectionName = shapeFileName.split('.')[0]

con, mongoDB = CreateMongoConnection()
LoadShapeFile(myShpPath, mongoDB, collectionName)

#mongoDB.list_collection_names()
