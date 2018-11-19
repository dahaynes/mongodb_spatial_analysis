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
    import csv
    with fiona.open(myShpPath, 'r', crs=SRID) as theShp, open(r"c:\work\mongoErrors2.csv", 'w', newline='\n') as fout:
        
        if theCollection in mongoDB.list_collection_names():
            print("Found Existing collection with same name %s \n Dropping old collection" % (theCollection))
            mongoDB.drop_collection(theCollection)
        
        theCSV = csv.writer(fout)
        theCSV.writerow(['feature', 'id', 'type', 'issues'])
        mongoCollection  = db[theCollection]
        
        for f, feature in enumerate(theShp):
            featureType = feature['geometry']['type']
            print(featureType, len(feature['geometry']['coordinates']))
            if featureType == 'Polygon':
                featureCoordinates = feature['geometry']['coordinates']
                mongoDBCoordinates = [list(p) for polys in featureCoordinates for p in polys]
                mongoCollection.insert_one({'geom': {'type': featureType, 'coordinates': [mongoDBCoordinates] }, 'name': feature['properties']['NAME'] })
                #print("Loaded feature %s of %s" % (f+1, len(theShp)))
                theCSV.writerow([f, feature['properties']['ID'], featureType, 'None' ])
            
            else:
                multiPolygon = []
                for polygon in feature['geometry']['coordinates']:
                    
                    mongoDBCoordinates = [list(p) for polys in polygon for p in polys]
                    if mongoDBCoordinates[0][0] == mongoDBCoordinates[-1][0] and mongoDBCoordinates[0][1] == mongoDBCoordinates[-1][1]: 
                        issue = "closed polygon"
                    else:
                        issue = "open polygon"
                        mongoDBCoordinates.append(mongoDBCoordinates[0])
                    theCSV.writerow([f, feature['properties']['ID'], featureType, issue ])
                    mongoDBCoordinates = tuple(mongoDBCoordinates)
                    multipart = [mongoDBCoordinates]
                    multiPolygon.append(multipart)

                
                mongoCollection.insert_one({'geom': {'type': featureType, 'coordinates': multiPolygon }, 'name': feature['properties']['NAME'] })
                
            
            print("Loaded feature %s of %s" % (f+1, len(theShp)))
                
        #mongoCollection.create_index([('coordinates', pymongo.GEO2D)])
        mongoCollection.create_index([('geom', pymongo.GEOSPHERE)])
        #mongoCollection.create_index([('coordinates', pymongo.GEOSPHERE)])
            
        
        
            
myShpPath = r"C:\scidb\us_counties.shp" #r"C:\scidb\mn_county_boundaries.shp" #r"C:\scidb\shapefiles\4326\counties.shp"
directory, shapeFileName = os.path.split(myShpPath)
collectionName = shapeFileName.split('.')[0]

con, mongoDB = CreateMongoConnection()
LoadShapeFile(myShpPath, mongoDB, collectionName)

#mongoDB.list_collection_names()
