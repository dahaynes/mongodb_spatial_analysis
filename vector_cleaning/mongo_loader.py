# -*- coding: utf-8 -*-
"""

This script loads shapefiles into MongoDB
"""



#from osgeo import ogr
import fiona, os
from shapely.geometry import *
from pymongo import MongoClient, GEOSPHERE


theShapeFilePath = r"c:\work\shapefiles\tabblock2010_06_pophu.shp" #r"C:\scidb\4326\tracts.shp" # r"C:\scidb\us_counties.shp"
shapeDir, shapeFileName = os.path.split(theShapeFilePath)
collectionName = shapeFileName.split('.')[0]


def CreateMongoConnection(host='localhost', port=27017, db='research'):
    """
    
    """
    connection = MongoClient(host, port)
    theDB = connection[db]
        
    return(connection, theDB)
    
def CreateIndex(theCollection):
    """
    
    """
    
    theCollection.create_index([('geom', GEOSPHERE)])
    
    
con, mongoDB = CreateMongoConnection()

with fiona.open(theShapeFilePath, 'r', crs=4326) as theShp:
    
    if collectionName in mongoDB.list_collection_names():
        print("Found Existing collection with same name %s \n Dropping old collection" % (collectionName))
        mongoDB.drop_collection(collectionName)
    
    mongoCollection  = mongoDB[collectionName]
            
    for f, feature in enumerate(theShp):
        featureType = feature['geometry']['type']
        #print(featureType, len(feature['geometry']['coordinates']))
        theFeaturePoints = feature['geometry']['coordinates']
        if featureType ==  "Polygon":
            theFeature = Polygon(theFeaturePoints[0])
        else:
            listofPolygons  = [ Polygon(poly[0]) for poly in theFeaturePoints]
            theFeature = MultiPolygon(listofPolygons)        
        
        if theFeature.is_valid and featureType == 'Polygon':
            mongoDBCoordinates = [list(pointPair) for ring in theFeaturePoints for pointPair in ring]
            mongoR = mongoCollection.insert_one({'geom': {'type': featureType, 'coordinates': [mongoDBCoordinates] }, 'name': feature['properties']['BLOCKID10'] })
            #print("Loaded %s %s of %s" % (featureType, feature['id'], len(theShp)))
            try:
                #CreateIndex(mongoCollection)
                mongoR = mongoCollection.create_index([('geom', GEOSPHERE)])
            except WriteError as e:
                print("removing")
                mongoCollection.delete_one({'name': feature['properties']['BLOCKID10']})
        
        else:        
            print("Feature id %s is not valid" % (feature['id']))
        

        
    
    #db.collection.createIndex( { "geom" : "2dsphere" } )
            
        
print("Finished")            
            
#vectorFile = ogr.Open(theShapeFilePath)
#layer = vectorFile.GetLayer()
#for feature in layer:
#    featureWKT = feature.geometry().ExportToWkt()
#    print(featureWKT)
#    
#    break