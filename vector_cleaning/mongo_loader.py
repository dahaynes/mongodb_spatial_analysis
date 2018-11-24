# -*- coding: utf-8 -*-
"""

This script loads shapefiles into MongoDB
"""



#from osgeo import ogr
import fiona, os
from shapely.geometry import *
from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import WriteError
import geopandas as gpd
import psycopg2, shapely

theShapeFilePath = r"C:\scidb\4326\tracts2.shp" #r"c:\work\shapefiles\tabblock2010_01_pophu.shp"  # r"C:\scidb\us_counties.shp"
shapeDir, shapeFileName = os.path.split(theShapeFilePath)
collectionName = shapeFileName.split('.')[0]


def CreatPostgreSQLConnection(theHost='localhost', theDB='research', thePort=5432, theUser='david'):
    """
    
    """
    con = psycopg2.connect(host=theHost, database=theDB, user=theUser, port=thePort)
    cur = con.cursor()
    
    return(con,cur)

def CreateMongoConnection(host='localhost', port=27017, db='research'):
    """
    
    """
    connection = MongoClient(host, port)
    theDB = connection[db]
        
    return(connection, theDB)

def CreateMongoCollection(theCon, collectionName ):
    """
    Input:
        Mongo Dabase Connection
        Collection Name
    """
    if collectionName in mongoDB.list_collection_names():
        print("Found Existing collection with same name %s \n Dropping old collection" % (collectionName))
        mongoDB.drop_collection(collectionName)
    
    
def CreateIndex(theCollection):
    """
    
    """
    
    theCollection.create_index([('geom', GEOSPHERE)])
    
    






mongoCon, mongoDB = CreateMongoConnection()
#pgCon, pgCur = CreatPostgreSQLConnection()    
#df = gpd.GeoDataFrame.from_postgis("SELECT gid, namelsad00, geom2 FROM tracts2", pgCon, geom_col='geom2' )
    
with fiona.open(theShapeFilePath, 'r', crs=4326) as theShp:
    
    CreateMongoCollection(mongoCon, collectionName )
    mongoCollection  = mongoDB[collectionName]
   
#    for k, row in df.iterrows():
#        featureGeom = shapely.wkb.loads(row.geom2.wkb_hex,hex=True)
#        break
        

    for f, feature in enumerate(theShp):
        #theShp.validate_record_geometry(feature)
        featureType = feature['geometry']['type']
        #print(featureType, len(feature['geometry']['coordinates']))
        theFeaturePoints = feature['geometry']['coordinates']
        if featureType ==  "Polygon":
            #Single ring polygons
            #if len(feature['geometry']['coordinates']) == 1:
            theFeature = Polygon(theFeaturePoints[0])
        
        else:
            listofPolygons  = [ Polygon(poly[0]) for poly in theFeaturePoints]
            theFeature = MultiPolygon(listofPolygons)
            #print(featureType)
#        
##        # and theFeature.is_closed
##        featureGeom.type
##        featureGeom[0].exterior.coords
        if featureType == 'Polygon':
            if theFeature.is_valid and theFeature.exterior.is_closed and theFeature.exterior.is_valid:
                
                if len(feature['geometry']['coordinates']) == 1:
                    ringCoordinates = [list(pointPair) for ring in theFeaturePoints for pointPair in ring]
                    mongoDBCoordinates = [ringCoordinates]
                
                if len(feature['geometry']['coordinates']) > 1:
                    mongoDBCoordinates = []
                    for rings in theFeaturePoints:
                        coordinateArray = [ list(pointPair) for pointPair in rings] 
                        mongoDBCoordinates.append(coordinateArray)
                    
                insertData = True
            
        if featureType == 'MultiPolygon' and len(feature['geometry']['coordinates']) > 1:
            for aPolygon in theFeature.geoms:
                if aPolygon.is_valid and aPolygon.exterior.is_closed and aPolygon.exterior.is_valid:   
                    mongoDBCoordinates = []
                    for rings in theFeaturePoints:
                        coordinateArray = [ list(pointPair) for pointPair in rings] 
                        mongoDBCoordinates.append(coordinateArray)
                    insertData = True
                
                    
                
        if insertData:
            mongoR = mongoCollection.insert_one({'geom': {'type': featureType, 'coordinates': mongoDBCoordinates }, 'name': feature['id'] }) #feature['properties']['BLOCKID10']
            print("Loaded %s %s of %s" % (featureType, feature['id'], len(theShp)))
            mongoCollection.create_index([('geom', GEOSPHERE)])
#                try:
#                    #CreateIndex(mongoCollection)
                #mongoR = mongoCollection.create_index([('geom', GEOSPHERE)])
#                except WriteError as e:
#                    print("removing")
#                    mongoCollection.delete_one({'name': feature['properties']['BLOCKID10']})
            
        else:        
            print("Feature id %s is not valid" % (feature['id']))
            
        
        if f == 3000:
            break
            #mongoR = mongoCollection.create_index([('geom', GEOSPHERE)]) 
        
    
    mongoCollection.create_index( [("geom",GEOSPHERE)])
            
        
print("Finished")            
            
#vectorFile = ogr.Open(theShapeFilePath)
#layer = vectorFile.GetLayer()
#for feature in layer:
#    featureWKT = feature.geometry().ExportToWkt()
#    print(featureWKT)
#    
#    break