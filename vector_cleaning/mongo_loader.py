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
    
    
def CreateSpatialIndex(theCollection):
    """
    
    """
    
    theCollection.create_index([('geom', GEOSPHERE)])
    
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






theShapeFilePath = r"C:\scidb\shapefiles\4326\tracts2.shp" #r"c:\work\shapefiles\tabblock2010_01_pophu.shp"  # r"C:\scidb\us_counties.shp"
theShapeFilePath = r"C:\aging\mn_facilities_2.shp"
shapeDir, shapeFileName = os.path.split(theShapeFilePath)
collectionName = shapeFileName.split('.')[0]



mongoCon, mongoDB = CreateMongoConnection()
with fiona.open(theShapeFilePath, 'r', crs=4326) as theShp:
    
    CreateMongoCollection(mongoCon, collectionName )
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
            mongoR = mongoCollection.insert_one({'geom': {'type': featureType, 'coordinates': mongoCoordinates }, 'name': feature['id'] }) #feature['properties']['BLOCKID10']
            #print("Loaded %s %s of %s" % (featureType, feature['id'], len(theShp)))
            
            if len(theShp) %(f+1):
                progress(f+1, len(theShp), status='loading')
            #mongoCollection.create_index( [("geom",GEOSPHERE)])
            del mongoCoordinates
            insertData = False

            
        else:        
            print("Feature id %s is not valid" % (feature['id']))
            badGeoms.append(feature['id'])
        
            #mongoR = mongoCollection.create_index([('geom', GEOSPHERE)]) 
    
    print("Creating index")
    CreateSpatialIndex(mongoCollection)
    #mongoCollection.create_index( [("geom",GEOSPHERE)])
            
        
print("Finished")            
            
#vectorFile = ogr.Open(theShapeFilePath)
#layer = vectorFile.GetLayer()
#for feature in layer:
#    featureWKT = feature.geometry().ExportToWkt()
#    print(featureWKT)
#    
#    break