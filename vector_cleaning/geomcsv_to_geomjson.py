# -*- coding: utf-8 -*-
"""

Converting geomCSV to JSON
"""

import fiona, os, csv
from shapely.geometry import *
from shapely.wkt import dumps, loads
from collections import OrderedDict
import timeit, json


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


def CreateMongoGeoDocument(geospatialType,mongoCoordinates,featureAttributes):
    """

    """
    mongoDocument = {'geom': {'type': geospatialType, 'coordinates': mongoCoordinates }  }
    mongoDocument.update(featureAttributes) 

    return mongoDocument

def ReadCSV(inFilePath, geomField="geom_text", delimiterChar=";", outJSONPath=None):
    """
    Function for reading in a large CSV
    geomField must be WKT
    """
    print("Reading CSV path %s and Writing JSON to %s " % (inFilePath, outJSONPath))
    badgeom = []
    
         

    with open(inFilePath, 'r', newline="\n") as fin:
        csvIn = csv.DictReader(fin, delimiter=delimiterChar)
        
        geoDocuments = []
        
        for rec in csvIn:
            theGeom = loads(rec[geomField])
            
            del rec[geomField]
            
            #print(theGeom.type) #, rec[geomField])
            if theGeom.type == "Point":
                ### Loaded type is tuple. Convert to list and take the first item ###
                coordinates = CreateMongoPoint(list(theGeom.coords)[0] ) 
                # print(mongoCoordinates)
                # if coordinates: geoDocuments.append(CreateMongoGeoDocument(theGeom.type, coordinates, rec) )
                    
            elif theGeom.type == "LineString":
                print("Not finished")
                break
            elif theGeom.type == "MultiLineString":
                coordinates = ValidateMultiLineString(theGeom)
                # print(coordinates)
                # CreateMongoGeospatialDocument( mongoCollection, theGeom.type, coordinates, rec)
            elif theGeom.type == "Polygon":
                coordinates = CreateMongoPolygon(theGeom)
                # CreateMongoGeospatialDocument( mongoCollection, theGeom.type, coordinates, rec)

            elif theGeom.type == "MultiPolygon":
                # print("Number of polygons: ",len(theGeom.geoms))
                coordinates = ValidateMultiPolygon(theGeom)
                # if coordinates: CreateMongoGeospatialDocument( mongoCollection, theGeom.type, coordinates, rec )
            
            if coordinates: geoDocuments.append(CreateMongoGeoDocument(theGeom.type, coordinates, rec) )
            
            if len(geoDocuments) == 1000:

                WriteJSON(outJSONPath, geoDocuments)

                geoDocuments = []

                    
        #Insert the remaining records
        if geoDocuments: WriteJSON(outJSONPath, geoDocuments)




            

def WriteJSON(jsonFilePath, geoDocs):
    """
    
    """
    
    if os.path.isfile(jsonFilePath):
        # File exists
        with open(jsonFilePath, 'a+') as outfile:
            outfile.seek(0, os.SEEK_END)
            outfile.seek(outfile.tell()-1, 0)
            outfile.truncate()
            
            for g in geoDocs:
                outfile.write(',\n')
                json.dump(g, outfile)
                
            outfile.write(']')
    else: 
        # Create file
        with open(jsonFilePath, 'w') as outfile:
    #            array = []
    #            array.append(geoDocs)
            json.dump(geoDocs, outfile)
            # print(json.dumps(geoDocs))


def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "Module for reading a geom csv and converting to geom json")    
    

    parser.add_argument("-i", required=True, help="The file path of the csv", dest="csv", default=None)
    parser.add_argument("-o", required=True, help="The file path of the out json", dest="json", default=None)
    parser.add_argument("--geom", required=True, help="The field name for the geometry text", dest="geom") 
    parser.add_argument("--delim", required=False, help="The field name for the geometry text", dest="delim", default=";") 

    return parser
        
if __name__ == '__main__':
    args, unknown = argument_parser().parse_known_args()
    if not args.json:
        outJson = "{}.json".format(args.csv.split(".")[0])
    else:
        outJson = args.json
    
    ReadCSV(args.csv, args.geom, args.delim, outJson)
    
    print("Finished")