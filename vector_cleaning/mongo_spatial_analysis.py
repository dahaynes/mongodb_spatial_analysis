# -*- coding: utf-8 -*-
"""
Spyder Editor

This script is used to for performance tests on MongoDB (Vector)
"""

import fiona, os, csv
from pymongo import MongoClient, GEOSPHERE, HASHED
from pymongo.errors import WriteError
from collections import OrderedDict
import timeit



def CreateMongoConnection(host='localhost', port=27017):
    """
    host=['localhost:27017']
    """
    connString ="%s:%s" % (host, port)
    print(connString)
    connection = MongoClient(connString) #host
        
    return(connection)


def GetSpatialJoinFunction():
    """
    This function creates the spatial join function
    """
    
    myFunction =  r""" \
    var polyCursor = db.getCollection(polyCollection).find(); \
    var startTime = new Date().getTime(); \
    var resultsDict = {}; \
    while (polyCursor.hasNext()) { \
        var poly = polyCursor.next() \
        var results = db.getCollection(pointCollection).aggregate( \
        [ \
            { \
                "$match": { \
                    "geom": { \
                        $geoIntersects: { \
                            $geometry: poly.geom \
                        } \
                    } \
                } \
            }, \
            { \
                "$count": "num_points" \
            } \
        ]); \
        
        if (results._batch.length > 0){ \
            var points = results._batch[0].num_points; \
            //print(poly.place_name, points); \
            resultsDict[poly.place_name] = points; \
        } else { \
            //print(poly.NAME, 0); \
            resultsDict[poly.place_name] = 0; \
        } \
    } \
    var stopTime = new Date().getTime(); \
    print("Elapsed Time: ", stopTime-startTime); \
    return resultsDict;""" 
    return myFunction

def CreateSpatialJoinFunction(db):
    """
    """
    theFunction = GetSpatialJoinFunction()
    db.system.js.save({ "_id": "pointPolygonCount", "value": theFunction })
    db.eval("db.loadServerScripts()")

    return db



# ''' \n    var polyCursor = db.getCollection(polyCollection).find();\n    var startTime = new Date().getTime();\n    var resultsDict = {};\n    while (polyCursor.hasNext()) {\n        var poly = polyCursor.next()\n        var results = db.getCollection(pointCollection).aggregate(\n        [\n            {\n                "$match": {\n
#                   "geom": {\n                        $geoIntersects: {\n
#                 $geometry: poly.geom\n                        }\n
#  }\n                }\n            },\n            {\n                "$count": "num_points"\n            }\n        ]);\n        \n        if (results._batch.length >
# 0){\n            var points = results._batch[0].num_points;\n            //print(poly.place_name, points);\n            resultsDict[poly.place_name] = points;\n
# } else {\n            //print(poly.NAME, 0);        \n            resultsDict[poly.place_name] = 0;\n        }\n        \n    }\n    var stopTime = new Date().getTime();\n    print("Elapsed Time: ", stopTime-startTime);\n    return resultsDict;\n    '''

def PointPolygonJoin(pointDatasets, polygonDatasets):
    """
    This function generates the datasets needed for perfoming point in polygon spatial join
    """
    #OrderedDict( [( ("point", "%s_hash_%s" % (p, h)), ("polygon", poly) ) for p in pointDatasets for h in range(2,10,2) for poly in polygonDatasets ] )
    return [ OrderedDict([ ("point_table", "%s_hash_%s" % (p, h)), ("poly_table", poly) ]) for p in pointDatasets for h in range(2,10,2) for poly in polygonDatasets ]

def WriteFile(filePath, theDictionary):
    """
    This function writes out the dictionary as csv
    """
    
    thekeys = list(theDictionary.keys())
    
    with open(filePath, 'w', newline="\n") as csvFile:
        fields = list(theDictionary[thekeys[0]].keys())
        theWriter = csv.DictWriter(csvFile, fieldnames=fields)
        theWriter.writeheader()

        for k in theDictionary.keys():
            theWriter.writerow(theDictionary[k])

def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "Analysis Script for running Spatial Analytics on CitusDB")  
    parser.add_argument("-csv", required =False, help="Output timing results into CSV file", dest="csv", default=None)  

    parser.add_argument("-r", required =False, type=int, help="Number of runs", dest="runs", default=3)  
   
    #All of the required connection information
    parser.add_argument("--host", required=True, type=str, help="Host of database", dest="host")
    parser.add_argument("-d", required=True, type=str, help="Name of database", dest="db")
    parser.add_argument("-p", required=True, type=int, help="port number of citusDB", dest="port")   
    parser.add_argument("-u", required=True, type=str, help="db username", dest="user")

    subparser = parser.add_subparsers(dest="command")
    pointPolyJoin_subparser = subparser.add_parser('point_polygon_join')
    pointPolyJoin_subparser.set_defaults(func=PointPolygonJoin) #["random","synthetic"])
    
    pointPolyJoin_subparser.add_argument("--point", required=False, help="Table name of the point dataset", dest="point")
    pointPolyJoin_subparser.add_argument("--polygon", required=False, help="Table name of the polygon dataset", dest="polygon")

    centroid_subparser = subparser.add_parser('centroid')
    centroid_subparser.add_argument("--polygon", required=False, help="Table name of the polygon dataset", dest="polygon")
    # count_subparser = subparser.add_parser('count')
    # count_subparser.set_defaults(func=localDatasetPrep)
    
    # reclass_subparser = subparser.add_parser('reclassify')
    # reclass_subparser.set_defaults(func=localDatasetPrep)

    # focal_subparser = subparser.add_parser('focal')
    # focal_subparser.set_defaults(func=localDatasetPrep)

    # overlap_subparser = subparser.add_parser('overlap')
    # overlap_subparser.set_defaults(func=localDatasetPrep)

    # add_subparser = subparser.add_parser('add')
    # add_subparser.set_defaults(func=localDatasetPrep)

    return parser

if __name__ == '__main__':

    args, unknown = argument_parser().parse_known_args()

    mongoCon = CreateMongoConnection()
    theDB = mongoCon[args.db]
