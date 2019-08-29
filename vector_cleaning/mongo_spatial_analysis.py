# -*- coding: utf-8 -*-
"""
Spyder Editor

This script is used to for performance tests on MongoDB (Vector)
"""

import fiona, os, csv, subprocess
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


def LoadMongoFunctions(mdbCon):
    """
    JavaScript function has to exist previously
    db.eval("db.loadServerScripts()")
    """

    mdbCon.eval("db.loadServerScripts()")


def LoadFunctions():
    """
    JavaScript function has to exist previously
    db.eval("db.loadServerScripts()")
    """

    return ["use research;", "db.loadServerScripts();"]


def PointPolygonQuery(polygonDataset, pointDataset):
    """
    pointPolygonCount("states","synthetic_1_hash_2")
    """
    return """var k = pointPolygonCount("{}","{}"); """.format(polygonDataset, pointDataset)


def PointPolygonJoin(pointDatasets, polygonDatasets):
    """
    This function generates the datasets needed for perfoming point in polygon spatial join
    """
    #OrderedDict( [( ("point", "%s_hash_%s" % (p, h)), ("polygon", poly) ) for p in pointDatasets for h in range(2,10,2) for poly in polygonDatasets ] )
    return [ OrderedDict([ ("point_table", "%s_hash_%s" % (p, h)), ("poly_table", poly) ]) for p in pointDatasets for h in range(2,10,2) for poly in polygonDatasets ]

def WriteJSFile(filePath, listofStrings):
    """

    """
    with open(filePath, 'w', newline="\n") as fOut:
        for i in listofStrings:
            fOut.writelines("{}\n".format(i) )


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
    

    subparser = parser.add_subparsers(dest="command")
    pointPolyJoin_subparser = subparser.add_parser('point_polygon_join')
    pointPolyJoin_subparser.set_defaults(func=PointPolygonJoin) #["random","synthetic"])
    
#    pointPolyJoin_subparser.add_argument("--point", required=False, help="Table name of the point dataset", dest="point")
#    pointPolyJoin_subparser.add_argument("--polygon", required=False, help="Table name of the polygon dataset", dest="polygon")

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
    outJSPath = "../js/test1.js"
    timings = OrderedDict()
    mongoCon = CreateMongoConnection(args.host, args.port)
    theDB = mongoCon[args.db]
    theFunctions = LoadFunctions()
    # LoadMongoFunctions(theDB)     
    pointDatasets = ["%s_%s" % (i, size) for i in ["random", "synthetic"] for size in [1,10] ] #,50,100
    polygonDatasets = ["states", "counties", "tracts"]#, "blocks"]    
    
    
    if args.command == "point_polygon_join":
#        print("HERE")
        datasets = args.func(pointDatasets, polygonDatasets)
        queries = [ PointPolygonQuery(d['poly_table'], d['point_table']) for d in datasets]
#        print(datasets)
#        print(queries)


        
    for query, d in zip(queries, datasets):
        theFunctions.append(query)
        WriteJSFile(outJSPath, theFunctions)
        theFunctions.pop()
        for r in range(1,args.runs+1):
            

            start = timeit.default_timer()
            # r = theDB.eval(query)
            finalCommand = r"mongo research --port {} < {}".format(args.port, outJSPath)
            print(finalCommand)            
            p = subprocess.Popen(finalCommand, shell=True)
            p.wait()
            out, err = p.communicate()

            stop = timeit.default_timer()
            queryTime = stop-start
            # theFunctions.pop()
            # tables = "%s_%s" % ()
            timings[(r,d["point_table"], d["poly_table"])] = OrderedDict([ ("point_table", d["point_table"]), ("poly_table", d["poly_table"]), ("query_time", queryTime),("run",r)  ])
        
            
    if args.csv: WriteFile(args.csv, timings)
    print("Finished")
