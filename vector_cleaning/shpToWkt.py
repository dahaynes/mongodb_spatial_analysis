# -*- coding: utf-8 -*-
"""
Created on Wed Jan  9 11:02:55 2019

@author: dahaynes
"""

from osgeo import ogr
import fiona, csv
from collections import OrderedDict

def WriteFile(filePath, theDictionary):
    """
    This function writes out the dictionary as csv
    """
    
    thekeys = list(theDictionary.keys())
    
    with open(filePath, 'w', newline='\n') as csvFile:
        fields = list(theDictionary[thekeys[0]].keys())
        theWriter = csv.DictWriter(csvFile, fieldnames=fields, delimiter=';')
        theWriter.writeheader()

        for k in theDictionary.keys():
            theWriter.writerow(theDictionary[k])

def GetAttributes(inFilePath, CRS=4326):
    """
    THe function will read the attributes / properites using fiona. 
    Returns an ordered dictionary
    """
    attributes = OrderedDict()
    with fiona.open(theShapefilePath, 'r', crs=CRS) as theShp:
        for f, feature in enumerate(theShp):
            attributes[f] = feature['properties']
        
    return(attributes)



theShapefilePath = r"E:\scidb_datasets\vector\randpoints_50m.shp"
outTextFile = r"E:\scidb_datasets\vector\randompoint_50million.txt"

attributes = GetAttributes(theShapefilePath)
ogrDriver = ogr.GetDriverByName("ESRI Shapefile")
theShp = ogrDriver.Open(theShapefilePath)
layer = theShp.GetLayer()
layer.GetName()
    
for feature, attribute in zip(layer, attributes):
    geom = feature.GetGeometryRef()
    text = geom.ExportToWkt()
    attributes[attribute]['geom'] = text

del layer, theShp      

WriteFile(outTextFile, attributes)