# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import arcpy

shapeFilesDir = r"c:\scidb\4326"
geoDatabase = r"c:\scidb\vector\topology.gdb"
featureDataset = "boundaries"
topologyName = "boundary_top"
featureDatasetPath = r"%s\%s" % (geoDatabase, featureDataset)
topologyPath = r"%s\%s\%s" % (geoDatabase, featureDataset, topologyName)

arcpy.env.workspace = shapeFilesDir
arcpy.env.overwriteOutput = True

def ImportFeatureClass(shapeFilePath, geoDatabaseFeatureDataset, featureClassName):
    """
    
    arcpy.FeatureClassToFeatureClass_conversion(r"c:\scidb\4326\states.shp", r"c:\scidb\vector\vector.gdb\boundaries", "states")
    """
    arcpy.FeatureClassToFeatureClass_conversion(shapeFilePath, geoDatabaseFeatureDataset, featureClassName)

def AddFeatureToTopology(theTopology, featureClassName, xy_rank=1, z_rank=1):
    """
    
    """
    
    arcpy.AddFeatureClassToTopology_management(theTopology, featureClassName, xy_rank, z_rank)


def AddTopologyRules(topologyPath, featureClassPath):
    """
    
    arcpy.AddRuleToTopology_management(r"c:\scidb\vector\vector.gdb\boundaries\boundaries_Topology", 'Must Not Have Gaps (Area)', r"c:\scidb\vector\vector.gdb\boundaries\states")
    """
    
    for rule in ['Must Not Have Gaps (Area)', 'Must Not Overlap (Area)']:
        arcpy.AddRuleToTopology_management(topologyPath, rule, featureClassPath)
    
    


arcpy.CreateFeatureDataset_management(geoDatabase, featureDataset)
arcpy.CreateTopology_management(featureDatasetPath, topologyName)  

shapefiles = [ (r"%s\%s" % (shapeFilesDir, f), f.split(".")[0]) for f in arcpy.ListFeatureClasses() ]

for shapefile in shapefiles:
    shapeFilePath, shapeFileName = shapefile
    ImportFeatureClass(shapeFilePath, featureDatasetPath, shapeFileName)
    featureClassPath = r"%s\%s" % (featureDatasetPath, shapeFileName)
    AddFeatureToTopology(topologyPath, featureClassPath)
    AddTopologyRules(topologyPath, featureClassPath)
    arcpy.ValidateTopology_management(topologyPath)
    
