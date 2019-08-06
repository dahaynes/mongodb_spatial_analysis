# -*- coding: utf-8 -*-
"""
Spyder Editor

This script is used to for performance tests on MongoDB (Vector)


def CreateMongoConnection(host='localhost', port=27017):
    """
    host=['localhost:27017']
    """
    connString ="%s:%s" % (host, port)
    print(connString)
    connection = MongoClient(connString) #host

        
    return(connection)