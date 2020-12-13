"""
    This file will be used for handling opperations with mongoDB such as
    1) creating and updating collections
    2) executing supporting queries
    TODO:- ADD MORE FUNCTIONALITIES HERE

    (establish connection link: https://kb.objectrocket.com/mongo-db/how-to-insert-data-in-mongodb-using-python-683)
"""
from pymongo import MongoClient


def connectMongoDB():
    try:
        # conect to mongo server
        # connect = MongoClient("mongodb://mongoadmin:mongoadmin@83.212.117.74/admin")
        # connect to local mongo db
        connect = MongoClient()
        print("Connected Successfully!")
        return connect
    except:
        print("Could not connect MongoDB")


def insertAISData(insertDoc, isMany= True):
    connection = connectMongoDB()

    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation

    # insert data based on whether it is many or not
    if isMany:
        collection.insert(insertDoc)
    else:
        collection.insert_one(insertDoc)

    # printData(collection)

def insertPortData(insertDoc, isMany= True):
    connection = connectMongoDB()

    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.world_port_geo

    # insert data based on whether it is many or not
    if isMany :
        collection.insert(insertDoc)
    else :
        collection.insert_one(insertDoc)


def insertFishingPortData(insertDoc, isMany= True):
    connection = connectMongoDB()

    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.fishing_port

    # insert data based on whether it is many or not
    if isMany :
        collection.insert(insertDoc)
    else :
        collection.insert_one(insertDoc)


def insertFullDetailedPortsData(insertDoc, isMany= True):
    connection = connectMongoDB()

    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.world_port_information

    # insert data based on whether it is many or not
    if isMany :
        collection.insert(insertDoc)
    else :
        collection.insert_one(insertDoc)

def insertWorldSeas(data, isMany= True):
    connection = connectMongoDB()

    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.world_seas

    # print(len(data["features"][""]))
    insertDoc = []
    for sea in data["features"] :
        properties = sea["properties"]
        if properties["NAME"] == "Celtic Sea" or properties["NAME"] == "Bay of Biscay" :
            print(properties["NAME"])
            insertDoc.append(sea)

    # insert data based on whether it is many or not
    if isMany :
        collection.insert(insertDoc)
    else :
        collection.insert_one(insertDoc)

def printData(collection):
    # Printing the data inserted
    cursor = collection.find()
    for record in cursor:
        print(record)
