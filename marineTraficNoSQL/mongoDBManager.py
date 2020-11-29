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
        connect = MongoClient()
        print("Connected Successfully!")
        return connect
    except:
        print("Could not connect MongoDB")


def insertData(insertDoc, isMany= True):
    connection = connectMongoDB()

    # connecting or switching to the database
    db = connection.marin_trafic

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation

    # insert data based on whether it is many or not
    if isMany:
        collection.insert_many(insertDoc)
    else:
        collection.insert_one(insertDoc)

    printData(collection)


def printData(collection):
    # Printing the data inserted
    cursor = collection.find()
    for record in cursor:
        print(record)
