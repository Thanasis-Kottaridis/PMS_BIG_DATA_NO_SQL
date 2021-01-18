from pymongo import MongoClient

# configuration const!
isLocal = True  # False to query server


def connectMongoDB():
    try :
        if isLocal:
            # connect to local mongo db
            connect = MongoClient()
            # get server database
        else:
            # conect to mongo server
            connect = MongoClient("mongodb://mongoadmin2:mongoadmin@83.212.117.74/admin")

        # connecting or switching to the database
        db = connect.marine_trafic

        return connect, db
    except :
        print("Could not connect MongoDB")
