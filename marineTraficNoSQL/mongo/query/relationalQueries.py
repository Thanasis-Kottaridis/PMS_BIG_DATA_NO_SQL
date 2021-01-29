"""
        Bullet 1:
        SQL Like/ Relational queries

        query1: Find trajectories for all greek flag vessels (by word)
        very slow because we dont have index on it
        local time on first run: --- 296.6688349246979 seconds ---
        local time on second run: --- 148.310476064682 seconds ---
        @SOS:-> After update to retrieve it by country code in mmsi  it tooks 1.2 sec
        local time to get and filter all country codes and all mmsi --- 0.07462787628173828 seconds ---
        local time to execute query --- 0.046157121658325195 seconds ---

        query2: find trajectories for all france and German Dredger for 72 hours interval from ts = 1448988894
        vres ola ta fishing vessels me galiki simea pou kinounte me
    """


# my packages
from mongo import mongoConnector as connector
from mongo import mongoUtils as utils
# python libraries
import numpy as np
import matplotlib.pyplot as plt
from descartes import PolygonPatch
import time
import json


main_options = ['1', '2', '3', '4', '5', '0']
valisSea_otions = ['1', '2', '3', '4', '0']
targetSeas = ["Celtic Sea",
                 "Bay of Biscay",
                 "English Channel",
                 "Bristol Channel",
                 "St. George's Channel"
                 ]

def relationalQueries_menu():
    print("\n")
    print('|-------------- Relational queries Menu -----------------|')
    print('|                                                        |')
    print('| 1.  Find trajectories for all                          |')
    print('|     greek flag vessels (by word)                       |')
    print('| 2.  find trajectories for all france and German        |')
    print('|     Dredger for 72 hours interval from ts = 1448988894 |')
    print('| 3.  Find Ship Trajectory Given MMSI                    |')
    print('|     and optional time interval (tsFrom, tsTo)          |')
    print('| 4.  Find Poly From Seas                                |')
    print('| 5.  Find Port                                          |')
    print('|                                                        |')
    print('| 0.  Exit                                               |')
    print('|--------------------------------------------------------|')
    return input('Your choice: ')


def selectSee_menu():
    print("\n")
    print('|-------------- Select One Of valid Seas -----------------|')
    print('|                                                         |')
    print('| 1.  Celtic Sea                                          |')
    print('| 2.  Bay of Biscay                                       |')
    print('| 3.  English Channel                                     |')
    print('| 4.  Bristol Channel                                     |')
    print('|                                                         |')
    print('| 0.  Exit                                                |')
    print('|---------------------------------------------------------|')
    return input('Your choice: ')


def getAllAisMMSI() :
    """
    This Query gets all mmsi from ais_navigation collection
    :return:
    """
    # connecting or switching to the database
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation
    document_ids = collection.find().distinct('mmsi')  # list of all ids
    return document_ids


def getShipsByCountry(countryName, db=None):
    """
        This Query gets all records from ships that their country is in countryName list
        :param db: db connection
        :param countryName: list of target country names
        :return:
    """
    start_time = time.time()

    if db is None :
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

    # creating or switching to countries collection
    collection = db.countries

    # get all codes by country name
    country = collection.find({"country" : {"$in" : countryName}})
    countryCodes = []
    for c in country :
        countryCodes.extend(c["country_codes"])

    countryCodes = np.array(countryCodes)

    # get all ships by mmsi
    ships = np.array(getAllAisMMSI())
    vmatch = np.vectorize(lambda mmsi : int(str(mmsi)[:3]) in countryCodes)

    ships_new = vmatch(ships)
    print("--- %s seconds ---" % (time.time() - start_time))
    return ships[ships_new]


def findShipTrajectory(mmsi, tsFrom=None, tsTo=None, collection=None) :
    """
    Gets trajectory for a given mmsi and have optional time interval (tsFrom, tsTo)
    :param mmsi: target mmsi
    :param tsFrom: optional time from (default is None)
    :param tsTo: optional time from (default is None)
    :return:
    """
    start_time = time.time()

    if collection is None :
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

    # create mongo aggregation pipeline
    pipeline = [
        {"$match" : {'mmsi' : mmsi,
                     **({'ts' : {"$gte" : tsFrom, "$lte" : tsTo}} if tsFrom is not None and tsTo is not None else {})
                    }
         },
        {"$group" : {"_id" : "$mmsi", "ts" : {"$push" : "$ts"}, "total" : {"$sum" : 1},
                     "location" : {"$push" : "$location.coordinates"}}}
    ]
    # explain = db.command('aggregate', 'ais_navigation', pipeline=pipeline, explain=True)

    results = collection.aggregate(pipeline)
    dictlist = utils.queryResultToDictList(results, dictlist=[])

    # print(json.dumps(dictlist, sort_keys=False, indent=4))
    print("--- %s seconds ---" % (time.time() - start_time))

    # convert point list into MultiPoint
    return utils.pointsListToLineString(dictlist[0]["location"])


def findPolyFromSeas(seaName="Celtic Sea") :
    start_time = time.time()

    # connecting or switching to the database
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.world_seas

    results = collection.find_one({"properties.NAME" : seaName})
    print("--- %s seconds ---" % (time.time() - start_time))

    return results


def findPort(portName='Brest') :
    start_time = time.time()

    # connecting or switching to the database
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.world_port_geo

    results = collection.find_one({"properties.libelle_po" : portName})
    print("--- %s seconds ---" % (time.time() - start_time))

    return results


def findTestPoly(polyId) :
    start_time = time.time()

    # connecting or switching to the database
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.query_polygons

    results = collection.find_one({"_id" : polyId})
    print("--- %s seconds ---" % (time.time() - start_time))

    return results


def findTrajectoryByVesselsFlag(country='Greece', collection=None) :
    start_time = time.time()
    if collection is None :
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

        # create mongo aggregation pipeline
    pipeline = [
        {"$match" : {{'ship_metadata.mmsi_country.country' : country}}},
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}}},
        {"$sort" : {'total' : -1}}
    ]

    # execute query
    results = collection.aggregate(pipeline)
    dictlist = utils.queryResultToDictList(results)
    print("--- %s seconds ---" % (time.time() - start_time))

    return dictlist


def executeRelationalQuery():
    choice = -1
    while choice not in main_options :
        choice = relationalQueries_menu()

        if choice == '1' :
            # query 1
            print("--------------You choose 1--------------")
            shipMMSI = getShipsByCountry(["Greece"])
            matchAggregation = {"$match" : {'mmsi' : {'$in' : shipMMSI.tolist()}}}
            utils.findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, logResponse=True)
        elif choice == '2' :
            # query 2
            print("--------------You choose 2--------------")
            shipMMSI = getShipsByCountry(["France", "German"])
            matchAggregation = {"$match" : {'mmsi' : {'$in': shipMMSI.tolist()},
                                            'ship_metadata.ship_type.type_name': 'Dredger',
                                            'ts' : {"$gte" : 1448988894, "$lte" : 1448988894 + (72 * utils.one_hour_in_unix_time)}}}
            utils.findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, logResponse=True, allowDiskUse=True)
        elif choice == '3' :
            print("--------------You choose 3--------------")
            print("\n")
            mmsi = input("GIVE A VALID MMSI. For example: 240266000 or 227002630: ")
            tsFrom = input("OPTIONAL GIVE TIME FROM IN UNIX ELSE PRESS 0. For example: 1448988894: ")
            tsTo = input("OPTIONAL GIVE TIME TO IN UNIX ELSE PRESS 0. For example: 1448988894: ")
            trajectory = findShipTrajectory(int(mmsi), tsFrom=tsFrom if tsFrom != '0' else None, tsTo=tsTo if tsTo != '0' else None)

            # create ax
            ax = utils.createAXNFigure()
            if 2 < len(trajectory["coordinates"]) :
                utils.plotLineString(ax, trajectory, color='r', alpha=1,
                                     label=mmsi)  # alpha 0.5 gia na doume overlaps

            plt.title("Find Trajectory For ship: {}".format(mmsi))
            plt.ylabel("Latitude")
            plt.xlabel("Longitude")
            plt.show()
        elif choice == '4' :
            print("--------------You choose 4--------------")
            sea_choice = -1
            while sea_choice not in main_options :
                sea_choice = selectSee_menu()

                if choice != '0':
                    sea = findPolyFromSeas(targetSeas[int(sea_choice) - 1])
                    # plot poly
                    ax = utils.createAXNFigure()
                    poly = {
                            "type" : "Polygon",
                            "coordinates" : [ sea['geometry']['coordinates'][0] ]
                    }

                    ax.add_patch(PolygonPatch(poly, fc='y', ec='k', alpha=0.1, zorder=2))

                    plt.title("Sea Constraints of: {}".format(targetSeas[int(sea_choice) - 1]))
                    plt.ylabel("Latitude")
                    plt.xlabel("Longitude")
                    plt.show()
        elif choice == '5' :
            print("--------------You choose 5--------------")
            print("\n")
            port_name = input("GIVE A VALID PORT NAME. For example: Brest: ")
            point = findPort(port_name)
            ax = utils.createAXNFigure()

            ax.plot(point['geometry']['coordinates'][0][0], point['geometry']['coordinates'][0][1], marker='x', alpha=1, c='r',
                    label="Target Point")

            plt.title("Location of Port: {}".format(port_name))
            plt.ylabel("Latitude")
            plt.xlabel("Longitude")
            plt.show()
