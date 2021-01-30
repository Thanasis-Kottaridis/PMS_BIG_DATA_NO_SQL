"""
    Bullet 2: Spatial queries
    Range, k-nn, distance join queries

    query1: find trajectories for all ships with greek flag in Bay of Biscay
    (den etrekse pote poli megali perioxi + oti argi pou argei gia tin simea)
    @SOS:- AFTER UPDATE GIA TA COUNTRIES
    time to fetch polygon --- 0.040461063385009766 seconds ---
    time to fetch all valid mmsi--- 0.05022907257080078 seconds ---
    time tou find existing trajectories in spatio box--- 2.985564947128296 seconds ---

    RANGE QUERIES
    query2: find all ships that moved in range from 10 to 30 sea miles from Burst port
    (this query can be used for any point and for any min and max range )
    [-4.47530,48.3827]
    time on first run --- 327.6065671443939 seconds ---

    SOS THE ABOVE QUERY CANT BE VISUALIZED SO WE USE A DIFFERENT TEST POINT [-4.1660385,50.334972]


    query3:
    find k closest ship sigmas to a point (test point:" coordinates" : [-4.1660385,50.334972]) (k=5)
"""

# my packages
from mongo.query import relationalQueries
from mongo import mongoConnector as connector
from mongo import mongoUtils as utils
from bson.json_util import dumps
import matplotlib.pyplot as plt
import time
import json


main_options = ['1', '2', '3', '0']


def spatialQueries_menu():
    print("\n")
    print('|---------------- Spatial queries Menu -------------------|')
    print('|                                                         |')
    print('| 1.  find trajectories for all ships                     |')
    print('|     with greek flag in Bay of Biscay                    |')
    print('| 2.  find all ships that moved in range from             |')
    print('|     10 to 20 sea miles from a point                     |')
    print('|     (test point:" Brest Port)                           |')
    print('| 3.  find k closest ship sigmas in range                 |')
    print('|     10 to 20 sea miles from a point                     |')
    print('|    (test point:" Brest Port) (k=100.000)                |')
    print('|                                                         |')
    print('| 0.  Exit                                                |')
    print('|---------------------------------------------------------|')
    return input('Your choice: ')


def findShipsNearPoint(point, tsFrom=None, tsTo=None, k_near=None, collection=None, doPlot=False, logResponse=False,
                       queryTitle=None) :
    start_time = time.time()
    if collection is None :
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

    query = {"location" : {"$near" : {"$geometry" : point}}}

    # add ts range
    if tsTo is not None and tsFrom is not None :
        query["ts"] = {"$gte" : tsFrom, "$lte" : tsTo}
    elif tsFrom is not None :
        query["ts"] = {"$gte" : tsFrom}
    elif tsTo is not None :
        query["ts"] = {"$lte" : tsTo}

    # set k near range if exist and execute query
    if k_near is not None :
        results = collection.find(query).limit(k_near)
    else :
        results = collection.find(query)

    dictlist = utils.queryResultToDictList(results)
    print("--- %s seconds ---" % (time.time() - start_time))

    print(json.dumps(dictlist, sort_keys=False, indent=4, default=str))

    if logResponse :
        print(json.dumps(dictlist, sort_keys=False, indent=4, default=str))

    # check if plot needed
    if doPlot :
        print("---  PLOTTING ---")

        ax = utils.createAXNFigure()

        # get n (ships) + 1 (point) random colors
        cmap = utils.get_cmap(len(dictlist) + 1)

        # plot points
        ax.plot(point["coordinates"][0], point["coordinates"][1], marker='x', alpha=0.5, c=cmap(0),
                label="Target Point")

        # plot pings
        for index, ship in enumerate(dictlist) :
            ax.plot(ship["location"]["coordinates"][0], ship["location"]["coordinates"][1], 'ro', alpha=0.5)

        plt.title(queryTitle)
        plt.ylabel("Latitude")
        plt.xlabel("Longitude")
        plt.show()

    return dictlist


def executeSpatialQuery():
    choice = -1
    while choice not in main_options :
        choice = spatialQueries_menu()

        if choice == '1' :
            # query 1
            print("--------------You choose 1--------------")
            poly = relationalQueries.findPolyFromSeas(seaName="Bay of Biscay")
            shipMMSI = relationalQueries.getShipsByCountry(["France"])
            matchAggregation = {"$match" : {'mmsi' : {'$in' : shipMMSI.tolist()},
                                            "location" : {"$geoWithin" : {"$geometry" : poly["geometry"]}}}}

            utils.findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, withPoly=poly["geometry"],
                                               logResponse=False)


        elif choice == '2' :
            # query 2
            print("--------------You choose 2--------------")
            port_point = relationalQueries.findPort()
            matchAggregation = {
                "$geoNear" : {"near" : {"type" : "Point", "coordinates" : port_point['geometry']['coordinates'][0]},
                              "distanceField" : "dist.calculated",
                              "minDistance" : utils.nautical_mile_in_meters * 10,
                              "maxDistance" : utils.nautical_mile_in_meters * 20,
                              "spherical" : True, "key" : "location"}}
            # den to kanei plot
            utils.findPointsForMatchAggr(matchAggregation, doPlot=True ,allowDiskUse=True ,queryTitle="Find all ships that moved in range from 10 to 50 sea miles from Burst port")

        elif choice == '3' :
            # print("--------------You choose 3--------------")
            # # point = {"type" : "Point", "coordinates" : [-4.1660385, 50.334972]}
            # port_point = relationalQueries.findPort()
            # point = {"type" : "Point", "coordinates" : port_point['geometry']['coordinates'][0]}
            # findShipsNearPoint(point, doPlot=True, k_near=1000)

            print("--------------You choose 3--------------")
            port_point = relationalQueries.findPort()
            matchAggregation = {
            "$geoNear": {"near" : {"type" : "Point", "coordinates" : port_point['geometry']['coordinates'][0]},
                         "distanceField" : "dist.calculated",
                         "minDistance" : utils.nautical_mile_in_meters * 10,
                         "maxDistance" : utils.nautical_mile_in_meters * 20,
                         "spherical" : True, "key" : "location"}}
            # den to kanei plot
            utils.findPointsForMatchAggr(matchAggregation, doPlot=False, allowDiskUse=True, k_near=100000,
                                         queryTitle="Find K-nn = 100.000 ships that moved in range\nfrom 10 to 20 sea miles from Burst port")

