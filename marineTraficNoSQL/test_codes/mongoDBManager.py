"""
    This file will be used for handling opperations with mongoDB such as
    1) creating and updating collections
    2) executing supporting queries
    TODO:- ADD MORE FUNCTIONALITIES HERE

    (establish connection link: https://kb.objectrocket.com/mongo-db/how-to-insert-data-in-mongodb-using-python-683)
"""
# my packages
from mongo import mongoConnector as connector
from mongo import mongoUtils as utils
from mongo.query import relationalQueries
from mongo.query import distanceJoinQueries

# python libraries
from pymongo import MongoClient
import numpy as np
import json
import math
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from descartes import PolygonPatch
from shapely.geometry import LineString
import shapely.geometry as sg
import geog
import time
from geospatial import geoDataPreprocessing

# CONSTS
nautical_mile_in_meters = 1852
one_hour_in_unix_time = 3600

# def findShipsNearPoint(point, tsFrom=None, tsTo=None, k_near=None, collection=None, doPlot=False, logResponse=False,
#                        queryTitle=None) :
#     start_time = time.time()
#     if collection is None :
#         # connecting or switching to the database
#         connection, db = connector.connectMongoDB()
#
#         # creating or switching to ais_navigation collection
#         collection = db.ais_navigation
#
#     query = {"location" : {"$near" : {"$geometry" : point}}}
#
#     # add ts range
#     if tsTo is not None and tsFrom is not None :
#         query["ts"] = {"$gte" : tsFrom, "$lte" : tsTo}
#     elif tsFrom is not None :
#         query["ts"] = {"$gte" : tsFrom}
#     elif tsTo is not None :
#         query["ts"] = {"$lte" : tsTo}
#
#     # set k near range if exist and execute query
#     if k_near is not None :
#         results = collection.find(query).limit(k_near)
#     else :
#         results = collection.find(query)
#
#     dictlist = utils.queryResultToDictList(results)
#     print("--- %s seconds ---" % (time.time() - start_time))
#
#     print(json.dumps(dictlist, sort_keys=False, indent=4, default=str))
#
#     if logResponse :
#         print(json.dumps(dictlist, sort_keys=False, indent=4, default=str))
#
#     # check if plot needed
#     if doPlot :
#         print("---  PLOTTING ---")
#
#         ax = utils.createAXNFigure()
#
#         # get n (ships) + 1 (point) random colors
#         cmap = utils.get_cmap(len(dictlist) + 1)
#
#         # plot points
#         ax.plot(point["coordinates"][0], point["coordinates"][1], marker='x', alpha=0.5, c=cmap(0),
#                 label="Target Point")
#
#         # plot pings
#         for index, ship in enumerate(dictlist) :
#             ax.plot(ship["location"]["coordinates"][0], ship["location"]["coordinates"][1], 'ro', alpha=0.5)
#
#         plt.title(queryTitle)
#         plt.y("Latitude")
#         plt.xlabel("Longitude")
#         plt.show()
#
#     return dictlist

def ext_givenTrajectoryFindSimilar(dictlist, trajectory, k_most) :
    """
    givenTrajectoryFindSimilar query extension for calculating k most similar trajectories

    :param dictlist: this list contains trajectories in dict format (the format returned from mongo quert)
    :param k_most: number of most similar trajectories
    :return: the dict list that contains only k most similar trajectories order by similarity.
    """

    # finds mean distance for all trajectories
    # 1) converts dict list to np array
    np_dictList = np.array(dictlist)

    # 2) extract mean dist for each trajectory
    # 5 is min pings to consider trajectory
    fv = np.vectorize(utils.calculatePointsDistance, signature='(n),(n)->()')
    dist_mean = []
    for ship in dictlist :
        if len(ship["location"]) > 2 :
            start_time2 = time.time()
            ps1 = np.array(trajectory)
            ps2 = np.array(ship["location"])
            r = fv(ps1[:, np.newaxis], ps2)
            dist_mean.append(np.mean(r))
            print("--- %s seconds ---" % (time.time() - start_time2))

    # 3) apply a np short on dist_mean, and then apply the same short to np_dictList
    # https://stackoverflow.com/questions/1903462/how-can-i-zip-sort-parallel-numpy-arrays
    sort_path = np.argsort(np.array(dist_mean))
    np_dictList = np_dictList[sort_path]
    dictlist = np_dictList[:k_most]

    print(dist_mean)
    print(sort_path)

    return dictlist


def givenTrajectoryFindSimilar(trajectory, tsFrom=1448988894, tsTo=1449075294, k_most=0, d=0.1) :
    """
        Given a trajectory and a time interval find other similar trajectories

        step1:
        convert trajectory from multiline into polygon

        step2 (optional): if k_most similar is greater than 0 then keep k trajectories with
        minimum mean distance to the target trajectory else finds similar trajectories threshold based using d

        step3:
        plot trajectory and the polygon created based on it

        plot step:
        plot all returned trajectories

    :param trajectory: is a multiLine geo json obj
    :param tsFrom: timestamp form in unix
    :param tsTo: timestamp to in unix
    :return: a list of similar trajectories
    """
    start_time = time.time()

    # connecting or switching to the database
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation

    # step 1
    poly, shpOuterPoly, shpInnerPoly = utils.convertLineStringToPolygon(trajectory["coordinates"], d=d)

    # create mongo aggregation pipeline
    pipeline = [
        {"$match" : {"location" : {"$geoWithin" : {"$geometry" : poly}}, 'ts' : {"$gte" : tsFrom, "$lte" : tsTo}}},
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}}},
        {"$sort" : {'total' : -1}}
    ]

    # execute query
    results = collection.aggregate(pipeline)
    dictlist = utils.queryResultToDictList(results)

    print(json.dumps(dictlist, sort_keys=False, indent=4, default=str))

    # step 2 checks if it is a k-most query or a threshold based
    if k_most > 0 :
        dictlist = ext_givenTrajectoryFindSimilar(dictlist, trajectory["coordinates"], k_most)

    print("--- %s seconds ---" % (time.time() - start_time))

    # step 2
    ax = utils.createAXNFigure()

    utils.plotLineString(ax, trajectory, color='k', alpha=0.2, label="Given Trajectory")
    # plot poly
    ax.add_patch(PolygonPatch(poly, fc=utils.BLUE, ec=utils.BLUE, alpha=0.5, zorder=2, label="Trajectory Polygon with (d=0.2)"))
    # ax.axis('scaled')

    # get n (ships) + points list len  random colors
    cmap = utils.get_cmap(len(dictlist))

    #  plot trajectories
    for i, ship in enumerate(dictlist) :
        trajj = utils.pointsListToLineString(ship["location"])

        if 2 < len(trajj["coordinates"]) and trajj["coordinates"] != trajectory["coordinates"] :
            utils.plotLineString(ax, trajj, color=cmap(i), alpha=0.5, label=ship["_id"])

    # plot step
    ax.legend(loc='center left', title='Ship MMSI', bbox_to_anchor=(1, 0.5),
              ncol=1 if len(dictlist) < 10 else int(len(dictlist) / 10))
    plt.title("Find Trajectories that pass near specific points in specific time interval")
    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
    plt.show()


def findPingsPerPoint(point, collection=None) :
    if collection is None :
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

    # create mongo aggregation pipeline
    pipeline = [
        {'$geoNear' : {"near" : point,
                       "distanceField" : "dist.calculated",
                       "maxDistance" : nautical_mile_in_meters * 10,
                       "spherical" : True,
                       "key" : "location"}},
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "ts" : {"$push" : "$ts"}}},
        # {"$sort" : {'total' : -1}}
    ]
    # execute query
    results = collection.aggregate(pipeline)
    dict = utils.queryResultsToDict(results)
    return dict


def findTrajectoriesFromPoints(pointsList, hoursList) :
    """
        this function is used to take all trajectories that pass from all points in point
        list with time less than 6 hours from one point to another

        @methodology
        Step1: Fetch all the pings received near each point grouped by mmsi
        and containing a list of timestamps which show how many times each ship
        did ping on near this location and convert them into a list of dicts each
        dict for each location

        Step2: for each mmsi in first location dict we have to check if exists in other 2
        location dicts

        step3: and then we have to check if times are in range
        how to do it? (6 HOURS ARE 21600 IN UNIX TIME)
        1) filter all point 1 timestamps to be after 12 hours interval
        (because we dont care for points in that interval 1 of them is enough)
        2) for each valid ts in dict 1 check if exists time in dict 2 greater than ts
        and les or equal to ts+6h
        3) same for step3.2 but for dict3 with ts+12h

        step4: fetch trajectories for valid mmsi/time pairs

        (Den xrisimopoiithike gt einai pio argo me auto ton tropo)
        Idea: make multiple parallel requests to mongo (1 for each point)
        we can use one thread for each processor my laptop has 4cores and server vm has 8
    :param pointsList: list of points to execute the query
    :return:
    """
    # start timer
    # me pool request --- 1.9434340000152588 seconds ---
    # me for kai 3 requests --- 0.32604384422302246 seconds ---
    start_time = time.time()

    # connecting or switching to the database
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation

    # Step 1
    # pool object creation
    # pool = multiprocessing.Pool(processes=4)  # spawn 4 processes for laptop 8 for server
    #
    resultsList = []  # each element contains the results for each point
    #
    # resultsList = pool.map(findPingsPerPoint, pointsList)

    for point in pointsList :
        dictlist = findPingsPerPoint(point, collection=collection)
        resultsList.append(dictlist)

    print(json.dumps(resultsList, sort_keys=False, indent=4, default=str))

    # step 2:
    totalHours = sum(hoursList)
    counter = 0
    validMMSITimePair = []
    for key in resultsList[0].keys() :
        # checks if key exists in all dicts
        isValidKey = True
        for dict in resultsList[1 :] :
            if not key in dict.keys() :
                isValidKey = False
                break

        if isValidKey :
            # Step 3:
            # if ket is valid i have to filter its timestamps
            # first order ts
            resultsList[0][key]["ts"].sort(reverse=False)
            validTS = [resultsList[0][key]["ts"][0]]
            for t in resultsList[0][key]["ts"] :
                if t >= (totalHours * utils.one_hour_in_unix_time) + validTS[-1] :
                    validTS.append(t)
            print(validTS)

            # get first ts
            isValid = True
            for ts in validTS :
                for count, d in enumerate(resultsList[1 :]) :
                    if not any(ts < t <= ts + (sum(hoursList[:(count + 1)]) * utils.one_hour_in_unix_time) for t in d[key]["ts"]) :
                        isValid = False
                        break
                if isValid :
                    validMMSITimePair.append({"mmsi" : key, "ts" : ts})

            # if isValid :
            #     validMMSITimePair.append(key)

            counter += 1

    # step 4
    trajectories = []
    for pair in validMMSITimePair :
        trajectories.append(
            relationalQueries.findShipTrajectory(mmsi=pair["mmsi"],
                               tsFrom=pair["ts"],
                               tsTo=pair["ts"] + (totalHours * utils.one_hour_in_unix_time),
                               collection=collection)
        )

    print(json.dumps(trajectories, sort_keys=False, indent=4, default=str))
    print("--- %s seconds ---" % (time.time() - start_time))

    # step 5 plot trajectories
    ax = utils.createAXNFigure()
    # get n (ships) + points list len  random colors
    cmap = utils.get_cmap(len(trajectories) + len(pointsList))

    # plot points
    for index, point in enumerate(pointsList) :
        ax.plot(point["coordinates"][0], point["coordinates"][1], marker='x', alpha=1, markersize=12, c=cmap(index),
                label="point {0}".format(index))

    # plot trajectories
    for i, trajj in enumerate(trajectories) :
        if 2 < len(trajj["coordinates"]) :
            utils.plotLineString(ax, trajj, color=cmap(i + 3), alpha=1,
                           label=validMMSITimePair[i]["mmsi"])  # alpha 0.5 gia na doume overlaps

    ax.legend(loc='center left', title='Ship MMSI', bbox_to_anchor=(1, 0.5),
              ncol=1 if len(trajectories) < 10 else int(len(trajectories) / 10))
    plt.title("Find Trajectories that pass near specific points in specific time interval")
    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
    plt.show()


if __name__ == '__main__' :
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
    # query 1
    # shipMMSI = relationalQueries.getShipsByCountry(["Greece"])

    """ 
    Elliniko plio pou kinitw wraia mesa sto bay of biscay
    mmsi 240031000
    time from 1449352691,
    time to 1449659042
    """
    shipMMSI = [240031000]
    matchAggregation = {"$match" : {'mmsi' : {'$in': shipMMSI}}}
    # utils.findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, logResponse=True)

    # query 2
    # shipMMSI = relationalQueries.getShipsByCountry(["France", "German"])
    # matchAggregation = {"$match" : {'mmsi' : {'$in': shipMMSI.tolist()},
    #                                 'ship_metadata.ship_type.type_name': 'Dredger',
    #                                 'ts' : {"$gte" : 1448988894, "$lte" : 1448988894 + (72 * one_hour_in_unix_time)}}}
    # utils.findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, logResponse=True, allowDiskUse=True)

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
        query2: find all ships that moved in range from 10 to 50 sea miles from Burst port
        (this query can be used for any point and for any min and max range )
        [-4.47530,48.3827]
        time on first run --- 335.2350790500641 seconds ---

        
        query3: 
        find k closest ship sigmas to a point (test point:" coordinates" : [-4.1660385,50.334972]) (k=5)
        
        query 4: Distance join queries
        given 2 A and B polygon or geo areas (circles on point or bounging box)
        find point pairs (a,b) that dictance from a to b is lees than threshold d
        time for poly: --- 4.524019002914429 seconds ---
        time for box: 
        
        Stats for circles:
        # WITHOUT OVERLAPPING POLYGONES 
        d(k1,k2) 46.84, r1 = 20, r2=20, theta= 30  extended_r1 60, extended_r2 50 (ola KM)
        time for k2, r2 query --- 0.320483922958374 seconds ---, results 1099
        time for k1 min r1, max r1 extended quert--- 0.04397106170654297 seconds ---, results 626
        total time for circles: --- 10.031288862228394 seconds ---
        
        # WITH OVERLAPING POLTGONES
        d(k1,k2) 46.84, r1 = 30, r2=20, theta= 30  extended_r1 60, extended_r2 50 (ola KM)
        total time for circles: --- 583.010231256485 seconds ---

        if i execute the same polygons with no overlaping results--- 13.801034927368164 seconds ---

    """
    # query1
    poly = relationalQueries.findPolyFromSeas(seaName="Bay of Biscay")
    shipMMSI = relationalQueries.getShipsByCountry(["France"])
    matchAggregation = {"$match" : {'mmsi' : {'$in' : shipMMSI.tolist()},
                                    "location" : {"$geoWithin" : {"$geometry" : poly["geometry"]}}}}
    # utils.findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, withPoly=poly["geometry"], logResponse=True)

    # query2 # ARGEI POLI
    port_point = relationalQueries.findPort()
    matchAggregation = {
        "$geoNear" : {"near" : {"type" : "Point", "coordinates" : [-4.475309812300752, 48.38273389573341]},
                      "distanceField" : "dist.calculated",
                      "minDistance" : nautical_mile_in_meters * 10,
                      "maxDistance" : nautical_mile_in_meters * 30,
                      "spherical" : True, "key" : "location"}}
    # den to kanei plot
    # utils.findPointsForMatchAggr(matchAggregation, doPlot=True ,allowDiskUse=True ,queryTitle="Find all ships that moved in range from 10 to 50 sea miles from Burst port")

    # query 3
    point = {"type" : "Point", "coordinates" : [-4.1660385, 50.334972]}
    # findShipsNearPoint(point, doPlot=True, k_near=20)

    # query 4.1 distance join with polygons
    poly1 = {
        "_id" : "test_poly_1",
        "type" : "Polygon",
        "coordinates" : [
            [
                [-3.9385986, 49.8946344],
                [-3.6804199, 50.0765319],
                [-3.3013916, 50.2103068],
                [-2.9388428, 49.9600554],
                [-3.4194946, 49.5287739],
                [-3.9605713, 49.7280302],
                [-3.9385986, 49.8946344]
            ]
        ]
    }

    poly2 = {
        "_id" : "test_poly_2",
        "type" : "Polygon",
        "coordinates" : [
            [
                [-3.9111328, 48.9405432],
                [-3.9385986, 49.3859487],
                [-3.4442139, 49.5145101],
                [-2.9223633, 49.3966751],
                [-2.3126221, 49.099049],
                [-2.6586914, 48.9188897],
                [-3.2958984, 49.0450696],
                [-3.9111328, 48.9405432]
            ]
        ]
    }

    # distanceJoinPolyQuery(poly1, poly2, 50)

    # query 4.2 distance join with boxies
    # sta 100km den vriskei tipota.
    box1 = [
            [-4.118, 49.451],
            [-5.401, 48.839]
    ]
    box2 = [
        [-4.861, 49.458],
        [-3.258, 50.164]
    ]
    # distanceJoinRectQuery(box1, box2, 50)

    # query 4.3 distance join given point and R d(k1,k2) = 167.84 , r1 = 30 and r2 = 20

    k1 = {"type" : "Point", "coordinates" : [-4.219, 49.098]}
    k2 = {"type" : "Point", "coordinates" : [-4.567, 49.336]}

    # overlaping circles
    # distanceJoinSphereQuery(k1, 30, k2, 20, 30)

    # NON OVERLAPPING CIRCLES
    # distanceJoinSphereQuery(k1, 20, k2, 20, 30)

    """
        Bullet 3: Spatio-temporal queries
        Range, k-nn, distance join queries
        
        query1: Find all ships that moved in range from 10 to 30 sea miles from Burst port for one day time range
        for 2 hours time interval
        (to many ping it took a lot of time to find them all 
        first run --- 408.56871485710144 seconds ---
        second run --- 220.98046803474426 seconds --- (great improvement))

        query2: find k closest ship sigmas to a point (test point:" coordinates" : [-4.1660385,50.334972]) (k=5)
        in one day interval (this point is at the entry of playmouth)
        
        query3: find trajectories for all ships with greek flag in Celtic Sea  for one hour interval
        (den etrekse pote poli megali perioxi + oti argi pou argei gia tin simea)
        times:
        time to get Celtic Sea from world seas collection --- 0.040419816970825195 seconds ---
        time to get all greek ships mmsi--- 0.052060842514038086 seconds ---
        time to get trajectories in spatio temporal box--- 1.3465988636016846 seconds ---
        
         query 4: Distance join queries
        given 2 A and B polygon or geo areas (circles on point or bounging box)
        find point pairs (a,b) that dictance from a to b is lees than threshold d
        time for poly: --- 4.524019002914429 seconds ---
        time for box: 
        
        Stats for circles:
        # WITHOUT OVERLAPPING POLYGONES 
        d(k1,k2) 46.84, r1 = 20, r2=20, theta= 30  extended_r1 60, extended_r2 50 (ola KM)
        
        TODO ADD TIMES

    """
    # query1
    port_point = relationalQueries.findPort()
    geoNearAgg = {"$geoNear" : {"near" : {"type" : "Point", "coordinates" : port_point['geometry']['coordinates'][0]},
                                "distanceField" : "dist.calculated",
                                "minDistance" : nautical_mile_in_meters * 10,
                                "maxDistance" : nautical_mile_in_meters * 30,
                                "spherical" : True, "key" : "location"}}

    matchAgg = {"$match" : {'ts' : {"$gte" : 1448988894, "$lte" : 1448988894 + (2 * one_hour_in_unix_time)}}}

    # utils.findPointsForMatchAggr(geoNearAgg,matchAgg,doPlot=True,
    #                        queryTitle="Find all ships that moved in range from 10 to 50 sea miles from Burst port for 2 hours interval")

    # query 2
    point = {"type" : "Point", "coordinates" : [-4.1660385, 50.334972]}
    geoNearAgg = {"$geoNear" : {"near" : point,
                                "distanceField" : "dist.calculated",
                                # "minDistance" : nautical_mile_in_meters * 10,
                                "maxDistance" : nautical_mile_in_meters * 30,
                                "spherical" : True, "key" : "location"}}

    matchAgg = {"$match" : {'ts' : {"$gte" : 1448988894, "$lte" : 1449075294}}}

    # utils.findPointsForMatchAggr(geoNearAgg, matchAgg, k_near=20, doPlot=True,
    #                        queryTitle="find k closest ship sigmas to a point (k=5) in one day interval")

    # query 3:
    poly = relationalQueries.findPolyFromSeas()
    shipMMSI = relationalQueries.getShipsByCountry(["France"])
    matchAggregation = {"$match" : {'mmsi' : {'$in' : shipMMSI.tolist()},
                                    "location" : {"$geoWithin" : {"$geometry" : poly["geometry"]}},
                                    'ts' : {"$gte" : 1448988894, "$lte" : 1448988894 + (6 * one_hour_in_unix_time)}}}
    # utils.findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, withPoly=poly["geometry"], logResponse=True)

    # query 4.1 distance join with polygons spatio temporal
    poly1 = {
        "_id" : "test_poly_1",
        "type" : "Polygon",
        "coordinates" : [
            [
                [-5.1855469,47.5765257],
                [-3.6474609,46.7097359],
                [-2.6586914,45.9511497],
                [-3.2299805,45.7828484],
                [-4.7680664,45.6908328],
                [-5.625,46.4378569],
                [-6.0644531,46.8000594],
                [-5.6469727,47.44295],
                [-5.1855469,47.5765257]
            ]
        ]
    }

    poly2 = {
        "_id" : "test_poly_2",
        "type" : "Polygon",
        "coordinates" : [
            [
                [-3.9111328, 48.9405432],
                [-3.9385986, 49.3859487],
                [-3.4442139, 49.5145101],
                [-2.9223633, 49.3966751],
                [-2.3126221, 49.099049],
                [-2.6586914, 48.9188897],
                [-3.2958984, 49.0450696],
                [-3.9111328, 48.9405432]
            ]
        ]
    }

    # distanceJoinPolyQuery(poly1, poly2, 50, timeFrom=1448988894, timeTo=1448988894 + (6 * one_hour_in_unix_time))

    # query 4.2 distance join with boxies
    # sta 100km den vriskei tipota.
    box1 = [
        [-4.118, 49.451],
        [-5.401, 48.839]
    ]
    box2 = [
        [-4.861, 49.458],
        [-3.258, 50.164]
    ]
    # distanceJoinRectQuery(box1, box2, 50,timeFrom=1448988894, timeTo=1448988894 + (16 * one_hour_in_unix_time))

    # query 4.3 distance join given point and R d(k1,k2) = 167.84 , r1 = 30 and r2 = 20
    # TODO find beater points
    k1 = {"type" : "Point", "coordinates" : [-4.219, 49.098]}
    k2 = {"type" : "Point", "coordinates" : [-5.653, 49.571]}

    # distanceJoinSphereQuery(k1, 30, k2, 20, 30, timeFrom=1448988894, timeTo=1448988894 + (206 * one_hour_in_unix_time))

    """
        bullet 4.1:
        find trajectories in spatio temporal box
    """
    # TODO FIND BEATER POLY WITH MANY POINTS AND TIMES

    box1 = [
        [-4.118, 49.451],
        [-5.401, 48.839]
    ]
    # findTrajectoriesInSpaTemBox(box1)

    """
        Bullet 4.2:
        Given a trajectory, find similar trajectories (threshold-based, k-most similar)
    """
    # trajectory = relationalQueries.findShipTrajectory(mmsi=227002630, tsFrom=1448988894, tsTo=1449075294)
    # trajectory = relationalQueries.findShipTrajectory(mmsi=240266000, tsFrom=1448988894, tsTo=1449075294)
    # trajectory = relationalQueries.findShipTrajectory(mmsi=240266000)

    # givenTrajectoryFindSimilar(trajectory)
    # givenTrajectoryFindSimilar(trajectory, k_most=3)

    """
        Bullet 4.3:
        Complex queries: find trajectories that passed through A,
        then through B (up to X hours later), then through C (up to 6 hours later)
        SOS:- for simplicity we assume that time from one point to another is 6 hours 
        but we have extended the number of points from 3 to N
    """
    pointsList = [
        {"type" : "Point", "coordinates" : [-7.072965, 47.371117]},
        {"type" : "Point", "coordinates" : [-6.494, 47.820]},
        {"type" : "Point", "coordinates" : [-5.683, 48.480]},
        {"type" : "Point", "coordinates" : [-4.911, 48.984]},
        {"type" : "Point", "coordinates" : [-3.683, 49.465]}
    ]

    hoursList = [3, 3]#, 4, 4]
    # findTrajectoriesFromPoints(pointsList, hoursList)

    #
    #TEST
    #
    print("------------ TEST -------------")

    """
    IDEA GIA DISTANCE JOIN
    
    step1: ftiaxnoume to grid tou poligonou me tetragona plevras theta.
    step2: ftiaxnoume expanded grid me tetragona pleuras 2 theta.
    step3: aneuazoume stin mongo kai ta 2 poly set:
        # GIA APLO POLY object tou kathe grid stin mongo:
        {
            _id: 1,
            parent_poly_id: 1,
            expanded_grid_id: 1,
            geometry: {
                "type" : "Polygon",
                "coordinates" : []
            }
        }
        
        # Gia Expanded grid (@SOS AUTO THA EINAI POLY ME TRIPA STO GRID)
        {
            _id: 1,
            parent_poly_id: 1,
            geometry: {
                "type" : "Polygon",
                "coordinates" : []
            }
        }
    """

    poly1 = {
        "_id" : "test_poly_1",
        "type" : "Polygon",
        "coordinates" : [
            [
                [-5.1855469, 47.5765257],
                [-3.6474609, 46.7097359],
                [-2.6586914, 45.9511497],
                [-3.2299805, 45.7828484],
                [-4.7680664, 45.6908328],
                [-5.625, 46.4378569],
                [-6.0644531, 46.8000594],
                [-5.6469727, 47.44295],
                [-5.1855469, 47.5765257]
            ]
        ]
    }
    poly2 = {
        "_id" : "test_poly_1",
        "type" : "Polygon",
        "coordinates" : [
            [
                [-3.9385986, 49.8946344],
                [-3.6804199, 50.0765319],
                [-3.3013916, 50.2103068],
                [-2.9388428, 49.9600554],
                [-3.4194946, 49.5287739],
                [-3.9605713, 49.7280302],
                [-3.9385986, 49.8946344]
            ]
        ]
    }

    # poly1 = relationalQueries.findPolyFromSeas(seaName="Bay of Biscay")
    # poly2 = relationalQueries.findPolyFromSeas(seaName="Celtic Sea")

    # distanceJoinUsingGPDGrid(poly1, mmsi=305810000) #538003876
    # distanceJoinUsingGrid(poly2, mmsi=538003876) # 50 kati plia sto poly2 373206000
    # distanceJoinQueries.distanceJoinUsingGPDGrid(poly2, mmsi=538003876)
    # distanceJoinQueries.distanceJoinUsingGPDGrid(poly2, mmsi=538003876) # 50 kati plia sto poly2 373206000

    # findThresholdBasedSimilarTrajectories(mmsi=240266000, tsFrom=1448988894, tsTo=1449075294)
    # findThresholdBasedSimilarTrajectories(mmsi=227574020, tsFrom=1443676587, tsTo=1443679590, d=0)

    utils.getGridAndSeas()
