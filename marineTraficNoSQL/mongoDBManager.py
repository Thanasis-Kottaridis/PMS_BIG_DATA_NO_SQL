"""
    This file will be used for handling opperations with mongoDB such as
    1) creating and updating collections
    2) executing supporting queries
    TODO:- ADD MORE FUNCTIONALITIES HERE

    (establish connection link: https://kb.objectrocket.com/mongo-db/how-to-insert-data-in-mongodb-using-python-683)
"""
import multiprocessing
from pymongo import MongoClient
import contextily as ctx
import json
import math
import geopandas as gpd
import matplotlib.pyplot as plt
from descartes import PolygonPatch
from shapely.geometry import LineString
import time


# CONSTS
BLUE = '#6699cc'
GRAY = '#999999'
RED = '#B20000'
nautical_mile_in_meters = 1852
one_hour_in_unix_time = 3600


def connectMongoDB() :
    try :
        # conect to mongo server
        # connect = MongoClient("mongodb://mongoadmin:mongoadmin@83.212.117.74/admin")
        # connect to local mongo db
        connect = MongoClient()
        print("Connected Successfully!")
        return connect
    except :
        print("Could not connect MongoDB")


def queryResultToDictList(results) :
    """
    this helper func converts pymongo query results into a list of dictionaries

    :param results: the results of pymongo query
    :return: returns a list of dictionaries
    """

    dictlist = []
    for doc in results :
        dictlist.append(doc)

    return dictlist


def queryResultsToDict(results) :
    """
        this helper func converts pymongo query results into a dictionary with _id as key

    :param results:
    :return:
    """

    dict = {}
    for doc in results :
        dict[doc["_id"]] = doc

    return dict


def pointsListToMultiPoint(pointList) :
    multiPoint = {
        "type" : "LineString",
        "coordinates" : pointList
    }
    return multiPoint


def pointListToMultiLineString(pointList) :
    multiLine = []
    for i in range(1, len(pointList)) :
        line = [pointList[i - 1], pointList[i]]
        if line not in multiLine :
            multiLine.append(line)

    multiLineString = {
        "type" : "MultiLineString",
        "coordinates" : multiLine
    }
    print("------------------", len(multiLine), "------------------")
    return multiLineString


def convertLineStringToPolygon(line, d=0.1) :
    """
        idea edo einai na kanw ena iterate sta poins ana 2
        na ftiksw 2 listes stin mia tha vazw ta points tis mias mergias stin alli tis allis
        sto proto kai to telefteo point to line tha ipologisw ta false negative
    :param line: line string geojson
    :param d: threshold distance (2*d is the width of poly)
    :return: polygon geojson
    """

    list1 = []
    list2 = []

    for i in range(0, len(line)) :
        list1.append([line[i][0] + d, line[i][1] - d])
        list2.append([line[i][0] - d, line[i][1] + d])

    list2.reverse()
    list1.extend(list2)
    list1.append(list1[0])

    geoPolygon = {
        "type" : "Polygon",
        "coordinates" : [list1]
    }

    print(json.dumps(geoPolygon, sort_keys=False, indent=4))
    return geoPolygon


def convertLineIntoPolygon(line, d=0.4) :
    teta = math.atan2(line[1][0] - line[0][0], line[1][1] - line[0][1])

    s = math.sin(teta)

    c = math.cos(teta)

    poly = [
        [line[0][0] - d * s, line[0][1] - d * c],
        [line[0][0] - d * c, line[0][1] + d * s],
        [line[1][0] - d * c, line[1][1] + d * s],
        [line[1][0] + d * s, line[1][1] + d * c],
        [line[1][0] + d * c, line[1][1] - d * s],
        [line[0][0] + d * c, line[0][1] - d * s],
        [line[0][0] - d * s, line[0][1] - d * c]
    ]

    return poly


def convertMultiLineToPoly(multiLine, d=0.2) :
    # for each line create a polygon and add it to polyList
    polyList = []

    for line in multiLine :
        polyList.append(convertLineIntoPolygon(line))

    geoPolygon = {
        "type" : "Polygon",
        "coordinates" : polyList
    }

    return geoPolygon


def get_cmap(n, name='hsv'):
    """
    Returns a function that maps each index in 0, 1, ..., n-1 to a distinct
    RGB color; the keyword argument name must be a standard mpl colormap name.
    """
    return plt.cm.get_cmap(name, n)


def createAXNFigure( showWorld=True) :
    # plt.figure(figNum)
    # world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    world = gpd.read_file("geospatial/EuropeanCoastline/Europe Coastline (Polygone).shp")
    world.to_crs(epsg=4326, inplace=True)

    ax = world.plot(figsize=(10, 6))
    plt.axis([-20, 15, 40, 60])
    return ax


def plotLineString(ax, json, color=GRAY, alpha=1, label=None) :
    """
    :param ax: gets the plot ax as argument
    :param json: trajectory's json in order to convert it to LineString and plot it
    :param color: trajectory color
    :param alpha: trajectory alpha (we use it to show overlapping trajectories)
    :return: void
    """
    line = LineString(json["coordinates"])
    x, y = line.xy
    ax.plot(x, y, color=color, linewidth=3, solid_capstyle='round', zorder=1, alpha=alpha, label=label)


def insertAISData(insertDoc, isMany=True) :
    connection = connectMongoDB()

    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation

    # insert data based on whether it is many or not
    if isMany :
        collection.insert(insertDoc)
    else :
        collection.insert_one(insertDoc)

    # printData(collection)


def insertPortData(insertDoc, isMany=True) :
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


def insertFishingPortData(insertDoc, isMany=True) :
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


def insertFullDetailedPortsData(insertDoc, isMany=True) :
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


def insertWorldSeas(data, isMany=True) :
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


def getAllAisIds() :
    connection = connectMongoDB()

    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.world_seas
    document_ids = collection.find().distinct('_id')  # list of all ids
    return document_ids


def printData(collection) :
    # Printing the data inserted
    cursor = collection.find()
    for record in cursor :
        print(record)


def findShipTrajectory(mmsi=240266000, tsFrom=1448988894, tsTo=1449075294, collection=None) :
    """

    :param mmsi:
    :param tsFrom:
    :param tsTo:
    :return:
    """

    if collection is None :
        connection = connectMongoDB()

        # connecting or switching to the database
        db = connection.marine_trafic

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation2

    # create mongo aggregation pipeline
    pipeline = [
        {"$match" : {'mmsi' : mmsi, 'ts' : {"$gte" : tsFrom, "$lte" : tsTo}}},
        {"$group" : {"_id" : "$mmsi", "ts" : {"$push" : "$ts"}, "total" : {"$sum" : 1},
                     "location" : {"$push" : "$location.coordinates"}}}
    ]

    results = collection.aggregate(pipeline)
    dictlist = queryResultToDictList(results)

    # print(json.dumps(dictlist, sort_keys=False, indent=4))

    # convert point list into MultiPoint
    return pointsListToMultiPoint(dictlist[0]["location"])


def findTrajectoriesForMatchAggr(matchAggregation, collection=None, doPlot=False, logResponse=False) :
    """
        This helper func is used to find trajectories by givent match aggregation

    :param matchAggregation: mongo $match aggregation json
    :param collection: collection on which we have establish a connection default value None
    :param doPlot: bool flag which check if plot needed or not
    :param logResponse: bool flag which check if log response on console needed or not
    :return: returns a dict with the folowing format:
        {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}}
    """
    start_time = time.time()

    if collection is None :
        connection = connectMongoDB()
        # connecting or switching to the database
        db = connection.marine_trafic

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation2

    # create mongo aggregation pipeline
    pipeline = [
        matchAggregation,
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}}},
        {"$sort" : {'total' : -1}}
    ]

    # execute query
    results = collection.aggregate(pipeline)
    dictlist = queryResultToDictList(results)
    print("--- %s seconds ---" % (time.time() - start_time))
    if logResponse :
        print(json.dumps(dictlist, sort_keys=False, indent=4))

    # check if plot needed
    if doPlot :
        ax = createAXNFigure()
        for ship in dictlist :
            trajj = pointsListToMultiPoint(ship["location"])
            if 2 < len(trajj["coordinates"]) :
                plotLineString(ax, trajj, color=GRAY, alpha=0.5)

        plt.show()

    return dictlist


def findPolyFromSeas(seaName="Celtic Sea") :
    start_time = time.time()

    connection = connectMongoDB()
    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.world_seas

    results = collection.find_one({"properties.NAME" : seaName})
    print("--- %s seconds ---" % (time.time() - start_time))

    return results


def findPort(portName='Brest') :
    start_time = time.time()
    connection = connectMongoDB()
    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.world_port_geo

    results = collection.find_one({"properties.libelle_po" : portName})
    print("--- %s seconds ---" % (time.time() - start_time))

    return results


def findPointsForMatchAggr(geoNearAgg, matchAgg,  k_near=None, collection=None, doPlot=False, logResponse=False, queryTitle=None) :
    """
           This helper func is used to find points (ship pings) by givet match aggregation

       :param queryTitle:
       :param matchAggregation: mongo $match aggregation json
       :param collection: collection on which we have establish a connection default value None
       :param doPlot: bool flag which check if plot needed or not
       :param logResponse: bool flag which check if log response on console needed or not
       :return: returns a dict with the folowing format:
           {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}}
       """
    start_time = time.time()

    if collection is None :
        connection = connectMongoDB()
        # connecting or switching to the database
        db = connection.marine_trafic

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation2

    # adds main aggregations to pipeline
    pipeline = [
        geoNearAgg,
        matchAgg,
    ]

    # adds limit aggrigation if exists
    if k_near is not None:
        pipeline.append({"$limit": k_near})

    # add group aggregation in order to display them
    pipeline.append({"$group" : {"_id" : "$mmsi","location" : {"$push" : "$location.coordinates"}}})

    # pipeline = [{
    #     "$geoNear" : {"near" : point,
    #                   "distanceField" : "dist.calculated",
    #                   "minDistance" : nautical_mile_in_meters * 10,
    #                   "maxDistance" : nautical_mile_in_meters * 30,
    #                   "spherical" : True, "key" : "location"}},
    #     {"$match" : {'ts' : {"$gte" : 1448988894, "$lte" : 1449075294}}},
    #     {"$group" : {"_id" : "$mmsi","location" : {"$push" : "$location.coordinates"}}},
    #
    # ]

    # execute query
    results = collection.aggregate(pipeline)
    dictlist = queryResultToDictList(results)
    print("--- %s seconds ---" % (time.time() - start_time))
    if logResponse :
        print(json.dumps(dictlist, sort_keys=False, indent=4))

    # check if plot needed
    if doPlot :
        print("---  PLOTTING ---")

        ax = createAXNFigure()

        # get n (ships) + 1 (point) random colors
        cmap = get_cmap(len(dictlist)+1)

        # plot points
        point = geoNearAgg["$geoNear"]["near"]
        ax.plot(point["coordinates"][0], point["coordinates"][1], marker='x', alpha=0.5, c=cmap(0), label="Target Point")

        # plot pings
        for index, ship in enumerate(dictlist) :
            isFirst = True
            for ping in ship["location"]:
                ax.plot(ping[0], ping[1], marker='o', alpha=0.5, c=cmap(index+1), label=ship["_id"] if isFirst else None)
                isFirst = False

        ax.legend(loc='center left', title='Ship MMSI', bbox_to_anchor=(1, 0.5), ncol= 1 if len(dictlist) < 10 else int(len(dictlist)/10))
        plt.title(queryTitle)
        plt.xlabel("Latitude")
        plt.ylabel("Longitude")
        plt.show()

    return dictlist


def findShipsNearPoint(point, tsFrom=None, tsTo=None, k_near=None, collection=None, doPlot=False, logResponse=False,
                       queryTitle=None) :
    start_time = time.time()
    if collection is None :
        connection = connectMongoDB()
        # connecting or switching to the database
        db = connection.marine_trafic

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation2

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

    dictlist = queryResultToDictList(results)
    print("--- %s seconds ---" % (time.time() - start_time))

    print(json.dumps(dictlist, sort_keys=False, indent=4))

    if logResponse :
        print(json.dumps(dictlist, sort_keys=False, indent=4))

    # check if plot needed
    if doPlot :
        print("---  PLOTTING ---")

        ax = createAXNFigure()

        # get n (ships) + 1 (point) random colors
        cmap = get_cmap(len(dictlist) + 1)

        # plot points
        ax.plot(point["coordinates"][0], point["coordinates"][1], marker='x', alpha=0.5, c=cmap(0),
                label="Target Point")

        # plot pings
        for index, ship in enumerate(dictlist) :
            ax.plot(ship["location"][0], ship["location"][1], 'ro', alpha=0.5)

        plt.title(queryTitle)
        plt.xlabel("Latitude")
        plt.ylabel("Longitude")
        plt.show()

    return dictlist


def findTrajectoryByVesselsFlag(country='Greece', collection=None) :
    start_time = time.time()
    if collection is None :
        connection = connectMongoDB()
        # connecting or switching to the database
        db = connection.marine_trafic

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation2

        # create mongo aggregation pipeline
    pipeline = [
        {"$match" : {{'ship_metadata.mmsi_country.country' : country}}},
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}}},
        {"$sort" : {'total' : -1}}
    ]

    # execute query
    results = collection.aggregate(pipeline)
    dictlist = queryResultToDictList(results)
    print("--- %s seconds ---" % (time.time() - start_time))

    return dictlist


def givenTrajectoryFindSimilar(trajectory, tsFrom=1448988894, tsTo=1449075294) :
    """
        Given a trajectory and a time interval find other similar trajectories

        step1:
        convert trajectory from multiline into polygon
        step2:
        plot trajectory and the polygon created based on it
        step3:
        plot all returned trajectories

    :param trajectory: is a multiLine geo json obj
    :param tsFrom: timestamp form in unix
    :param tsTo: timestamp to in unix
    :return: a list of similar trajectories
    """
    start_time = time.time()

    connection = connectMongoDB()
    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation2

    # step 1
    poly = convertLineStringToPolygon(trajectory["coordinates"])

    # create mongo aggregation pipeline
    pipeline = [
        {"$match" : {"location" : {"$geoWithin" : {"$geometry" : poly}}, 'ts' : {"$gte" : tsFrom, "$lte" : tsTo}}},
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}}},
        {"$sort" : {'total' : -1}}
    ]

    # execute query
    results = collection.aggregate(pipeline)
    dictlist = queryResultToDictList(results)
    print("--- %s seconds ---" % (time.time() - start_time))

    print(json.dumps(dictlist, sort_keys=False, indent=4))

    # step 2
    ax = createAXNFigure()
    # world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    # ax = world.plot(figsize=(10, 6))
    # plt.axis([-30, 15, 30, 70])

    plotLineString(ax, trajectory, color=RED)
    # plot poly
    ax.add_patch(PolygonPatch(poly, fc=BLUE, ec=BLUE, alpha=0.5, zorder=2))
    # ax.axis('scaled')

    # step 3
    for ship in dictlist :
        trajj = pointsListToMultiPoint(ship["location"])

        if 2 < len(trajj["coordinates"]) and trajj["coordinates"] != trajectory["coordinates"] :
            plotLineString(ax, trajj, color=GRAY, alpha=0.5)

    plt.show()


def findPingsPerPoint(point, collection=None) :
    if collection is None :
        connection = connectMongoDB()
        # connecting or switching to the database
        db = connection.marine_trafic

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation2

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
    dict = queryResultsToDict(results)
    return dict


def findTrajectoriesFromPoints(pointsList) :
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

    connection = connectMongoDB()
    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation2

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

    print(json.dumps(resultsList, sort_keys=False, indent=4))

    # step 2:
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
                if t >= (21600 * len(resultsList)) + validTS[-1] :
                    validTS.append(t)
            print(validTS)

            # get first ts
            isValid = True
            for ts in validTS :
                for count, d in enumerate(resultsList[1 :]) :
                    if not any(ts < t <= ts + (21600 * (count + 1)) for t in d[key]["ts"]) :
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
            findShipTrajectory(mmsi=pair["mmsi"],
                               tsFrom=pair["ts"],
                               tsTo=pair["ts"] + (21600 * len(pointsList)),
                               collection=collection)
        )

    print(json.dumps(trajectories, sort_keys=False, indent=4))
    print("--- %s seconds ---" % (time.time() - start_time))

    # step 5 plot trajectories
    ax = createAXNFigure()
    # get n (ships) + points list len  random colors
    cmap = get_cmap(len(trajectories) + len(pointsList))

    # plot points
    for index, point in enumerate(pointsList):
        ax.plot(point["coordinates"][0], point["coordinates"][1], marker='x', alpha=1, markersize=12, c=cmap(index), label="point {0}".format(index))

    # plot trajectories
    for i,trajj in enumerate(trajectories) :
        if 2 < len(trajj["coordinates"]) :
            plotLineString(ax, trajj, color=cmap(i+3), alpha=0.5, label=validMMSITimePair[i]["mmsi"])  # alpha 0.5 gia na doume overlaps


    ax.legend(loc='center left', title='Ship MMSI', bbox_to_anchor=(1, 0.5),
              ncol=1 if len(trajectories) < 10 else int(len(trajectories) / 10))
    plt.title("Find Trajectories that pass near specific points in specific time interval")
    plt.xlabel("Latitude")
    plt.ylabel("Longitude")
    plt.show()


if __name__ == '__main__' :
    """
        Bullet 1:
        SQL Like/ Relational queries
        
        query1: Find trajectories for all greek flag vessels (by word)
        very slow because we dont have index on it
        local time on first run: --- 296.6688349246979 seconds ---
        local time on second run: --- 148.310476064682 seconds ---
        
        @SOS:- Afou exoume san use case query me country/country code prepei na 
        kanoume store collection me ship codes
    
        query2: same query as above but now using country code (for greece is 237 )
        
        query3: vres ola ta fishing vessels me galiki simea pou kinounte me 
    """
    matchAggregation = {"$match" : {'ship_metadata.mmsi_country.country' : 'Greece'}}
    # findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, logResponse=True)

    """
        Bullet 2: Spatial queries
        Range, k-nn, distance join queries
        
        query1: find trajectories for all ships with greek flag in Celtic Sea 
        (den etrekse pote poli megali perioxi + oti argi pou argei gia tin simea)
        
        RANGE QUERIES
        query2: find all ships that moved in range from 10 to 50 sea miles from Burst port
        (this query can be used for any point and for any min and max range )
        [-4.47530,48.3827]

        
        query3: 
        find k closest ship sigmas to a point (test point:" coordinates" : [-4.1660385,50.334972]) (k=5)
    """
    # query1
    poly = findPolyFromSeas()
    # matchAggregation = {"$match": {"ship_metadata.mmsi_country.country": "Greece", "location": {"$geoWithin": {"$geometry": poly["geometry"]}}}}
    # findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, logResponse=True)

    # query2 # ARGEI POLI
    port_point = findPort()
    matchAggregation = {
        "$geoNear" : {"near" : {"type" : "Point", "coordinates" : [-4.475309812300752, 48.38273389573341]},
                      "distanceField" : "dist.calculated",
                      "minDistance" : nautical_mile_in_meters * 10,
                      "maxDistance" : nautical_mile_in_meters * 30,
                      "spherical" : True, "key" : "location"}}
    # findPointsForMatchAggr(matchAggregation)

    # query 3
    point = {"type" : "Point", "coordinates" : [-4.1660385, 50.334972]}
    # findShipsNearPoint(point, k_near=5)

    """
        Bullet 3: Spatio-temporal queries
        Range, k-nn, distance join queries
        
        query1: Find all ships that moved in range from 10 to 50 sea miles from Burst port for one day time range
        for 6 hours time interval
        (to many ping it took a lot of time to find them all 
        first run --- 408.56871485710144 seconds ---
        second run --- 220.98046803474426 seconds --- (great improvement))

        query2: find k closest ship sigmas to a point (test point:" coordinates" : [-4.1660385,50.334972]) (k=5)
        in one day interval (this point is at the entry of p)
    """
    # query1
    port_point = findPort()
    geoNearAgg = {"$geoNear" : {"near" : {"type" : "Point", "coordinates" : [-4.47530, 48.3827]},
                      "distanceField" : "dist.calculated",
                      "minDistance" : nautical_mile_in_meters * 10,
                      "maxDistance" : nautical_mile_in_meters * 30,
                      "spherical" : True, "key" : "location"}}

    matchAgg = {"$match" : {'ts' : {"$gte" : 1448988894, "$lte" : 1448988894 + (2*one_hour_in_unix_time)}}}

    # findPointsForMatchAggr(geoNearAgg,matchAgg,doPlot=True,
    #                        queryTitle="Find all ships that moved in range from 10 to 50 sea miles from Burst port for 2 hours interval")


    # findPointsForMatchAggr(point, doPlot=True, queryTitle="Find all ships that moved in range from 10 to 50 sea miles "
    #                                                       "from Burst port")

    # query 2
    point = {"type" : "Point", "coordinates" : [-4.1660385, 50.334972]}
    geoNearAgg = {"$geoNear" : {"near" : point,
                                "distanceField" : "dist.calculated",
                                # "minDistance" : nautical_mile_in_meters * 10,
                                "maxDistance" : nautical_mile_in_meters * 30,
                                "spherical" : True, "key" : "location"}}

    matchAgg = {"$match" : {'ts' : {"$gte" : 1448988894, "$lte" : 1449075294}}}

    findPointsForMatchAggr(geoNearAgg, matchAgg, k_near=20, doPlot=True,
                           queryTitle="find k closest ship sigmas to a point (k=5) in one day interval")

    # findShipsNearPoint(point,
    #                    tsFrom=1448988894,
    #                    tsTo=1449075294,
    #                    k_near=5,
    #                    doPlot=True,
    #                    logResponse=True,
    #                    queryTitle="find k closest ship sigmas to a point (k=5) in one day interval")

    """
        Bullet 4.2:
        Given a trajectory, find similar trajectories (threshold-based, k-most similar)
    """
    # trajectory = findShipTrajectory()
    # givenTrajectoryFindSimilar(trajectory)

    """
        Bullet 4.3:
        Complex queries: find trajectories that passed through A,
        then through B (up to X hours later), then through C (up to 6 hours later)
        SOS:- for simplicity we assume that time from one point to another is 6 hours 
        but we have extended the number of points from 3 to N
    """
    pointsList = [
        {"type" : "Point", "coordinates" : [-7.072965, 47.371117]},
        {"type" : "Point", "coordinates" : [-7.0410814, 47.4131]},
        {"type" : "Point", "coordinates" : [-6.868215, 47.6239]}
    ]
    # findTrajectoriesFromPoints(pointsList)
