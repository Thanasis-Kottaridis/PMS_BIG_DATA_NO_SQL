"""
    This file will be used for handling opperations with mongoDB such as
    1) creating and updating collections
    2) executing supporting queries
    TODO:- ADD MORE FUNCTIONALITIES HERE

    (establish connection link: https://kb.objectrocket.com/mongo-db/how-to-insert-data-in-mongodb-using-python-683)
"""
from pymongo import MongoClient
import numpy as np
import json
import math
import geopandas as gpd
import matplotlib.pyplot as plt
from descartes import PolygonPatch
from shapely.geometry import LineString
import shapely.geometry as sg
import geog
import time
from mongo import mongoConnector as connector
from geospatial import geoDataPreprocessing

# CONSTS
BLUE = '#6699cc'
GRAY = '#999999'
RED = '#B20000'
nautical_mile_in_meters = 1852
one_hour_in_unix_time = 3600


def connectMongoDB() :
    try :
        # conect to mongo server
        # connect = MongoClient("mongodb://mongoadmin2:mongoadmin@83.212.117.74/admin")
        # connect to local mongo db
        connect = MongoClient()
        # print("Connected Successfully!")
        return connect
    except :
        print("Could not connect MongoDB")


def queryResultToDictList(results) :
    """
    this helper func converts pymongo query results into a list of dictionaries

    :param dictlist:
    :param results: the results of pymongo query
    :return: returns a list of dictionaries
    """
    dictlist = []

    for doc in results :
        dictlist.append(doc)

    return dictlist


def queryResultToDictList(results, dictlist=[]) :
    """
    this helper func converts pymongo query results into a list of dictionaries

    :param dictlist:
    :param results: the results of pymongo query
    :return: returns a list of dictionaries
    """

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
        This helper func creates a polygon from a linestring geo json
    :param line: line string geojson
    :param d: threshold distance (2*d is the width of poly)
    :return :
     - geoPolygon polygon geojson,
     - dilated:shapely outer polygon,
     - eroded: shapely inner poly
    """

    line = LineString(line)
    dilated = line.buffer(d)
    eroded = dilated.buffer(d / 2)

    geoPolygon = sg.mapping(dilated)
    print(json.dumps(geoPolygon, sort_keys=False, indent=4))

    return geoPolygon, dilated, eroded


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


def get_cmap(n, name='hsv') :
    """
    Returns a function that maps each index in 0, 1, ..., n-1 to a distinct
    RGB color; the keyword argument name must be a standard mpl colormap name.
    """
    return plt.cm.get_cmap(name, n)


def createAXNFigure() :
    # geopandas basic world map with out details
    # world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    world = gpd.read_file("geospatial/EuropeanCoastline/Europe Coastline (Polygone).shp")
    world.to_crs(epsg=4326, inplace=True)  # convert axes tou real world coordinates

    ax = world.plot(figsize=(10, 6))
    plt.axis([-20, 15, 40, 60])  # set plot bounds
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
    collection = db.ais_navigation_shard

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


def insertTestPolyData(insertDoc, isMany=True) :
    connection = connectMongoDB()

    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.query_polygons

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


def insertCountries(insertDoc, isMany=True) :
    connection = connectMongoDB()

    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.countries

    # insert data based on whether it is many or not
    if isMany :
        collection.insert(insertDoc)
    else :
        collection.insert_one(insertDoc)


def getAllAisMMSI() :
    connection = connectMongoDB()

    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation2
    document_ids = collection.find().distinct('mmsi')  # list of all ids
    return document_ids


def getShipsByCountry(countryName, db=None) :
    start_time = time.time()

    if db is None :
        connection = connectMongoDB()

        # connecting or switching to the database
        db = connection.marine_trafic

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
    dictlist = queryResultToDictList(results, dictlist=[])

    # print(json.dumps(dictlist, sort_keys=False, indent=4))

    # convert point list into MultiPoint
    return pointsListToMultiPoint(dictlist[0]["location"])


def findTrajectoriesForMatchAggr(matchAggregation, collection=None, doPlot=False, withPoly=None, logResponse=False) :
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

        # plot polygon if exists
        if withPoly is not None :
            # plot poly
            ax.add_patch(
                PolygonPatch(withPoly, fc=BLUE, ec=BLUE, alpha=0.5, zorder=2, label="Trajectories Within Polygon"))

        # get n (ships) + points list len  random colors
        cmap = get_cmap(len(dictlist))

        #  plot trajectories
        for i, ship in enumerate(dictlist) :
            trajj = pointsListToMultiPoint(ship["location"])

            if 2 < len(trajj["coordinates"]) :
                plotLineString(ax, trajj, color=cmap(i), alpha=0.5, label=ship["_id"])

        ax.legend(loc='center left', title='Ship MMSI', bbox_to_anchor=(1, 0.5),
                  ncol=1 if len(dictlist) < 10 else int(len(dictlist) / 10))

        if len(dictlist) < 50 :  # show legend
            plt.title("Trajectories")
        plt.ylabel("Latitude")
        plt.xlabel("Longitude")
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


def findPointsForMatchAggr(geoNearAgg, matchAgg=None, k_near=None, collection=None, allowDiskUse=False, doPlot=False,
                           logResponse=False,
                           queryTitle=None) :
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
        geoNearAgg
    ]

    # adds match agg if exists
    if matchAgg is not None :
        pipeline.append(matchAgg)

    # adds limit aggrigation if exists
    if k_near is not None :
        pipeline.append({"$limit" : k_near})

    # add group aggregation in order to display them
    pipeline.append({"$group" : {"_id" : "$mmsi", "location" : {"$push" : "$location.coordinates"}}})

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
    results = collection.aggregate(pipeline, allowDiskUse=allowDiskUse)
    dictlist = queryResultToDictList(results)
    print("--- %s seconds ---" % (time.time() - start_time))
    if logResponse :
        print(json.dumps(dictlist, sort_keys=False, indent=4))

    # check if plot needed
    if doPlot :
        print("---  PLOTTING ---")

        ax = createAXNFigure()

        # get n (ships) + 1 (point) random colors
        cmap = get_cmap(len(dictlist) + 1)

        # plot points
        if geoNearAgg is not None:
            point = geoNearAgg["$geoNear"]["near"]
            ax.plot(point["coordinates"][0], point["coordinates"][1], marker='x', alpha=0.5, c=cmap(0),
                    label="Target Point")

        # plot pings
        for index, ship in enumerate(dictlist) :
            isFirst = True
            for ping in ship["location"] :
                ax.plot(ping[0], ping[1], marker='o', alpha=0.5, c=cmap(index + 1),
                        label=ship["_id"] if isFirst else None)
                isFirst = False

        ax.legend(loc='center left', title='Ship MMSI', bbox_to_anchor=(1, 0.5),
                  ncol=1 if len(dictlist) < 10 else int(len(dictlist) / 10))
        plt.title(queryTitle)
        plt.ylabel("Latitude")
        plt.xlabel("Longitude")
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
            ax.plot(ship["location"]["coordinates"][0], ship["location"]["coordinates"][1], 'ro', alpha=0.5)

        plt.title(queryTitle)
        plt.y("Latitude")
        plt.xlabel("Longitude")
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
    fv = np.vectorize(calculatePointsDistance, signature='(n),(n)->()')
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

    connection = connectMongoDB()
    # connecting or switching to the database
    db = connection.marine_trafic

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation2

    # step 1
    poly, shpOuterPoly, shpInnerPoly = convertLineStringToPolygon(trajectory["coordinates"], d=d)

    # create mongo aggregation pipeline
    pipeline = [
        {"$match" : {"location" : {"$geoWithin" : {"$geometry" : poly}}, 'ts' : {"$gte" : tsFrom, "$lte" : tsTo}}},
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}}},
        {"$sort" : {'total' : -1}}
    ]

    # execute query
    results = collection.aggregate(pipeline)
    dictlist = queryResultToDictList(results)

    print(json.dumps(dictlist, sort_keys=False, indent=4))

    # step 2 checks if it is a k-most query or a threshold based
    if k_most > 0 :
        dictlist = ext_givenTrajectoryFindSimilar(dictlist, trajectory["coordinates"], k_most)

    print("--- %s seconds ---" % (time.time() - start_time))

    # step 2
    ax = createAXNFigure()

    plotLineString(ax, trajectory, color='k', alpha=0.2, label="Given Trajectory")
    # plot poly
    ax.add_patch(PolygonPatch(poly, fc=BLUE, ec=BLUE, alpha=0.5, zorder=2, label="Trajectory Polygon with (d=0.2)"))
    # ax.axis('scaled')

    # get n (ships) + points list len  random colors
    cmap = get_cmap(len(dictlist))

    #  plot trajectories
    for i, ship in enumerate(dictlist) :
        trajj = pointsListToMultiPoint(ship["location"])

        if 2 < len(trajj["coordinates"]) and trajj["coordinates"] != trajectory["coordinates"] :
            plotLineString(ax, trajj, color=cmap(i), alpha=0.5, label=ship["_id"])

    # plot step
    ax.legend(loc='center left', title='Ship MMSI', bbox_to_anchor=(1, 0.5),
              ncol=1 if len(dictlist) < 10 else int(len(dictlist) / 10))
    plt.title("Find Trajectories that pass near specific points in specific time interval")
    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
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
    for index, point in enumerate(pointsList) :
        ax.plot(point["coordinates"][0], point["coordinates"][1], marker='x', alpha=1, markersize=12, c=cmap(index),
                label="point {0}".format(index))

    # plot trajectories
    for i, trajj in enumerate(trajectories) :
        if 2 < len(trajj["coordinates"]) :
            plotLineString(ax, trajj, color=cmap(i + 3), alpha=1,
                           label=validMMSITimePair[i]["mmsi"])  # alpha 0.5 gia na doume overlaps

    ax.legend(loc='center left', title='Ship MMSI', bbox_to_anchor=(1, 0.5),
              ncol=1 if len(trajectories) < 10 else int(len(trajectories) / 10))
    plt.title("Find Trajectories that pass near specific points in specific time interval")
    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
    plt.show()


def calculatePointsDistance(coords_1, coords_2) :
    """
    Vectorized helper func for calculating distance between 2 geo points

    source:
     https://towardsdatascience.com/heres-how-to-calculate-distance-between-2-geolocations-in-python-93ecab5bbba4

    :param coords_1: lat long of first point
    :param coords_2: lat log of second point
    :return: distance between points
    """

    r = 6371
    lat1 = coords_1[0]
    lat2 = coords_2[0]
    lon1 = coords_1[1]
    lon2 = coords_2[1]
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    a = np.sin(delta_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    res = r * (2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)))
    return np.round(res, 2)
    # approach using geopy
    # return geopy.distance.geodesic(coords_1, coords_2).km


def comparePoints(point1, point2, d):
    if calculatePointsDistance(point1, point2) <= d:
        return True
    else:
        return False


def comparePointSets(pointSet1, pointSet2, d) :
    """
    this helper func is calculating the distance per point between 2 point sets
     (trajectories or points observed in a polygon)

    step 1) create a vectorized 2d numpy array by applying calculatePointsDistance() helper function
    step 2) find 2d elements with values less than d, create a pair and add it in to a list
    step 3) calculate average distance for this 2 trajectories

    :param pointSet1: list of lat, log pairs
    :param pointSet2: list of lat, log pairs
    :param d: distance threshold in kilometers.
    :return m: trajectory mean distance. The mean distance for all to all points of 2 point sets
    """
    start_time = time.time()

    ps1 = np.array(pointSet1)
    ps2 = np.array(pointSet2)

    # to signature sto np.vectorize leei gia kathe ena apo ta 2 (2d vectors me to (n),(n)) pare ola ta items (n) tis kathe "gramis"
    # kai tha epistrepsei ena return me to "()"
    # fv = np.vectorize(calculatePointsDistance, signature='(n),(n)->()')
    # r = fv(ps1[:, np.newaxis], ps2)
    # # r = geopy.distance.geodesic(ps1, ps2).km  #a[:, None] + b * 2
    #
    # # calculate mean
    # # m = np.mean(r)
    #
    # # check if tranjectory is closer than threshold
    # rmatch = np.vectorize(lambda i: i <= d)
    # r_new = rmatch(r)
    #
    # # get i, j of ture values
    # r = np.argwhere(r_new)# r[r_new]
    # # rmatch = np.vectorize(lambda i : i <= d)
    #
    fv = np.vectorize(lambda i , j: comparePoints(i, j, d), signature='(n),(n)->()')
    r = fv(ps1[:, np.newaxis], ps2)
    r = np.argwhere(r)

    p1i, p2i = zip(*r)
    # r = r[~np.isnan(r)]
    # r = r[r.astype(bool)]

    # get mean of 2d array
    print(len(p1i))
    print("--- %s seconds ---" % (time.time() - start_time))
    print(len(p2i))

    return p1i, p2i


def getEnrichBoundingBox(pointsList, theta=0) :
    """
    This helper func calculates the enrich bounding box of a polygon.
    and returns its bottom left and upper right coordinates

    :param poly:
    :param theta:
    :return: the polygon geo json of bounding box
    """
    x_coordinates, y_coordinates = zip(*pointsList)

    MBB_points = [
                [min(x_coordinates), min(y_coordinates)],
                [min(x_coordinates), max(y_coordinates)],
                [max(x_coordinates), max(y_coordinates)],
                [max(x_coordinates), min(y_coordinates)],
                [min(x_coordinates), min(y_coordinates)],
            ]

    enritch_MBB_points_x = []
    enritch_MBB_points_y = []
    for point in MBB_points:
        d = theta * 1000  # meters
        p = sg.Point(point)
        n_points = 5
        angles = np.linspace(0, 360, n_points)
        polygon = geog.propagate(p, angles, d)

        xs, ys = zip(*polygon)
        print(calculatePointsDistance(point,
                                      polygon[3]))

        enritch_MBB_points_x.extend([min(xs), max(xs)])
        enritch_MBB_points_y.extend([min(ys), max(ys)])

    return {
        "type" : "Polygon",
        "coordinates" : [
            [
                [min(enritch_MBB_points_x), min(enritch_MBB_points_y)],
                [min(enritch_MBB_points_x), max(enritch_MBB_points_y)],
                [max(enritch_MBB_points_x), max(enritch_MBB_points_y)],
                [max(enritch_MBB_points_x), min(enritch_MBB_points_y)],
                [min(enritch_MBB_points_x), min(enritch_MBB_points_y)],
            ]
        ]
    }


def generatePolyFromPoint(p, d, polyPoints):
    p = sg.Point(p)
    angles = np.linspace(0, 360, polyPoints)
    polygon = geog.propagate(p, angles, d)

    # ax.add_patch(PolygonPatch(test, fc=BLUE, ec=BLUE, alpha=0.5, zorder=2, label="Polygon 1"))
    # plt.ylabel("Latitude")
    # plt.xlabel("Longitude")
    # plt.show()
    return {
        "type" : "Polygon",
        "coordinates" : [polygon]
    }


def distanceJoinPolyQuery(p1, p2, theta, timeFrom=None, timeTo=None, allowDiskUse=False, collection=None) :
    """
    Algorithm logic:
    @MAIN IDEA
    step 1: get all points in poly1 and poly2
    step 2: for each poly filter the points that their distance from the other poly is greater than theta
    because their is no way for point that have distance greater than theta to have a valid pair.

    @ TEST IDEA.
    step 1: get only points that are likely to be valid from poly1 and poly2
        how to do that?
        by creating a bounding box of each poly greater than 2theta on each side.
        then query all points of poly1 that are inside the bounding box of poly2
        and via versa

    :param p1:
    :param p2:
    :param theta:
    :param timeFrom:
    :param timeTo:
    :param allowDiskUse:
    :param collection:
    :return:
    """

    start_time = time.time()
    if collection is None :
        connection = connectMongoDB()
        # connecting or switching to the database
        db = connection.marine_trafic

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation2

    # group all the pings in a list no requirement for more information
    groupAgg = {"$group" : {"_id" : None, "location" : {"$push" : "$location.coordinates"}}}

    poly_dictList = []
    # for i, poly in enumerate(polyList):

    # crate a poly from target poly border as line
    # sos: adding 0.1 distance pou poly to reduce failure probability
    # poly1_expanded, shpOuterPoly, shpInnerPoly = convertLineStringToPolygon(poly1["coordinates"][0], d=theta + 0.1)
    # poly2_expanded, shpOuterPoly, shpInnerPoly = convertLineStringToPolygon(poly2["coordinates"][0], d=theta + 0.1)
    poly1_expanded = getEnrichBoundingBox(p1["coordinates"][0], theta)
    poly2_expanded = getEnrichBoundingBox(p2["coordinates"][0], theta)

    pipeline = [
        {"$match" : {
            **({'ts': {"$gte": timeFrom, "$lte": timeTo}} if timeFrom is not None and timeTo is not None else {}),
            "location" : {"$geoWithin" : {"$geometry" : p1}}
            }
        },
        {"$match" : {
            **({'ts': {"$gte": timeFrom, "$lte": timeTo}} if timeFrom is not None and timeTo is not None else {}),
            "location" : {"$geoWithin" : {"$geometry" : poly2_expanded}}
            }
        },
        groupAgg
        # TEST
        # {"$group" : {"_id" : "$mmsi", "location" : {"$push" : "$location.coordinates"}}}
    ]

    # execute query
    results = collection.aggregate(pipeline, allowDiskUse=allowDiskUse)
    poly_dictList = queryResultToDictList(results, dictlist=poly_dictList)

    pipeline = [
        {"$match" : {
            # **({'ts': {"$gte": timeFrom, "$lte": timeTo}} if timeFrom is not None and timeTo is not None else {}),
            "location" : {"$geoWithin" : {"$geometry" : p2}}
        }
        },
        {"$match" : {
            # **({'ts': {"$gte": timeFrom, "$lte": timeTo}} if timeFrom is not None and timeTo is not None else {}),
            "location" : {"$geoWithin" : {"$geometry" : poly1_expanded}}
        }
        },
        groupAgg,
        #TEST
        # {"$group" : {"_id" : "$mmsi", "location" : {"$push" : "$location.coordinates"}}}
    ]

    # execute query
    results = collection.aggregate(pipeline, allowDiskUse=allowDiskUse)
    poly_dictList = queryResultToDictList(results, dictlist=poly_dictList)

    try:
        ps1, ps2 = comparePointSets(poly_dictList[0]["location"], poly_dictList[1]["location"], theta)
        print("--- %s seconds ---" % (time.time() - start_time))
    except:
        print("no points found")

    # Draw polygons
    ax = createAXNFigure()

    # cmap = get_cmap(len(poly_dictList["location"]))

    # plot pings
    isFirstRed = True
    isFirstGreen = True
    for index, ship in enumerate(poly_dictList) :
        for pIndex, ping in enumerate(ship["location"]) :
            if(index == 0 and pIndex in ps1) or (index == 1 and pIndex in ps2):
                ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.5, c='g', label="matching points" if isFirstGreen else None)
                isFirstGreen = False
            else:
                ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.5, c='r', label="non matching points" if isFirstRed else None)
                isFirstRed = False
            # ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.5, c= 'g' if( index == 0 and pIndex in ps1) or (index == 1 and pIndex in ps2) else 'r',
            #         label=ship["_id"] if isFirst else None)


    # plot lines



    ax.add_patch(PolygonPatch(poly1_expanded, fc="k", ec="k", alpha=0.2, zorder=2, label="Polygon 1 Expanded"))
    ax.add_patch(PolygonPatch(poly1, fc=BLUE, ec=BLUE, alpha=0.5, zorder=2, label="Polygon 1"))
    ax.add_patch(PolygonPatch(poly2_expanded, fc="r", ec="r", alpha=0.2, zorder=2, label="Polygon 2 Expanded"))
    ax.add_patch(PolygonPatch(poly2, fc=GRAY, ec=GRAY, alpha=0.5, zorder=2, label="Polygon 2"))

    ax.legend(loc='center left', title='Plot Info', bbox_to_anchor=(1, 0.5), ncol=1)
    plt.title("Distance Join Query using Polygons and Theta: {}".format(theta))
    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
    plt.show()


def distanceJoinRectQuery(rect1, rect2, theta, timeFrom=None, timeTo=None, allowDiskUse=False, collection=None):
    """
       :param rect1:
       :param rect2:
       :param theta:
       :param timeFrom:
       :param timeTo:
       :param allowDiskUse:
       :param collection:
       :return:
       """

    start_time = time.time()
    if collection is None :
        connection = connectMongoDB()
        # connecting or switching to the database
        db = connection.marine_trafic

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation2

    # group all the pings in a list no requirement for more information
    groupAgg = {"$group" : {"_id" : None, "location" : {"$push" : "$location.coordinates"}}}

    poly_dictList = []
    # for i, poly in enumerate(polyList):

    # crate a poly from target poly border as line
    # sos: adding 0.1 distance pou poly to reduce failure probability
    # poly1_expanded, shpOuterPoly, shpInnerPoly = convertLineStringToPolygon(poly1["coordinates"][0], d=theta + 0.1)
    # poly2_expanded, shpOuterPoly, shpInnerPoly = convertLineStringToPolygon(poly2["coordinates"][0], d=theta + 0.1)
    rect1_expanded = getEnrichBoundingBox(rect1, theta)
    rect2_expanded = getEnrichBoundingBox(rect2, theta)

    pipeline = [
        {"$match" : {
            **({'ts' : {"$gte" : timeFrom, "$lte" : timeTo}} if timeFrom is not None and timeTo is not None else {}),
            "location" : {"$geoWithin" : {"$box" : rect1}}
        }
        },
        {"$match" : {
            **({'ts' : {"$gte" : timeFrom, "$lte" : timeTo}} if timeFrom is not None and timeTo is not None else {}),
            "location" : {"$geoWithin" : {"$geometry" : rect2_expanded}}
        }
        },
        groupAgg
        # TEST
        # {"$group" : {"_id" : "$mmsi", "location" : {"$push" : "$location.coordinates"}}}
    ]

    # execute query
    results = collection.aggregate(pipeline, allowDiskUse=allowDiskUse)
    poly_dictList = queryResultToDictList(results, dictlist=poly_dictList)

    pipeline = [
        {"$match" : {
            **({'ts': {"$gte": timeFrom, "$lte": timeTo}} if timeFrom is not None and timeTo is not None else {}),
            "location" : {"$geoWithin" : {"$box" : rect2}}
        }
        },
        {"$match" : {
            **({'ts': {"$gte": timeFrom, "$lte": timeTo}} if timeFrom is not None and timeTo is not None else {}),
            "location" : {"$geoWithin" : {"$geometry" : rect1_expanded}}
        }
        },
        groupAgg,
        # TEST
        # {"$group" : {"_id" : "$mmsi", "location" : {"$push" : "$location.coordinates"}}}
    ]

    # execute query
    results = collection.aggregate(pipeline, allowDiskUse=allowDiskUse)
    poly_dictList = queryResultToDictList(results, dictlist=poly_dictList)

    # check if list is not empty

    if len(poly_dictList) == 2 :
        ps1, ps2 = comparePointSets(poly_dictList[0]["location"], poly_dictList[1]["location"], theta)
    else:
        print("list is empty")
    print("--- %s seconds ---" % (time.time() - start_time))

    # Draw polygons
    ax = createAXNFigure()

    cmap = get_cmap(len(poly_dictList))

    # plot pings
    isFirstRed = True
    isFirstGreen = True
    for index, ship in enumerate(poly_dictList) :
        for pIndex, ping in enumerate(ship["location"]) :
            if (index == 0 and pIndex in ps1) or (index == 1 and pIndex in ps2) :
                ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.5, c='g',
                        label="matching points" if isFirstGreen else None)
                isFirstGreen = False
            else :
                ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.5, c='r',
                        label="non matching points" if isFirstRed else None)
                isFirstRed = False
            # ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.5, c= 'g' if( index == 0 and pIndex in ps1) or (index == 1 and pIndex in ps2) else 'r',
            #         label=ship["_id"] if isFirst else None)

    rect1_x, rect1_y = zip(*rect1)
    rect2_x, rect2_y = zip(*rect2)
    rect1_poly = {
        "type" : "Polygon",
        "coordinates" : [
            [[min(rect1_x), min(rect1_y)], [min(rect1_x), max(rect1_y)], [max(rect1_x), max(rect1_y)],
             [max(rect1_x), min(rect1_y)], [min(rect1_x), min(rect1_y)]]
        ]
    }

    rect2_poly = {
        "type" : "Polygon",
        "coordinates" : [
            [[min(rect2_x), min(rect2_y)], [min(rect2_x), max(rect2_y)], [max(rect2_x), max(rect2_y)],
             [max(rect2_x), min(rect2_y)], [min(rect2_x), min(rect2_y)]]
        ]
    }

    ax.add_patch(PolygonPatch(rect1_expanded, fc="k", ec="k", alpha=0.2, zorder=2, label="Box 1 Expanded"))
    ax.add_patch(PolygonPatch(rect1_poly, fc=BLUE, ec=BLUE, alpha=0.5, zorder=2, label="Box 1"))
    ax.add_patch(PolygonPatch(rect2_expanded, fc="r", ec="r", alpha=0.2, zorder=2, label="Box 2 Expanded"))
    ax.add_patch(PolygonPatch(rect2_poly, fc=GRAY, ec=GRAY, alpha=0.5, zorder=2, label="Box 2"))

    ax.legend(loc='center left', title='Plot Info', bbox_to_anchor=(1, 0.5), ncol=1)
    plt.title("Distance Join Query using Boxes and Theta: {}".format(theta))
    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
    plt.show()


def distanceJoinSphereQuery(k1, r1, k2, r2, theta, timeFrom=None, timeTo=None, allowDiskUse=False, collection=None):
    """
    step1: check id d(k1,k2) >=< theta
    #SOS: idea den tha pernw stin defteri anazitisi osa einai mesa stin aktina gt den mas niazoun ta epikaliptomena.
    :param p1:
    :param r1:
    :param p2:
    :param r2:
    :param theta:
    :param timeFrom:
    :param timeTo:
    :param allowDiskUse:
    :param collection:
    :return:
    """
    start_time = time.time()
    if collection is None :
        connection = connectMongoDB()
        # connecting or switching to the database
        db = connection.marine_trafic

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation2

    k_dist = calculatePointsDistance(k1['coordinates'], k2['coordinates'])
    r1_extended = theta + r1
    r2_extended = theta + r2

    # check if spheres are overlapping if no use min distance on the second stage of each pipeline
    overlaping = True
    if (k_dist > r1_extended + r2_extended):
        print("No matching points for the given centers. Centers are too far.")
    elif(k_dist > r1 + r2):
        overlaping = False


    print(k_dist," ",r1_extended, " ", r2_extended)

    # group all the pings in a list no requirement for more information
    groupAgg = {"$group" : {"_id" : None, "location" : {"$push" : "$location.coordinates"}}}

    poly_dictList = []
    # pipeline stage1
    pipeline = [

        {"$geoNear" : {"near" :  k2,
                      "distanceField" : "dist.calculated",
                      "maxDistance" : 1000 * r2,
                      "spherical" : True,"key" : "location"}},
        {"$match" : {
            **({'ts' : {"$gte" : timeFrom, "$lte" : timeTo}} if timeFrom is not None and timeTo is not None else {}),
        }
        },
        {"$group" : {"_id" : "", "ids" : {"$push" : "$_id"}, }},
        # TEST
        # {"$group" : {"_id" : "$mmsi", "location" : {"$push" : "$location.coordinates"}}}
    ]

    # execute query
    results = list(collection.aggregate(pipeline, allowDiskUse=allowDiskUse))
    print(len(results[0]["ids"]))
    idsInK1 = results[0]["ids"]

    # pipeline stage1
    pipeline = [

        {"$geoNear" : {"near" : k1,
                       "distanceField" : "dist.calculated",
                       "minDistance" : 0 if overlaping else 1000 * r1,
                       "maxDistance" : 1000 * r1_extended,
                       "spherical" : True, "key" : "location"}},
        {"$match" : {
            **({'ts' : {"$gte" : timeFrom, "$lte" : timeTo}} if timeFrom is not None and timeTo is not None else {}),
           "_id": {'$in' : idsInK1}
            # "location" : {"$geoWithin" : {"$box" : rect1}}
        }
        },
        #TEST
        # {"$group" : {"_id" : "", "ids" : {"$push" : "$_id"}, }}
        groupAgg
    ]

    # execute query
    results = list(collection.aggregate(pipeline, allowDiskUse=allowDiskUse))
    poly_dictList = queryResultToDictList(results, dictlist=poly_dictList)


    # repeat sep1 and 2 for second pointset
    pipeline = [

        {"$geoNear" : {"near" :  k1,
                      "distanceField" : "dist.calculated",
                      "maxDistance" : 1000 * r1,
                      "spherical" : True, "key" : "location"}},
        {"$match" : {
            **({'ts' : {"$gte" : timeFrom, "$lte" : timeTo}} if timeFrom is not None and timeTo is not None else {}),
        }
        },
        {"$group" : {"_id" : "", "ids" : {"$push" : "$_id"}, }},
        # TEST
        # {"$group" : {"_id" : "$mmsi", "location" : {"$push" : "$location.coordinates"}}}
    ]

    # execute query
    results = list(collection.aggregate(pipeline, allowDiskUse=allowDiskUse))
    print(len(results[0]["ids"]))
    idsInK1 = results[0]["ids"]

    # pipeline stage1
    pipeline = [

        {"$geoNear" : {"near" : k2,
                       "distanceField" : "dist.calculated",
                       "minDistance" : 0 if overlaping else 1000 * r2,
                       "maxDistance" : 1000 * r2_extended,
                       "spherical" : True, "key" : "location"}},
        {"$match" : {
            **({'ts' : {"$gte" : timeFrom, "$lte" : timeTo}} if timeFrom is not None and timeTo is not None else {}),
           "_id": {'$in' : idsInK1}
            # "location" : {"$geoWithin" : {"$box" : rect1}}
        }
        },
        #TEST
        # {"$group" : {"_id" : "", "ids" : {"$push" : "$_id"}, }}
        groupAgg
    ]

    # execute query
    results = list(collection.aggregate(pipeline, allowDiskUse=allowDiskUse))
    poly_dictList = queryResultToDictList(results, dictlist=poly_dictList)

    ps1, ps2 = comparePointSets(poly_dictList[0]["location"], poly_dictList[1]["location"], theta)
    print("--- %s seconds ---" % (time.time() - start_time))

    # Draw polygons
    ax = createAXNFigure()

    # plot k
    ax.plot(k1["coordinates"][0], k1["coordinates"][1], marker='x', alpha=1, c='y',
            label="Target Point: ({:.2f},{:.2f})".format(k1["coordinates"][0], k1["coordinates"][1]))
    ax.plot(k2["coordinates"][0], k2["coordinates"][1], marker='x', alpha=1, c='k',
            label="Target Point: ({:.2f},{:.2f})".format(k2["coordinates"][0], k2["coordinates"][1]))

    # plot pings
    isFirstRed = True
    isFirstGreen = True
    for index, ship in enumerate(poly_dictList) :
        for pIndex, ping in enumerate(ship["location"]) :
            if (index == 0 and pIndex in ps1) or (index == 1 and pIndex in ps2) :
                ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.5, c='g',
                        label="matching points" if isFirstGreen else None)
                isFirstGreen = False
            else :
                ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.5, c='r',
                        label="non matching points" if isFirstRed else None)
                isFirstRed = False
            # ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.5, c= 'g' if( index == 0 and pIndex in ps1) or (index == 1 and pIndex in ps2) else 'r',
            #         label=ship["_id"] if isFirst else None)


    #Generate polygons
    circle_poly1 = generatePolyFromPoint(k1["coordinates"], r1*1000, 100)
    circle_poly1_extended = generatePolyFromPoint(k1["coordinates"], r1_extended*1000, 100)
    circle_poly2 = generatePolyFromPoint(k2["coordinates"], r2*1000, 100)
    circle_poly2_extended = generatePolyFromPoint(k2["coordinates"], r2_extended*1000, 100)

    ax.add_patch(PolygonPatch(circle_poly1_extended, fc="k", ec="k", alpha=0.2, zorder=2, label="Sphere 1 Expanded"))
    ax.add_patch(PolygonPatch(circle_poly1, fc=BLUE, ec=BLUE, alpha=0.5, zorder=2, label="Sphere 1"))
    ax.add_patch(PolygonPatch(circle_poly2_extended, fc="r", ec="r", alpha=0.2, zorder=2, label="Sphere 2 Expanded"))
    ax.add_patch(PolygonPatch(circle_poly2, fc=GRAY, ec=GRAY, alpha=0.5, zorder=2, label="Sphere 2"))

    ax.legend(loc='center left', title='Plot Info', bbox_to_anchor=(1, 0.5), ncol=1)
    plt.title("Distance Join Query using Spheres and theta: {}".format(theta))
    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
    plt.show()


def findTrajectoriesInSpaTemBox(rect1, timeFrom=None, timeTo=None, doPlot=True, allowDiskUse=False, collection=None):
    start_time = time.time()
    if collection is None :
        connection = connectMongoDB()
        # connecting or switching to the database
        db = connection.marine_trafic

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation2

    pipeline = [
        {"$match" : {
            **({'ts' : {"$gte" : timeFrom, "$lte" : timeTo}} if timeFrom is not None and timeTo is not None else {}),
            "location" : {"$geoWithin" : {"$box" : rect1}}
        }
        },
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}}},
        {"$sort" : {'total' : -1}}
    ]

    # execute query
    results = collection.aggregate(pipeline)
    dictlist = queryResultToDictList(results)
    print("--- %s seconds ---" % (time.time() - start_time))

    # check if plot needed
    if doPlot :
        ax = createAXNFigure()

        # plot poly
        rect1_x, rect1_y = zip(*rect1)
        rect1_poly = {
            "type" : "Polygon",
            "coordinates" : [
                [[min(rect1_x), min(rect1_y)], [min(rect1_x), max(rect1_y)], [max(rect1_x), max(rect1_y)],
                 [max(rect1_x), min(rect1_y)], [min(rect1_x), min(rect1_y)]]
            ]
        }

        ax.add_patch(
            PolygonPatch(rect1_poly, fc=BLUE, ec=BLUE, alpha=0.5, zorder=2, label="Trajectories Within Polygon"))

        # get n (ships) + points list len  random colors
        cmap = get_cmap(len(dictlist))

        #  plot trajectories
        for i, ship in enumerate(dictlist) :
            trajj = pointsListToMultiPoint(ship["location"])

            if 2 < len(trajj["coordinates"]) :
                plotLineString(ax, trajj, color=cmap(i), alpha=0.5, label=ship["_id"])

        ax.legend(loc='center left', title='Ship MMSI', bbox_to_anchor=(1, 0.5),
                  ncol=1 if len(dictlist) < 10 else int(len(dictlist) / 10))

        if len(dictlist) < 50 :  # show legend
            plt.title("Trajectories")
        plt.ylabel("Latitude")
        plt.xlabel("Longitude")
        plt.show()

    return dictlist


def distanceJoinUsingGrid(poly, mmsi=227430000, ts_from=None, ts_to=None, theta=12):
    """
    IMPLEMENT DISANCE JOIN USING GRID
    given a polygon and a ship find all given locations within distance theta of ship pings
    test ship 228858000 366 pings

    # step 1 find all grids intersects with polygon.
    # step 2 find target ship pings in polygon grouped by grid
    # step 3 find other ship pings in polygon grids.
    # step 4 find all points matching with in the same grid with target ship grids. and place them
             into matching list and the rest to the non_matching_list
    # step 5 expand target grids by theta and search non_matching points with spatial join in them
    """
    start_time = time.time()

    connection, db = connector.connectMongoDB()

    # step 1
    collection = db.target_map_grid

    grid_results = list(collection.find(
        {"geometry" : {"$geoIntersects": {"$geometry": poly}}}
        # ,{"_id": 1}
        ))

    # get target grid ids in list
    grid_ids = [r["_id"] for r in grid_results]
    print(grid_ids)
    print(len(grid_results))

    # step 2
    collection = db.ais_navigation_grid
    pipeline = [{
        "$match" : {
            "mmsi" : mmsi,
            "location" : {"$geoIntersects": {"$geometry": poly}}
        }},
        {"$group" : {"_id": "$mmsi", #"_id" :  "$grid_id",
                     "grid_ids": {"$push" : "$grid_id"},
                     "locations" : {"$push" : "$location.coordinates"},
                     "total" : {"$sum" : 1}
                     }
        }]

    target_ship_results = list(collection.aggregate(pipeline))

    # get target grid ids and locations grouped

    target_grid_ids = target_ship_results[0]["grid_ids"]#[i["_id"] for i in target_ship_results]


    # step 3
    collection = db.ais_navigation_grid

    pipeline = [{
        "$match" : {
            "mmsi" : {"$ne": mmsi},
            "grid_id" : {"$in": grid_ids}
        }},
        {"$group" :
            {"_id": "$grid_id",
                # {
                #     "mmsi": "$mmsi",
                #     "grid_id": "$grid_id"
                # },
             "locations" : {"$push" : "$location.coordinates"},
             "total" : {"$sum" : 1}
        }
    }]

    results = list(collection.aggregate(pipeline))
    print(len(results))

    # step 4
    # for key in results new check if key is in target mmsi polys and add its pings to matching pings
    # else add them to non matching pings
    matching_locs = []
    non_matching_locs = []
    for i in results :
        if i["_id"] in target_grid_ids :
            matching_locs.extend(i["locations"])
        else:
            non_matching_locs.extend(i["locations"])


    # step 5

    # 1) create a multypoligon from expanded grids
    # expanded_grid = []
    expanded_multi_poly = {
        "type" : "MultiPolygon",
        "coordinates" : []
    }
    for i in grid_results:
        if i["_id"] in target_grid_ids:
            expanded_multi_poly["coordinates"].append(getEnrichBoundingBox(i["geometry"]["coordinates"][0], theta)["coordinates"])
            # expanded_grid.append({
            #     "_id": i["_id"],
            #     "geometry": sg.shape(getEnrichBoundingBox(i["geometry"]["coordinates"][0], theta))
            # })

    # 2) perform geointersects on grids with expanded_multy_poly and initial poly and not in target grids
    # in order to get new target grids
    collection = db.target_map_grid

    grid_results = list(collection.find(
        {"geometry" : {"$geoIntersects" : {"$geometry" : poly}}}
        # ,{"_id": 1}
    ))

    pipeline = [{
        "$match" : {
            "_id" : {"$nin": target_grid_ids},
            "geometry" : {"$geoIntersects" : {"$geometry" : poly}}
        }},
        {"$match" : {
            "geometry" : {"$geoIntersects" : {"$geometry" : expanded_multi_poly}}
        }}
        # ,{"$project" : {"_id" : 1}}
        ]

    expanded_intersects = list(collection.aggregate(pipeline))
    # get target grid ids
    expanded_grid_ids = [i["_id"] for i in expanded_intersects if i]

    # step 6 compare distance of trajectory points with expanded_intersects points
    # for

    target_locs = []
    for i in results:
        if i["_id"] in expanded_grid_ids:
            target_locs.extend(i["locations"])

    # ps1, ps2 = comparePointSets(target_ship_results[0]["locations"], target_locs, theta)
    # print(ps1)
    # print("--- %s seconds ---" % (time.time() - start_time))
    # print(ps2)
    """
    ENDS
    """

    """
    COMMENT ATTEMPT WITH GEOPANDAS spatial join
    """
    # create geo pandas df from grids
    # expanded_grid_df = gpd.GeoDataFrame(expanded_grid)
    #
    # # create geo pandas df for non matching locs
    # non_matching_locs_shape = [sg.Point(i[0], i[1]) for i in non_matching_locs]
    # non_matching_df = gpd.GeoDataFrame(geometry=non_matching_locs_shape)
    # non_matching_df = non_matching_df.rename(columns={'location' : 'geometry'})
    #
    # # perform s join to find non_matching pings in expanded target grids
    # valid_pings = gpd.sjoin(non_matching_df, expanded_grid_df, how="inner", op="intersects")
    # print(valid_pings)
    #
    # for i in target_grid_ids:
    #     t = valid_pings.loc[valid_pings['_id'] == i]
    #     print(t["geometry"].tolist())
    #     # valid_pings["_id"].tolist()

    # group results per grid.
    # results_new = {}
    # for item in results :
    #     results_new.setdefault(item["_id"]['grid_id'], []).append({
    #         "mmsi": item["_id"]['mmsi'],
    #         "locations": item["locations"]
    #     })
    #
    # print(len(results_new))
    """
    END
    """

    print("--- %s seconds ---" % (time.time() - start_time))

    # plot results
    # Draw polygons
    ax = createAXNFigure()

    # plot poly
    ax.add_patch(PolygonPatch(poly, fc='y', ec='k', alpha=0.1, zorder=2))
    ax.add_patch(PolygonPatch(expanded_multi_poly, fc='m', ec='k', alpha=0.3, zorder=2))

    for cell in grid_results:
        ax.add_patch(
            PolygonPatch(cell["geometry"], fc=GRAY, ec=GRAY, alpha=0.5, zorder=2))

    for cell in expanded_intersects:
        ax.add_patch(
            PolygonPatch(cell["geometry"],  fc="m", ec="m", alpha=1, zorder=2))

    # # plot non matching points
    # isFirstRed = True
    # for ping in non_matching_locs :
    #     ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.3, c='r',
    #             label="non matching points" if isFirstRed else None)
    #     isFirstRed = False
    #
    # # plot matching points
    # isFirstGreen = True
    # for ping in matching_locs:
    #     ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.5, c='g',
    #             label="matching points" if isFirstGreen else None)
    #     isFirstGreen = False

    # plot target ship pings
    isFirstBlue = True
    # for grid in target_ship_results:
    for ping in target_ship_results[0]["locations"]: #grid["locations"] :
        ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=1, c='b',
                label="target points" if isFirstBlue else None)
        isFirstBlue = False

    ax.legend(loc='center left', title='Plot Info', bbox_to_anchor=(1, 0.5), ncol=1)
    plt.title("Distance Join Query using Spheres and theta: {}".format(theta))
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
            
        query2: find trajectories for all france fishing vessels and German tankers
        vres ola ta fishing vessels me galiki simea pou kinounte me 
    """
    # query 1
    # shipMMSI = getShipsByCountry(["Greece"])
    # matchAggregation = {"$match" : {'mmsi' : {'$in': shipMMSI.tolist()}}}
    # findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, logResponse=True)

    # query 2
    # shipMMSI = getShipsByCountry(["France", "German"])
    # matchAggregation = {"$match" : {'mmsi' : {'$in': shipMMSI.tolist()}}}
    # TODO THIS QUERY HAS PROBLEM WITH $group
    # Exceeded memory limit for $group, but didn't allow external sort. Pass allowDiskUse:true
    # findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, logResponse=True)

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
    poly = findPolyFromSeas(seaName="Bay of Biscay")
    shipMMSI = getShipsByCountry(["France"])
    matchAggregation = {"$match" : {'mmsi' : {'$in' : shipMMSI.tolist()},
                                    "location" : {"$geoWithin" : {"$geometry" : poly["geometry"]}}}}
    # findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, withPoly=poly["geometry"], logResponse=True)

    # query2 # ARGEI POLI
    # port_point = findPort()
    matchAggregation = {
        "$geoNear" : {"near" : {"type" : "Point", "coordinates" : [-4.475309812300752, 48.38273389573341]},
                      "distanceField" : "dist.calculated",
                      "minDistance" : nautical_mile_in_meters * 10,
                      "maxDistance" : nautical_mile_in_meters * 30,
                      "spherical" : True, "key" : "location"}}
    # den to kanei plot
    # findPointsForMatchAggr(matchAggregation, doPlot=True ,allowDiskUse=True ,queryTitle="Find all ships that moved in range from 10 to 50 sea miles from Burst port")

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
        
        query1: Find all ships that moved in range from 10 to 50 sea miles from Burst port for one day time range
        for 6 hours time interval
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
    # port_point = findPort()
    geoNearAgg = {"$geoNear" : {"near" : {"type" : "Point", "coordinates" : [-4.47530, 48.3827]},
                                "distanceField" : "dist.calculated",
                                "minDistance" : nautical_mile_in_meters * 10,
                                "maxDistance" : nautical_mile_in_meters * 30,
                                "spherical" : True, "key" : "location"}}

    matchAgg = {"$match" : {'ts' : {"$gte" : 1448988894, "$lte" : 1448988894 + (2 * one_hour_in_unix_time)}}}

    # findPointsForMatchAggr(geoNearAgg,matchAgg,doPlot=True,
    #                        queryTitle="Find all ships that moved in range from 10 to 50 sea miles from Burst port for 2 hours interval")

    # query 2
    point = {"type" : "Point", "coordinates" : [-4.1660385, 50.334972]}
    geoNearAgg = {"$geoNear" : {"near" : point,
                                "distanceField" : "dist.calculated",
                                # "minDistance" : nautical_mile_in_meters * 10,
                                "maxDistance" : nautical_mile_in_meters * 30,
                                "spherical" : True, "key" : "location"}}

    matchAgg = {"$match" : {'ts' : {"$gte" : 1448988894, "$lte" : 1449075294}}}

    # findPointsForMatchAggr(geoNearAgg, matchAgg, k_near=20, doPlot=True,
    #                        queryTitle="find k closest ship sigmas to a point (k=5) in one day interval")

    # query 3:
    poly = findPolyFromSeas()
    shipMMSI = getShipsByCountry(["France"])
    matchAggregation = {"$match" : {'mmsi' : {'$in' : shipMMSI.tolist()},
                                    "location" : {"$geoWithin" : {"$geometry" : poly["geometry"]}},
                                    'ts' : {"$gte" : 1448988894, "$lte" : 1448988894 + (6 * one_hour_in_unix_time)}}}
    # findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, withPoly=poly["geometry"], logResponse=True)

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
    # trajectory = findShipTrajectory(mmsi=227002630)
    # trajectory = findShipTrajectory()
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
        {"type" : "Point", "coordinates" : [-5.683, 48.480]}
    ]
    # findTrajectoriesFromPoints(pointsList)

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
    # poly1 = {
    #     "type" : "Polygon",
    #     "coordinates" : [
    #         [
    #             [-5.1855469, 47.5765257],
    #             [-3.6474609, 46.7097359],
    #             [-2.6586914, 45.9511497],
    #             [-3.2299805, 45.7828484],
    #             [-4.7680664, 45.6908328],
    #             [-5.625, 46.4378569],
    #             [-6.0644531, 46.8000594],
    #             [-5.6469727, 47.44295],
    #             [-5.1855469, 47.5765257]
    #         ]
    #     ]
    # }
    distanceJoinUsingGrid(poly1)
    # poly1 = findPolyFromSeas(seaName="Bay of Biscay")
    # poly2 = findPolyFromSeas(seaName="Celtic Sea")
    # grid = mongoUtils.getPolyGrid(poly, theta=10)
    # expanded_grid = mongoUtils.getPolyGrid(poly, theta=30)


    # print(poly1["geometry"]["coordinates"][0])
    # print(poly2["geometry"]["coordinates"][0])

    # ax = createAXNFigure()

    # ax.add_patch(PolygonPatch(poly1["geometry"], fc=BLUE, ec=BLUE, alpha=0.5, zorder=2, label="Trajectories Within Polygon"))
    # ax.add_patch(PolygonPatch(poly2["geometry"], fc=BLUE, ec=BLUE, alpha=0.5, zorder=2, label="Trajectories Within Polygon"))


    # gridList = []
    # for cell in grid["geometry"] :
    #     gridList.append(cell.__geo_interface__)
    #
    # import mongo.mongoConnector as connector
    #
    # connection, db = connector.connectMongoDB()
    #
    # # creating or switching to ais_navigation collection
    # collection = db.test_grid
    #
    # # insert data based on whether it is many or not
    # collection.insert(gridList)



    # for cell in grid["geometry"]:
    #
    #     eb = getEnrichBoundingBox(cell.__geo_interface__["coordinates"][0], 10)
    #     # expanded_grid2.append(getEnrichBoundingBox(cell.__geo_interface__[0]["coordinates"], 10))
    #     ax.add_patch(
    #         PolygonPatch(eb, fc='y', ec='k', alpha=0.1, zorder=2))
    #
    #     ax.add_patch(
    #         PolygonPatch(cell, fc=GRAY, ec=GRAY, alpha=0.5, zorder=2))

    # for cell in expanded_grid["geometry"]:
    #     ax.add_patch(
    #         PolygonPatch(cell, fc='r', ec='r', alpha=0.3, zorder=2))

        # matchAggregation = {"$match" : {"location" : {"$geoWithin" : {"$geometry" : cell.__geo_interface__}}}}
        # response = findPointsForMatchAggr(matchAggregation, doPlot=False)
        # responseList.extend(response)

    # print(responseList)

    # print("--- %s seconds ---" % (time.time() - start_time))
    # plt.ylabel("Latitude")
    # plt.xlabel("Longitude")
    # plt.show()