"""
    query 4: Distance join queries spatial
    given 2 A and B polygon or geo areas (circles on point or bounging box)
    find point pairs (a,b) that dictance from a to b is lees than threshold d
    time for poly: --- 4.524019002914429 seconds ---
    time for box: --- 414.99709010124207 seconds --- poli kako. --- 240.29256677627563 seconds --- na ferei ta pings apo ta poligona.


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


     query 4: Distance join queries spatio temporal
        given 2 A and B polygon or geo areas (circles on point or bounging box)
        find point pairs (a,b) that dictance from a to b is lees than threshold d
        time for poly: --- 4.524019002914429 seconds ---
        time for box: --- 4.433316946029663 seconds ---

        Stats for circles:
        # WITHOUT OVERLAPPING POLYGONES
        d(k1,k2) 46.84, r1 = 20, r2=20, theta= 30  extended_r1 60, extended_r2 50 (ola KM)
"""
# My packages
from mongo import mongoConnector as connector
from mongo import mongoUtils as utils
from mongo.query import relationalQueries
# Python libraries
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import shapely.geometry as sg
from descartes import PolygonPatch
import time


distanceJoin_main_options = ['1', '2', '0']
distanceJoin_spatial_options = ['1', '2', '3', '4', '5', '6', '0']
distanceJoin_spatiotemporal_options = ['1', '2', '3', '4', '5', '6', '0']



def distanceJoinMain_menu():
    print("\n")
    print('|---------------- Distance Join queries Menu -------------------|')
    print('|                                                               |')
    print('| 1.  Distance Join  Spatial Queries                            |')
    print('| 2.  Distance Join Spatio-temporal Queries                     |')
    print('|                                                               |')
    print('| 0.  Exit                                                      |')
    print('|---------------------------------------------------------------|')
    return input('Your choice: ')


def distanceJoinSpatial_menu():
    print("\n")
    print('|---------------- Distance Join Spatial Queries -------------------|')
    print('|                                                                  |')
    print('|     Given 2 A and B polygon or geo areas                         |')
    print('|     (circles on point or bounging box) find point pairs (a,b)    |')
    print('|     that dictance from a to b is lees than threshold d           |')
    print('|                                                                  |')
    print('| 1.  Distance Join Spatial With 2 Polygons                        |')
    print('| 2.  Distance Join Spatial With 2 Boxes                           |')
    print('| 3.  Distance Join Spatial With 2 Circles (overlapping)           |')
    print('| 4.  Distance Join Spatial With 2 Circles                         |')
    print('| 5.  Distance Join Spatial for Ship Locations within polygon      |')
    print('| 6.  Distance Join Spatial for Ship Locations within polygon      |')
    print('|     using Geopandas to post process mongo results                |')
    print('|                                                                  |')
    print('| 0.  Exit                                                         |')
    print('|------------------------------------------------------------------|')
    return input('Your choice: ')


def distanceJoinSpatioTemporal_menu():
    print("\n")
    print('|---------------- Distance Join Spatio Temporal Queries -------------------|')
    print('|                                                                          |')
    print('|     Given 2 A and B polygon or geo areas                                 |')
    print('|     (circles on point or bounging box) find point pairs (a,b)            |')
    print('|     that dictance from a to b is lees than threshold d                   |')
    print('|                                                                          |')
    print('| 1.  Distance Join Spatio-temporal With 2 Polygons                        |')
    print('|     (from time  1448988894 and for 1 month interval)                     |')
    print('| 2.  Distance Join Spatio-temporal With 2 Boxes                           |')
    print('|     (from time  1448988894 and for 72 hours interval)                    |')
    print('| 3.  Distance Join Spatio-temporal With 2 Circles (overlapping)           |')
    print('|     (from time  1448988894 and for 72 hours interval)                    |')
    print('| 4.  Distance Join Spatio-temporal With 2 Circles                         |')
    print('|     (from time  1448988894 and for 72 hours interval)                    |')
    print('| 5.  Distance Join Spatio-temporal for Ship Locations within polygon      |')
    print('|     (from time  1448988894 and for 72 hours interval)                    |')
    print('| 6.  Distance Join Spatio-temporal for Ship Locations within polygon      |')
    print('|     using Geopandas to post process mongo results                        |')
    print('|     (from time  1448988894 and for 72 hours interval)                    |')
    print('|                                                                          |')
    print('| 0.  Exit                                                                 |')
    print('|--------------------------------------------------------------------------|')
    return input('Your choice: ')


def distanceJoinPolyQuery(p1, p2, theta, timeFrom=None, timeTo=None,allowDiskUse=False, collection=None) :
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
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

    # group all the pings in a list no requirement for more information
    groupAgg = {"$group" : {"_id" : None, "location" : {"$push" : "$location.coordinates"}}}

    poly_dictList = []
    # for i, poly in enumerate(polyList):

    # crate a poly from target poly border as line
    # sos: adding 0.1 distance pou poly to reduce failure probability
    # poly1_expanded, shpOuterPoly, shpInnerPoly = convertLineStringToPolygon(poly1["coordinates"][0], d=theta + 0.1)
    # poly2_expanded, shpOuterPoly, shpInnerPoly = convertLineStringToPolygon(poly2["coordinates"][0], d=theta + 0.1)
    poly1_expanded = utils.getEnrichBoundingBox(p1["coordinates"][0], theta)
    poly2_expanded = utils.getEnrichBoundingBox(p2["coordinates"][0], theta)

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
    poly_dictList = utils.queryResultToDictList(results, dictlist=poly_dictList)

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
    poly_dictList = utils.queryResultToDictList(results, dictlist=poly_dictList)

    try:
        ps1, ps2 = utils.comparePointSets(poly_dictList[0]["location"], poly_dictList[1]["location"], theta)
        print("--- %s seconds ---" % (time.time() - start_time))
    except:
        ps1 = ps2 = None
        print("no points found")

    # Draw polygons
    ax = utils.createAXNFigure()

    # cmap = get_cmap(len(poly_dictList["location"]))

    # plot pings
    if ps1 is not None and ps2 is not None:
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


    ax.add_patch(PolygonPatch(poly1_expanded, fc="k", ec="k", alpha=0.2, zorder=2, label="Polygon 1 Expanded"))
    ax.add_patch(PolygonPatch(p1, fc=utils.BLUE, ec=utils.BLUE, alpha=0.5, zorder=2, label="Polygon 1"))
    ax.add_patch(PolygonPatch(poly2_expanded, fc="r", ec="r", alpha=0.2, zorder=2, label="Polygon 2 Expanded"))
    ax.add_patch(PolygonPatch(p2, fc=utils.GRAY, ec=utils.GRAY, alpha=0.5, zorder=2, label="Polygon 2"))

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
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

    # group all the pings in a list no requirement for more information
    groupAgg = {"$group" : {"_id" : None, "location" : {"$push" : "$location.coordinates"}}}

    poly_dictList = []
    # for i, poly in enumerate(polyList):

    # crate a poly from target poly border as line
    # sos: adding 0.1 distance pou poly to reduce failure probability
    # poly1_expanded, shpOuterPoly, shpInnerPoly = convertLineStringToPolygon(poly1["coordinates"][0], d=theta + 0.1)
    # poly2_expanded, shpOuterPoly, shpInnerPoly = convertLineStringToPolygon(poly2["coordinates"][0], d=theta + 0.1)
    rect1_expanded = utils.getEnrichBoundingBox(rect1, theta)
    rect2_expanded = utils.getEnrichBoundingBox(rect2, theta)

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
    poly_dictList = utils.queryResultToDictList(results, dictlist=poly_dictList)

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
    poly_dictList = utils.queryResultToDictList(results, dictlist=poly_dictList)

    # check if list is not empty

    if len(poly_dictList) == 2 :
        ps1, ps2 = utils.comparePointSets(poly_dictList[0]["location"], poly_dictList[1]["location"], theta)
    else:
        print("list is empty")
    print("--- %s seconds ---" % (time.time() - start_time))

    # Draw polygons
    ax = utils.createAXNFigure()

    cmap = utils.get_cmap(len(poly_dictList))

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
    ax.add_patch(PolygonPatch(rect1_poly, fc=utils.BLUE, ec=utils.BLUE, alpha=0.5, zorder=2, label="Box 1"))
    ax.add_patch(PolygonPatch(rect2_expanded, fc="r", ec="r", alpha=0.2, zorder=2, label="Box 2 Expanded"))
    ax.add_patch(PolygonPatch(rect2_poly, fc=utils.GRAY, ec=utils.GRAY, alpha=0.5, zorder=2, label="Box 2"))

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
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

    k_dist = utils.calculatePointsDistance(k1['coordinates'], k2['coordinates'])
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
    poly_dictList = utils.queryResultToDictList(results, dictlist=poly_dictList)
    print("--- %s seconds ---" % (time.time() - start_time))


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
    print("--- %s seconds ---" % (time.time() - start_time))

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
    poly_dictList = utils.queryResultToDictList(results, dictlist=poly_dictList)

    ps1, ps2 = utils.comparePointSets(poly_dictList[0]["location"], poly_dictList[1]["location"], theta)
    print("--- %s seconds ---" % (time.time() - start_time))

    # Draw polygons
    ax = utils.createAXNFigure()

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
    circle_poly1 = utils.generatePolyFromPoint(k1["coordinates"], r1*1000, 100)
    circle_poly1_extended = utils.generatePolyFromPoint(k1["coordinates"], r1_extended*1000, 100)
    circle_poly2 = utils.generatePolyFromPoint(k2["coordinates"], r2*1000, 100)
    circle_poly2_extended = utils.generatePolyFromPoint(k2["coordinates"], r2_extended*1000, 100)

    ax.add_patch(PolygonPatch(circle_poly1_extended, fc="k", ec="k", alpha=0.2, zorder=2, label="Sphere 1 Expanded"))
    ax.add_patch(PolygonPatch(circle_poly1, fc=utils.BLUE, ec=utils.BLUE, alpha=0.5, zorder=2, label="Sphere 1"))
    ax.add_patch(PolygonPatch(circle_poly2_extended, fc="r", ec="r", alpha=0.2, zorder=2, label="Sphere 2 Expanded"))
    ax.add_patch(PolygonPatch(circle_poly2, fc=utils.GRAY, ec=utils.GRAY, alpha=0.5, zorder=2, label="Sphere 2"))

    ax.legend(loc='center left', title='Plot Info', bbox_to_anchor=(1, 0.5), ncol=1)
    plt.title("Distance Join Query using Spheres and theta: {}".format(theta))
    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
    plt.show()


def distanceJoinUsingGrid(poly, mmsi, ts_from=None, ts_to=None, theta=12):
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

    run 1 --- 616.8660519123077 seconds ---
    run 2 --- 585.5844461917877 seconds ---


    """
    start_time = time.time()

    connection, db = connector.connectMongoDB()

    # step 1
    collection = db.target_map_grid

    pipeline = [
        {
            "$match" : {"geometry" : {"$geoIntersects" : {"$geometry" : poly}}}
        }
    ]

    grid_results = list(collection.aggregate(pipeline))

    # perform explain if needed
    utils.queryExplain("target_map_grid", pipeline)

    # get target grid ids in list
    grid_ids = [r["_id"] for r in grid_results]
    print(len(grid_results))
    print("Step 1")
    print("--- %s seconds ---" % (time.time() - start_time))

    # step 2
    collection = db.ais_navigation
    pipeline = [{
        "$match" : {
            "mmsi" : mmsi,
            "location" : {"$geoWithin": {"$geometry": poly}},
            **({'ts' : {"$gte" : ts_from, "$lte" : ts_to}} if ts_from is not None and ts_to is not None else {}),
        }},
        {"$group" : {"_id" :  "$grid_id",
                     "locations" : {"$push" : "$location.coordinates"},
                     "total" : {"$sum" : 1}
                     }
        }]

    # perform explain if needed
    utils.queryExplain("ais_navigation", pipeline)

    target_ship_results = list(collection.aggregate(pipeline))
    # get target grid ids and locations grouped
    print("Step 2")
    print("--- %s seconds ---" % (time.time() - start_time))

    target_grid_ids = [i["_id"] for i in target_ship_results]

    # 1) create a multypoligon from expanded grids
    # expanded_grid = []
    expanded_multi_poly = {
        "type" : "MultiPolygon",
        "coordinates" : []
    }
    for i in grid_results :
        if i["_id"] in target_grid_ids :
            expanded_multi_poly["coordinates"].append(
                utils.getEnrichBoundingBox(i["geometry"]["coordinates"][0], theta)["coordinates"])
            # expanded_grid.append({
            #     "_id": i["_id"],
            #     "geometry": sg.shape(getEnrichBoundingBox(i["geometry"]["coordinates"][0], theta))
            # })

    print("Step 3")
    print("--- %s seconds ---" % (time.time() - start_time))


    # step 3
    collection = db.ais_navigation

    pipeline = [{
        "$match" : {
            "mmsi" : {"$ne": mmsi},
            "location" : {"$geoWithin" : {"$geometry" : expanded_multi_poly}},
            **({'ts' : {"$gte" : ts_from, "$lte" : ts_to}} if ts_from is not None and ts_to is not None else {}),
            # "grid_id" : {"$in": grid_ids}
        }},{"$match" : {
            "location" : {"$geoWithin" : {"$geometry" : poly}}
            # "grid_id" : {"$in": grid_ids}
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

    utils.queryExplain("ais_navigation", pipeline)
    results = list(collection.aggregate(pipeline))
    print(len(results))
    print("--- %s seconds ---" % (time.time() - start_time))


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
    print("--- %s seconds ---" % (time.time() - start_time))


    # # step 5
    # # 2) perform geointersects on grids with expanded_multy_poly and initial poly and not in target grids
    # # in order to get new target grids
    # collection = db.target_map_grid
    #
    # grid_results = list(collection.find(
    #     {"geometry" : {"$geoIntersects" : {"$geometry" : poly}}}
    #     # ,{"_id": 1}
    # ))
    #
    # pipeline = [{
    #     "$match" : {
    #         "_id" : {"$nin": target_grid_ids},
    #         "geometry" : {"$geoIntersects" : {"$geometry" : poly}},
    #         **({'ts' : {"$gte" : ts_from, "$lte" : ts_to}} if ts_from is not None and ts_to is not None else {})
    #     }},
    #     {"$match" : {
    #         "geometry" : {"$geoIntersects" : {"$geometry" : expanded_multi_poly}}
    #     }}
    #     # ,{"$project" : {"_id" : 1}}
    #     ]
    #
    # expanded_intersects = list(collection.aggregate(pipeline))
    # # get target grid ids
    # expanded_grid_ids = [i["_id"] for i in expanded_intersects if i]

    # step 6 compare distance of trajectory points with expanded_intersects points
    # for

    # target_locs = []
    # for i in results:
    #     if i["_id"] in expanded_grid_ids:
    #         target_locs.extend(i["locations"])

    # gets ps1 is the indexes of points of our target rajectory, ps2 are the indexes of target locs

    total_target_ship_results = []
    for i in target_ship_results:
        total_target_ship_results.extend(i["locations"])

    ps1, ps2 = utils.comparePointSets(total_target_ship_results, non_matching_locs, theta)
    np_locs = np.array(non_matching_locs)
    matching_locs.extend(list(np_locs[list(ps2)]))
    mask = np.ones(np_locs.shape[0], dtype=bool)
    mask[list(ps2)] = False
    non_matching_locs = np_locs[mask,:]

    print("--- %s seconds ---" % (time.time() - start_time))

    # plot results
    # Draw polygons
    ax = utils.createAXNFigure()

    # plot poly
    ax.add_patch(PolygonPatch(poly, fc='y', ec='k', alpha=0.1, zorder=2))
    ax.add_patch(PolygonPatch(expanded_multi_poly, fc='m', ec='k', alpha=0.3, zorder=2, label="Expanded Poly"))

    for cell in grid_results:
        ax.add_patch(
            PolygonPatch(cell["geometry"], fc=utils.GRAY, ec=utils.GRAY, alpha=0.5, zorder=2))

    # for cell in expanded_intersects:
    #     ax.add_patch(
    #         PolygonPatch(cell["geometry"],  fc="m", ec="m", alpha=1, zorder=2))

    # plot non matching points
    isFirstRed = True
    for ping in non_matching_locs :
        ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.3, c='r',
                label="non matching points" if isFirstRed else None)
        isFirstRed = False

    # plot matching points
    isFirstGreen = True
    for ping in matching_locs:
        ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.2, c='g',
                label="matching points" if isFirstGreen else None)
        isFirstGreen = False

    # plot target ship pings
    isFirstBlue = True
    # for grid in target_ship_results:
    for ping in total_target_ship_results: #grid["locations"] :
        ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=1, c='b',
                label="target points" if isFirstBlue else None)
        isFirstBlue = False

    ax.legend(loc='center left', title='Plot Info', bbox_to_anchor=(1, 0.5), ncol=1)
    plt.title("Distance Join Query using Grids and theta: {}".format(theta))
    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
    plt.show()


def distanceJoinUsingGPDGrid(poly, mmsi, ts_from=None, ts_to=None, theta=12):
    start_time = time.time()

    connection, db = connector.connectMongoDB()

    # step 1
    collection = db.target_map_grid

    pipeline = [
        {
            "$match" :  {"geometry" : {"$geoIntersects": {"$geometry": poly}}}
        }
    ]

    grid_results = list(collection.aggregate(pipeline))

    # perform explain if needed
    utils.queryExplain("target_map_grid", pipeline)
    print("step 1")
    print("--- %s seconds ---" % (time.time() - start_time))

    # get target grid ids in list
    grid_ids = [r["_id"] for r in grid_results]
    print(len(grid_results))

    # step 2
    collection = db.ais_navigation
    pipeline = [{
        "$match" : {
            "mmsi" : mmsi,
            "location" : {"$geoWithin": {"$geometry": poly}},
            **({'ts' : {"$gte" : ts_from, "$lte" : ts_to}} if ts_from is not None and ts_to is not None else {}),
        }},
        {"$group" : {"_id": "$grid_id", #"_id" :  "$grid_id",
                     "locations" : {"$push" : "$location.coordinates"},
                     "total" : {"$sum" : 1}
                     }
        }]

    utils.queryExplain("ais_navigation", pipeline)
    print("step 2")
    print("--- %s seconds ---" % (time.time() - start_time))

    target_ship_results = list(collection.aggregate(pipeline))

    # get target grid ids and locations grouped

    target_grid_ids = [i["_id"] for i in target_ship_results]

    # 1) create a multypoligon from expanded grids
    expanded_grid = []
    expanded_multi_poly = {
        "type" : "MultiPolygon",
        "coordinates" : []
    }
    for i in grid_results :
        if i["_id"] in target_grid_ids :
            expanded_multi_poly["coordinates"].append(
                utils.getEnrichBoundingBox(i["geometry"]["coordinates"][0], theta)["coordinates"])
            expanded_grid.append({
                "_id" : i["_id"],
                "geometry" : sg.shape(utils.getEnrichBoundingBox(i["geometry"]["coordinates"][0], theta))
            })

    print("step 3")
    print("--- %s seconds ---" % (time.time() - start_time))


    # step 4
    collection = db.ais_navigation

    pipeline = [{
        "$match" : {
            "mmsi" : {"$ne": mmsi},
            "location" : {"$geoWithin" : {"$geometry" : expanded_multi_poly}},
            **({'ts' : {"$gte" : ts_from, "$lte" : ts_to}} if ts_from is not None and ts_to is not None else {}),
            # "grid_id" : {"$in": grid_ids}
        }},{
        "$match" : {
        "location" : {"$geoWithin" : {"$geometry" : poly}},
        # "grid_id" : {"$in": grid_ids}
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

    utils.queryExplain("ais_navigation", pipeline)
    print("step 4")
    print("--- %s seconds ---" % (time.time() - start_time))

    # step 5
    # for key in results new check if key is in target mmsi polys and add its pings to matching pings
    # else add them to non matching pings
    matching_locs = []
    non_matching_locs = []
    for i in results :
        if i["_id"] in target_grid_ids :
            matching_locs.extend(i["locations"])
        else:
            non_matching_locs.extend(i["locations"])

    print("step 5")
    print("--- %s seconds ---" % (time.time() - start_time))

    # step 6
    # create geo pandas df from grids
    expanded_grid_df = gpd.GeoDataFrame(expanded_grid)

    # create geo pandas df for non matching locs
    non_matching_locs_shape = [sg.Point(i[0], i[1]) for i in non_matching_locs]
    non_matching_df = gpd.GeoDataFrame(geometry=non_matching_locs_shape)
    non_matching_df = non_matching_df.rename(columns={'location' : 'geometry'})

    # perform s join to find non_matching pings in expanded target grids
    valid_pings = gpd.sjoin(non_matching_df, expanded_grid_df, how="inner", op="intersects")
    print("step 6")
    print("--- %s seconds ---" % (time.time() - start_time))

    non_matching_locs = []
    for i, target_id in enumerate(target_grid_ids):
        t = valid_pings.loc[valid_pings['_id'] == target_id]
        ps = [j.__geo_interface__['coordinates'] for j in t["geometry"].tolist()]  # TODO MAKE THIS QUICKER
        print(target_id)
        if len(ps) == 0 :
            continue
        print("target ship pings in grid: {}, and non_matching_loc in expanded grid: {}".format(len(target_ship_results[i]["locations"]), len(ps)))
        ps1, ps2 = utils.comparePointSets(target_ship_results[i]["locations"], ps, theta)
        # print(ps1)
        # print("--- %s seconds ---" % (time.time() - start_time))
        # print(ps2)
        # valid_ping_indexes.extend(ps2)
        print(len(ps))
        valid_pings["_id"].tolist()
        np_locs = np.array(ps)
        matching_locs.extend(list(np_locs[list(ps2)]))
        mask = np.ones(np_locs.shape[0], dtype=bool)
        mask[list(ps2)] = False
        non_matching_locs.extend(list(np_locs[mask, :]))
    print("step 7 above")

    # # group results per grid.
    # results_new = {}
    # for item in results :
    #     results_new.setdefault(item["_id"]['grid_id'], []).append({
    #         "mmsi": item["_id"]['mmsi'],
    #         "locations": item["locations"]
    #     })

    # print(len(results_new))

    print("--- %s seconds ---" % (time.time() - start_time))

    print(len(non_matching_locs))

    # plot results
    # Draw polygons
    ax = utils.createAXNFigure()

    # plot poly
    ax.add_patch(PolygonPatch(poly, fc='y', ec='k', alpha=0.1, zorder=2))
    ax.add_patch(PolygonPatch(expanded_multi_poly, fc='m', ec='k', alpha=0.3, zorder=2))

    for cell in grid_results :
        ax.add_patch(
            PolygonPatch(cell["geometry"], fc=utils.GRAY, ec=utils.GRAY, alpha=0.5, zorder=2))

    # plot non matching points
    isFirstRed = True
    for ping in non_matching_locs :
        ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.3, c='r',
                label="non matching points" if isFirstRed else None)
        isFirstRed = False

    # plot matching points
    isFirstGreen = True
    for ping in matching_locs :
        ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=0.2, c='g',
                label="matching points" if isFirstGreen else None)
        isFirstGreen = False

    # plot target ship pings
    isFirstBlue = True
    for grid in target_ship_results:
        for ping in grid["locations"] :  # grid["locations"] :
            ax.plot(ping[0], ping[1], marker='o', markersize=2, alpha=1, c='b',
                    label="target points" if isFirstBlue else None)
            isFirstBlue = False

    ax.legend(loc='center left', title='Plot Info', bbox_to_anchor=(1, 0.5), ncol=1)
    plt.title("Distance Join Query using grid and theta: {}".format(theta))
    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
    plt.show()


def executeDistanceJoinQuery():
    choice = -1
    while choice not in distanceJoin_main_options :
        choice = distanceJoinMain_menu()

        if choice == '1' :
            # spatial distance join
            print("--------------You choose Spatial Join Distance Queries--------------")
            spatial_choice = -1
            while spatial_choice not in distanceJoin_main_options :
                spatial_choice = distanceJoinSpatial_menu()

                if spatial_choice == '1' :
                    # spatial distance join poly
                    print("--------------You choose 1--------------")
                    # get polygons
                    poly1 = relationalQueries.findTestPoly("test_poly_5")
                    poly2 = relationalQueries.findTestPoly("test_poly_6")

                    distanceJoinPolyQuery(poly1, poly2, 50)

                elif spatial_choice == '2' :
                    # spatial distance join rect query
                    print("--------------You choose 2--------------")
                    box1 = [
                        [-4.118, 49.451],
                        [-5.401, 48.839]
                    ]
                    box2 = [
                        [-4.861, 49.458],
                        [-3.258, 50.164]
                    ]
                    distanceJoinRectQuery(box1, box2, 50)

                elif spatial_choice == '3' :
                    # spatial distance join circles
                    print("--------------You choose 3--------------")
                    # query 4.3 distance join given point and R d(k1,k2) = 167.84 , r1 = 30 and r2 = 20

                    k1 = {"type" : "Point", "coordinates" : [-4.219, 49.098]}
                    k2 = {"type" : "Point", "coordinates" : [-4.567, 49.336]}

                    # overlaping circles
                    distanceJoinSphereQuery(k1, 30, k2, 20, 30)

                elif spatial_choice == '4' :
                    # spatio temporal distance join
                    print("--------------You choose 4--------------")

                    k1 = {"type" : "Point", "coordinates" : [-4.219, 49.098]}
                    k2 = {"type" : "Point", "coordinates" : [-4.567, 49.336]}

                    # NON OVERLAPPING CIRCLES
                    distanceJoinSphereQuery(k1, 20, k2, 20, 30)
                elif spatial_choice == '5' :
                    # spatio temporal distance join
                    print("--------------You choose 5--------------")
                    bay_of_biscay_sea = relationalQueries.findPolyFromSeas(seaName="Bay of Biscay")
                    bay_of_biscay_poly = {
                        "type" : "Polygon",
                        "coordinates" : [bay_of_biscay_sea['geometry']['coordinates'][0]]
                    }
                    distanceJoinUsingGrid(bay_of_biscay_poly, mmsi=228762000) #538003876

                elif spatial_choice == '6' :
                    # spatio temporal distance join
                    print("--------------You choose 6--------------")
                    # poly1 = relationalQueries.findTestPoly("test_poly_5")
                    bay_of_biscay_sea = relationalQueries.findPolyFromSeas(seaName="Bay of Biscay")
                    bay_of_biscay_poly = {
                        "type" : "Polygon",
                        "coordinates" : [bay_of_biscay_sea['geometry']['coordinates'][0]]
                    }
                    distanceJoinUsingGPDGrid(bay_of_biscay_poly, mmsi=228762000)  # 530+ pings ship gia bay of biscay
                    # distanceJoinUsingGPDGrid(poly1, mmsi=538003876) # gia test poly 5


        elif choice == '2' :
            # spatio temporal distance join
            print("--------------You choose 2--------------")
            # 228336000 229 pings near breast from 1448988894 and 2 hours interval.

            spatio_temporal_choice = -1
            while spatio_temporal_choice not in distanceJoin_spatiotemporal_options :
                spatio_temporal_choice = distanceJoinSpatial_menu()

            if spatio_temporal_choice == '1' :
                # spatial distance join poly
                print("--------------You choose 1--------------")
                # get polygons
                poly1 = relationalQueries.findTestPoly("test_poly_3")
                poly2 = relationalQueries.findTestPoly("test_poly_4")

                distanceJoinPolyQuery(poly1, poly2, 150, timeFrom=1448988894, timeTo=1448988894 + (720 * utils.one_hour_in_unix_time))

            elif spatio_temporal_choice == '2' :
                # spatial distance join rect query
                print("--------------You choose 2--------------")
                box1 = [
                    [-5.084, 48.161],
                    [-4.939, 48.377]
                ]
                box2 = [
                    [-5.038, 48.024],
                    [-4.723, 48.211]
                ]
                distanceJoinRectQuery(box1, box2, 10, timeFrom=1448988894, timeTo=1448988894 + (10 * utils.one_hour_in_unix_time))

            elif spatio_temporal_choice == '3' :
                # spatial distance join circles
                print("--------------You choose 3--------------")
                # query 4.3 distance join given point and R d(k1,k2) = 167.84 , r1 = 30 and r2 = 20

                k1 = {"type" : "Point", "coordinates" : [-4.834, 47.987]}
                k2 = {"type" : "Point", "coordinates" : [-4.880, 48.223]}

                # overlaping circles
                distanceJoinSphereQuery(k1, 10, k2, 10, 20, timeFrom=1448988894, timeTo=1448988894 + (2 * utils.one_hour_in_unix_time))

            elif spatio_temporal_choice == '4' :
                # spatio temporal distance join
                print("--------------You choose 4--------------")

                k1 = {"type" : "Point", "coordinates" : [-4.834, 47.987]}
                k2 = {"type" : "Point", "coordinates" : [-4.880, 48.223]}

                # NON OVERLAPPING CIRCLES
                distanceJoinSphereQuery(k1, 10, k2, 10, 20, timeFrom=1448988894, timeTo=1448988894 + (72 * utils.one_hour_in_unix_time))
            elif spatio_temporal_choice == '5' :
                # spatio temporal distance join
                print("--------------You choose 5--------------")
                bay_of_biscay_sea = relationalQueries.findPolyFromSeas(seaName="Bay of Biscay")
                bay_of_biscay_poly = {
                    "type" : "Polygon",
                    "coordinates" : [bay_of_biscay_sea['geometry']['coordinates'][0]]
                }
                distanceJoinUsingGrid(bay_of_biscay_poly, mmsi=227300000, ts_from=1448988894, ts_to=1449075294)  # 538003876

            elif spatio_temporal_choice == '6' :
                # spatio temporal distance join
                print("--------------You choose 6--------------")
                # poly1 = relationalQueries.findTestPoly("test_poly_5")
                bay_of_biscay_sea = relationalQueries.findPolyFromSeas(seaName="Bay of Biscay")
                bay_of_biscay_poly = {
                    "type" : "Polygon",
                    "coordinates" : [bay_of_biscay_sea['geometry']['coordinates'][0]]
                }

                """ 
                    before performance update:
                    test mmsi: 227300000 51 ping, --- 75.34329080581665 seconds ---
                    mmsi: 228208800 21 pings --- 26.400819778442383 seconds ---
                    after performance update:
                    mmsi: 227300000 51 ping, --- 12.190508127212524 seconds ---
                    mmsi: 228208800 21 pings: --- 7.512688159942627 seconds ---
                """

                distanceJoinUsingGPDGrid(bay_of_biscay_poly, mmsi=227300000, ts_from=1448988894, ts_to=1449075294)  # 530+ pings ship gia bay of biscay
                # distanceJoinUsingGPDGrid(poly1, mmsi=538003876) # gia test poly 5


