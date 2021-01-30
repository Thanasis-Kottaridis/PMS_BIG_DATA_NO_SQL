"""
     bullet 4.1:
     find trajectories in spatio temporal box
     # because $doesn't use 2d sphere index we made 2 implamatations
     1) using mongo $box aggregation time: --- 134.49762606620789 seconds ---
     2) converting box to polygon and using geoWithin geometry  --- 1.2093191146850586 seconds ---


     Bullet 4.2:
        Given a trajectory, find similar trajectories (threshold-based, k-most similar)
        1) using euclidian distance
        2) using grid

    Bullet 4.3:
        Complex queries: find trajectories that passed through A,
        then through B (up to X hours later), then through C (up to 6 hours later)
        SOS:- for simplicity we assume that time from one point to another is 6 hours
        but we have extended the number of points from 3 to N
 """
import math

from mongo import mongoConnector as connector
from mongo import mongoUtils as utils
from mongo.query import relationalQueries

import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from descartes import PolygonPatch
from shapely.geometry import LineString
import shapely.geometry as sg
import json
import time

main_options = ['1', '2', '3', '4', '0']


def trajectoryQueriesMain_menu() :
    print("\n")
    print('|--------------- Trajectory queries Menu ------------------|')
    print('|                                                          |')
    print('| 1.  find trajectories in spatio temporal box using $box  |')
    print('| 2.  find trajectories in spatio temporal box             |')
    print('|     to polygon and use geoWithin $geometry aggrigation   |')
    print('| 3.  Given a trajectory, find similar trajectories        |')
    print('|     (threshold-based, k-most similar)                    |')
    print('| 4.  Complex queries: find trajectories that passed       |')
    print('|     through A, then through B (up to X hours later),     |')
    print('|     then through C (up to 6 hours later)                 |')
    print('|                                                          |')
    print('| 0.  Exit                                                 |')
    print('|----------------------------------------------------------|')
    return input('Your choice: ')


def findTrajectoriesInSpaTemBox(rect1, timeFrom=None, timeTo=None, doPlot=True, allowDiskUse=False, collection=None) :
    start_time = time.time()
    if collection is None :
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

    pipeline = [
        {"$match" : {
            **({'ts' : {"$gte" : timeFrom, "$lte" : timeTo}} if timeFrom is not None and timeTo is not None else {}),
            "location" : {"$geoWithin" : {"$box" : rect1}}
        }
        },
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}}},
        # {"$sort" : {'total' : -1}}
    ]

    # execute query
    results = collection.aggregate(pipeline)
    dictlist = utils.queryResultToDictList(results)
    utils.queryExplain("ais_navigation", pipeline)
    print("--- %s seconds ---" % (time.time() - start_time))

    # check if plot needed
    if doPlot :
        ax = utils.createAXNFigure()

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
            PolygonPatch(rect1_poly, fc=utils.BLUE, ec=utils.BLUE, alpha=0.5, zorder=2,
                         label="Trajectories Within Polygon"))

        # get n (ships) + points list len  random colors
        cmap = utils.get_cmap(len(dictlist))

        #  plot trajectories
        for i, ship in enumerate(dictlist) :
            trajj = utils.pointsListToLineString(ship["location"])

            if 2 < len(trajj["coordinates"]) :
                utils.plotLineString(ax, trajj, color=cmap(i), alpha=0.5, label=ship["_id"])

        ax.legend(loc='center left', title='Ship MMSI', bbox_to_anchor=(1, 0.5),
                  ncol=1 if len(dictlist) < 10 else int(len(dictlist) / 10))

        if len(dictlist) < 50 :  # show legend
            plt.title("Trajectories")
        plt.ylabel("Latitude")
        plt.xlabel("Longitude")
        plt.show()

    return dictlist


def findThresholdBasedSimilarTrajectories(mmsi, tsFrom=None, tsTo=None, d=12, k=None, similarity_threshold=0.5) :
    """
    1) Given an mmsi find its trajectory.
    2) find the grids of each trajectory and expand the by d
    3) create a multy polygon and find all the grids that intersects with them
     in order to find threshold based trajectories
    :param similarity_threshold:
    :param k:
    :param mmsi:
    :param tsFrom:
    :param tsTo:
    :param d:
    :return:
    """
    start_time = time.time()

    connection, db = connector.connectMongoDB()

    # step 1
    collection = db.ais_navigation
    pipeline = [{
        "$match" : {
            "mmsi" : mmsi,
            **({'ts' : {"$gte" : tsFrom, "$lte" : tsTo}} if tsFrom is not None and tsTo is not None else {}),
        }},
        {
            "$sort" : {"ts" : 1}
        },
        {
            "$group" : {
                "_id" : "$mmsi",
                "grid_ids" : {"$push" : "$grid_id"},
                "locations" : {"$push" : "$location.coordinates"}
            }
        }]

    target_ship_results = list(collection.aggregate(pipeline))
    # get trajectory grid ids
    target_grid_ids = target_ship_results[0]["grid_ids"]
    target_grid_ids = list(dict.fromkeys(target_grid_ids))
    target_trajectory = utils.pointsListToLineString(target_ship_results[0]["locations"])

    utils.queryExplain("ais_navigation", pipeline)
    print("Step 1")
    print("--- %s seconds ---" % (time.time() - start_time))

    # step 2 get target grids
    collection = db.target_map_grid

    pipeline = [
        {
            "$match" : {"geometry" : {"$geoIntersects" : {"$geometry" : target_trajectory}}}
        }
    ]

    grid_results = list(collection.aggregate(pipeline))

    utils.queryExplain("target_map_grid", pipeline)
    print("Step 2")
    print("--- %s seconds ---" % (time.time() - start_time))

    if d > 10 :
        # step 3 expand them and convert them into a multi polygon
        expanded_multi_poly = {
            "type" : "MultiPolygon",
            "coordinates" : []
        }
        for i in grid_results :
            expanded_multi_poly["coordinates"].append(
                utils.getEnrichBoundingBox(i["geometry"]["coordinates"][0], (d - 10) / 2)["coordinates"])

        # step4 find grids intersecting with this multy poly
        collection = db.target_map_grid

        # create mongo aggregation pipeline
        pipeline = [
            {"$match" : {
                "_id" : {"$nin" : target_grid_ids},
                "geometry" : {"$geoIntersects" : {"$geometry" : expanded_multi_poly}}
            }},
        ]

        # execute query
        results = list(collection.aggregate(pipeline))
        # add results to grid list
        grids_new = []
        # for grid in grid_results:
        #     if grid["_id"] not in
        # [dict(t) for t in {tuple(d.items()) for d in grid_results}]
        grid_results.extend(results)
        # grids_new = grids_new.extend(results)
        # find gid ids
        # get results grid id
        target_grid_ids.extend([d['_id'] for d in results])

        utils.queryExplain("target_map_grid", pipeline)
        print("Step 3 optional")
        print("--- %s seconds ---" % (time.time() - start_time))

    collection = db.ais_navigation

    # create mongo aggregation pipeline
    pipeline = [
        {"$match" : {
            "mmsi" : {"$ne" : mmsi},
            **({'ts' : {"$gte" : tsFrom, "$lte" : tsTo}} if tsFrom is not None and tsTo is not None else {}),
            "grid_id" : {"$in" : target_grid_ids}
        }},
        {
            "$sort" : {"ts" : 1}
        },
        {
            "$group" : {
                "_id" : "$mmsi",
                "total" : {"$sum" : 1},
                "geometry" : {"$push" : "$location.coordinates"}}
        }
    ]

    results = collection.aggregate(pipeline)
    dictlist = utils.queryResultToDictList(results)

    utils.queryExplain("ais_navigation", pipeline)
    print("Step 4")
    print("--- %s seconds ---" % (time.time() - start_time))

    # filter trajectories
    # step 0 append target trajectory to dictList
    dictlist.append(
        {
            "_id" : mmsi,
            "total" : len(target_ship_results[0]["locations"]),
            "geometry" : target_ship_results[0]["locations"]
        }
    )

    # step 1 convert all trajectories to in to a geo dataframe of lineStrings
    trajectory_df = pd.DataFrame(dictlist)
    trajectory_df['geometry'] = trajectory_df['geometry'].apply(
        lambda point_list : LineString(point_list) if len(point_list) >= 5 else None)
    trajectory_df = gpd.GeoDataFrame(trajectory_df.dropna(), geometry=trajectory_df["geometry"])

    # create grid geo dataframe.
    grid_df = pd.DataFrame(grid_results)
    grid_df['geometry'] = grid_df['geometry'].apply(lambda grid : sg.shape(grid))
    grid_df = gpd.GeoDataFrame(grid_df.dropna(), geometry=grid_df["geometry"])

    # perform spatial join
    sjoin_df = gpd.sjoin(trajectory_df, grid_df, how="inner", op="intersects")

    # get grids per ship Series
    grids_per_ship = sjoin_df["_id_left"].value_counts()
    # find the grid count of target trajectory
    target_traj_grid_intersect = grids_per_ship.loc[mmsi]
    grids_per_ship = grids_per_ship.drop(mmsi)
    grids_per_ship = grids_per_ship.apply(
        lambda count : count if int(target_traj_grid_intersect * similarity_threshold) < count < int(
            target_traj_grid_intersect * 1.5) else None)
    filtered_similar_trajectories = grids_per_ship.dropna()

    # check if k-most similar needed
    if k is not None and k > 0 :
        filtered_similar_trajectories = filtered_similar_trajectories.head(k)

    print("Step 5-total time")
    print("--- %s seconds ---" % (time.time() - start_time))

    ax = utils.createAXNFigure()
    # get n (ships) + points list len  random colors
    cmap = utils.get_cmap(len(dictlist) + 1)

    # plot poly
    # ax.add_patch(PolygonPatch(expanded_multi_poly, fc='m', ec='k', alpha=0.3, zorder=2))

    # plot grid
    for cell in grid_results :
        ax.add_patch(
            PolygonPatch(cell["geometry"], fc=utils.GRAY, ec=utils.GRAY, alpha=0.3, zorder=2))

    #  plot trajectories
    trajj = utils.pointsListToLineString(target_ship_results[0]["locations"])

    if 2 < len(trajj["coordinates"]) :
        utils.plotLineString(ax, trajj, color=cmap(0), alpha=1, label="target_trajectory")

    for i, trajectory in enumerate(dictlist) :
        if trajectory["_id"] in filtered_similar_trajectories.index :
            trajj = utils.pointsListToLineString(trajectory["geometry"])

            if 2 < len(trajj["coordinates"]) :
                utils.plotLineString(ax, trajj, color=cmap(i), alpha=1, label=trajectory["_id"])

    ax.legend(loc='center left', title='Ship MMSI', bbox_to_anchor=(1, 0.5),
              ncol=1 if len(filtered_similar_trajectories) < 10 else int(len(filtered_similar_trajectories) / 10))
    plt.title("Find threshold based similar trajectories: d = {}".format(d))
    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
    plt.show()


def findPingsPerPoint(point, collection=None) :
    start_time = time.time()
    if collection is None :
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

    # create mongo aggregation pipeline
    pipeline = [
        {'$geoNear' : {"near" : point,
                       "distanceField" : "dist.calculated",
                       "maxDistance" : utils.nautical_mile_in_meters * 10,
                       "spherical" : True,
                       "key" : "location"}},
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "ts" : {"$push" : "$ts"}}},
        # {"$sort" : {'total' : -1}}
    ]
    # execute query
    results = collection.aggregate(pipeline)
    dict = utils.queryResultsToDict(results)

    utils.queryExplain("ais_navigation", pipeline)
    print("Step 1")
    print("--- %s seconds ---" % (time.time() - start_time))

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

    # print(json.dumps(resultsList, sort_keys=False, indent=4, default=str))

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
                    if not any(ts < t <= ts + (sum(hoursList[:(count + 1)]) * utils.one_hour_in_unix_time) for t in
                               d[key]["ts"]) :
                        isValid = False
                        break
                if isValid :
                    validMMSITimePair.append({"mmsi" : key, "ts" : ts})

            # if isValid :
            #     validMMSITimePair.append(key)

            counter += 1

    print("Step 2 and 3 total execution time")
    print("--- %s seconds ---" % (time.time() - start_time))

    # step 4
    trajectories = []
    for pair in validMMSITimePair :
        trajectories.append(
            relationalQueries.findShipTrajectory(mmsi=pair["mmsi"],
                                                 tsFrom=pair["ts"],
                                                 tsTo=pair["ts"] + (totalHours * utils.one_hour_in_unix_time),
                                                 collection=collection)
        )

    # print(json.dumps(trajectories, sort_keys=False, indent=4, default=str))
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


def executeTrajectoryQuery() :
    choice = -1
    while choice not in main_options :
        choice = trajectoryQueriesMain_menu()

        if choice == '1' :
            # query 1
            print("--------------You choose 1--------------")
            bottomLeftCorner = input("Give bottom left corner separated by comma ex: -5.084,48.161: ")
            upperRightCorner = input("Give top right corner separated by comma ex: -4.939,48.377: ")
            # bottomLeftCorner = input("Give bottom left corner separated by comma ex: -8.00,46.62: ")  # -5.084,48.161
            # upperRightCorner = input("Give top right corner separated by comma ex: -3.76,47.21: ")  # -4.939,48.377
            timeFrom = input("Give Time From eg: 1448988894: ")
            timeTo = input("Give Time To eg: 1449075294: ")

            try :
                box1 = [
                    [float(bottomLeftCorner.split(',')[0]), float(bottomLeftCorner.split(',')[1])],
                    [float(upperRightCorner.split(',')[0]), float(upperRightCorner.split(',')[1])]
                ]
                timeFrom = int(timeFrom)
                timeTo = int(timeTo)
                findTrajectoriesInSpaTemBox(box1, timeFrom=timeFrom, timeTo=timeTo)

            except :
                print("------------------ INVALID ARGUMENTS ------------------")

        elif choice == '2' :
            # query 2
            print("--------------You choose 2--------------")
            bottomLeftCorner = input("Give bottom left corner separated by comma ex: -5.084,48.161: ")
            upperRightCorner = input("Give top right corner separated by comma ex: -4.939,48.377: ")
            # bottomLeftCorner = input("Give bottom left corner separated by comma ex: -8.00,46.62: ")#-5.084,48.161
            # upperRightCorner = input("Give top right corner separated by comma ex: -3.76,47.21: ")#-4.939,48.377
            timeFrom = input("Give Time From eg: 1448988894: ")
            timeTo = input("Give Time To eg: 1449075294: ")

            try :
                poly = {
                    "type" : "Polygon",
                    "coordinates" : [
                        [
                            [float(bottomLeftCorner.split(',')[0]), float(bottomLeftCorner.split(',')[1])],
                            [float(bottomLeftCorner.split(',')[0]), float(upperRightCorner.split(',')[1])],
                            [float(upperRightCorner.split(',')[0]), float(upperRightCorner.split(',')[1])],
                            [float(upperRightCorner.split(',')[0]), float(bottomLeftCorner.split(',')[1])],
                            [float(bottomLeftCorner.split(',')[0]), float(bottomLeftCorner.split(',')[1])]
                        ]
                    ]
                }
                timeFrom = int(timeFrom)
                timeTo = int(timeTo)

                matchAggregation = {"$match" : {'ts' : {"$gte" : timeFrom, "$lte" : timeTo},
                                                "location" : {"$geoWithin" : {"$geometry" : poly}}}}

                utils.findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, withPoly=poly,
                                                   logResponse=False)
            except :
                print("------------------ INVALID ARGUMENTS ------------------")

        elif choice == '3' :
            print("--------------You choose 3--------------")
            print("\n")
            print('|--------------------- Recommended queries ------------------------|')
            print('|                                                                  |')
            print('| 1:  mmsi: 240266000, time From: 1448988894, time To: 1449075294  |')
            print('|     distance_threshold = 12 (in KM) ,                            |')
            print('|     k=null, similarity_threshold = 0.5 (in percentage)           |')
            print('|                                                                  |')
            print('| 2:  mmsi: 227574020, time From: 1443676587, time To: 1443679590  |')
            print('|     distance_threshold = 0 (in KM), k=null or k=2,               |')
            print('|     similarity_threshold = 0.5 (in percentage)                   |')
            print('|                                                                  |')
            print('|     SOS: Minimun distance threshold is 10km (grid size)          |')
            print('|     so when passing threshold 0 it is equivalent to 10 km        |')
            print('|     SOS: Minimun distance threshold is 10km (grid size)          |')
            print('|     so when passing threshold 0 it is equivalent to 10 km        |')
            print('|------------------------------------------------------------------|')

            # read main inputs
            mmsi = input("Give MMSI: ")
            timeFrom = input("Give Time from: ")
            timeTo = input("Give Time To: ")
            d = input("Give distance Threshold: ")

            try :
                mmsi = int(mmsi)
                timeFrom = int(timeFrom)
                timeTo = int(timeTo)
                d = float(d)

            except :
                print("------------------ INVALID ARGUMENTS ------------------")
                return

            # read k if exists and similarity threshold
            k = None
            try:
                k = int(input("Give target k: "))
            except:
                k = None

            similarity_threshold = 0.5
            try:
                similarity_threshold = float(input("Give similarity threshold (default value 0.5): "))
            except:
                similarity_threshold = 0.5

            findThresholdBasedSimilarTrajectories(mmsi=mmsi,
                                                  tsFrom=timeFrom,
                                                  tsTo=timeTo,
                                                  d=d,
                                                  k=k,
                                                  similarity_threshold=similarity_threshold)

        elif choice == '4' :
            print("--------------You choose 3--------------")

            print("\n")
            print('|-------------------- - Query Explanation - -----------------------|')
            print('|                                                                  |')
            print('|     The target of this query is to find trajectories who pass    |')
            print('|     through point A,B and C after a certain period of time       |')
            print('|     We have upgrowth this query even more and user can pass      |')
            print('|     More than 3 points                                           |')
            print('|                                                                  |')
            print('|     example points and times from point to point:                |')
            print('|     [-7.072965, 47.371117] (as initial point)                    |')
            print('|     [-6.494, 47.820],  point to point time = 3 hours             |')
            print('|     [-5.683, 48.480],  point to point time = 3 hours             |')
            print('|     [-4.911, 48.984],  point to point time = 4 hours             |')
            print('|     [-3.683, 49.465],  point to point time = 4 hours             |')
            print('|                                                                  |')
            print('|     Enter latter to start the query!                             |')
            print('|------------------------------------------------------------------|')

            pointList = []
            timeList = []
            point_x = input("Give Initial point longitude: ")
            point_y = input("Give Initial point latitude: ")
            time = 0

            while True :
                try :
                    pointList.append({"type" : "Point", "coordinates" : [float(point_x), float(point_y)]})
                    if len(pointList) != 1 :
                        timeList.append(int(time))

                    point_x = input("Give next point longitude: ")
                    point_y = input("Give next point latitude: ")
                    time = input("Give point to point time: ")

                except :
                    if len(pointList) < 3 :
                        print("------------------ INVALID ARGUMENTS ------------------")
                        return
                    elif len(pointList) >= 3 :
                        break
                    else :
                        print("------------------ At least 3 points needed ------------------")

            print("execute query")
            findTrajectoriesFromPoints(pointList, timeList)
