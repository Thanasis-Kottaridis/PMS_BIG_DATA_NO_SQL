"""
    This file contains all the helper functions that created in order to perform
    pre query or post query operations in mongo

    And Also contains Plotting helper functions in order to visualize our results
"""
from mongo import mongoConnector as connector

import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import shapely.geometry as sg
from shapely.geometry import LineString
from shapely.geometry import Polygon
from descartes import PolygonPatch
import geog
import json
import time

# CONSTS
nautical_mile_in_meters = 1852
one_hour_in_unix_time = 3600

#
# PLOT UTILS
#
BLUE = '#6699cc'
GRAY = '#999999'
RED = '#B20000'


def createAXNFigure() :
    """
    creates a map ploting european coastline
    :return: the ax
    """
    # geopandas basic world map with out details
    # world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    world = gpd.read_file("/Users/thanoskottaridis/Documents/metaptixiako_files/main lectures/noSQL/apalaktiki ergasia/PMS_BIG_DATA_NO_SQL/marineTraficNoSQL/geospatial/EuropeanCoastline/Europe Coastline (Polygone).shp")
    world.to_crs(epsg=4326, inplace=True)  # convert axes tou real world coordinates

    ax = world.plot(figsize=(10, 6))
    plt.axis([-20, 15, 40, 60])  # set plot bounds
    return ax


def get_cmap(n, name='hsv') :
    """
    Returns a function that maps each index in 0, 1, ..., n-1 to a distinct
    RGB color; the keyword argument name must be a standard mpl colormap name.
    """
    return plt.cm.get_cmap(name, n)


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

#
# MONGO PRE/POST QUERY UTILS
#


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


def queryResultToDictList(results, dictlist=[]) :
    """
    this helper func appends pymongo query results into a list of dictionaries

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


def pointsListToLineString(pointList) :
    """
        This helper func converts a point list into lineString
    :param pointList: target point List
    :return: LineString geoJson
    """
    multiPoint = {
        "type" : "LineString",
        "coordinates" : pointList
    }
    return multiPoint


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
    print(json.dumps(geoPolygon, sort_keys=False, indent=4, default=str))

    return geoPolygon, dilated, eroded


def findTrajectoriesForMatchAggr(matchAggregation, collection=None, doPlot=False, withPoly=None, logResponse=False, allowDiskUse=False) :
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
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

    # create mongo aggregation pipeline
    pipeline = [
        matchAggregation,
        {"$sort" : {"ts" : 1}},
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}, "ts" : {"$push" : "$ts"}}},
        {"$sort" : {'total' : -1}}
    ]

    # execute query
    results = collection.aggregate(pipeline, allowDiskUse=allowDiskUse)
    dictlist = queryResultToDictList(results)
    print("--- %s seconds ---" % (time.time() - start_time))
    if logResponse :
        print(json.dumps(dictlist, sort_keys=False, indent=4, default=str))

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
            trajj = pointsListToLineString(ship["location"])

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
        # connecting or switching to the database
        connection, db = connector.connectMongoDB()

        # creating or switching to ais_navigation collection
        collection = db.ais_navigation

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

    # execute query
    results = collection.aggregate(pipeline, allowDiskUse=allowDiskUse)
    dictlist = queryResultToDictList(results)
    print("--- %s seconds ---" % (time.time() - start_time))
    if logResponse :
        print(json.dumps(dictlist, sort_keys=False, indent=4, default=str))

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


"""
    #
    # Spatial Join Utils
    #
"""


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


"""
    #
    # GRID UTILS
    #
"""


def getPolyGrid(poly, theta):
    """
    Helper func for generating a grid for a target poly
    :param poly: target poly
    :param theta: grid square sidi length in meters
    :return: a grid geo dataframe
    """

    # geom = [Polygon(poly["coordinates"](i) for i in poly["coordinates"])]
    geom = Polygon(poly["coordinates"][0])
    poly_gpd = gpd.GeoDataFrame({'Country': "poly", 'geometry':[geom]})
    xmin, ymin, xmax, ymax = poly_gpd.total_bounds

    kmPerDegree = 1/111
    length = wide = kmPerDegree*theta

    cols = list(np.arange(xmin, xmax + wide, wide))
    rows = list(np.arange(ymin, ymax + length, length))
    rows.reverse()

    polygons = []
    for x in cols :
        for y in rows :
            polygons.append(Polygon([(x, y), (x + wide, y), (x + wide, y - length), (x, y - length)]))

    grid = gpd.GeoDataFrame({'geometry' : polygons})
    valid_grid = gpd.sjoin(grid, poly_gpd, how="inner", op='intersects')
    print(len(poly))
    return valid_grid
    # grid.to_file("grid.shp")


#
# Query explain util
#
def queryExplain(collectionStr, pipeline):
    if connector.showQueryExplain:
        _, db = connector.connectMongoDB()
        explain = db.command('aggregate', collectionStr, pipeline=pipeline, explain=True)
        print("\n----------------- Query Explain -----------------\n")


def calculateTotalDocs(dictList):
    count = 0
    for result in dictList:
        count += result["total"]

    return count
