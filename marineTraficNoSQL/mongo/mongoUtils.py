"""
    This file contains all the helper functions that created in order to perform
    pre query or post query operations in mongo

    And Also contains Plotting helper functions in order to visualize our results
"""
from mongo import mongoConnector as connector

import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import shapely.geometry as sg
from shapely.geometry import LineString
from shapely.geometry import Polygon
from descartes import PolygonPatch
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
    print(json.dumps(geoPolygon, sort_keys=False, indent=4))

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
        {"$group" : {"_id" : "$mmsi", "total" : {"$sum" : 1}, "location" : {"$push" : "$location.coordinates"}}},
        {"$sort" : {'total' : -1}}
    ]

    # execute query
    results = collection.aggregate(pipeline, allowDiskUse=allowDiskUse)
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

# import geopands as gpd
# from shapely.geometry.polygon import Polygon
# from shapely.geometry.multipolygon import MultiPolygon
#
# def explode(indata):
#     indf = gpd.GeoDataFrame.from_file(indata)
#     outdf = gpd.GeoDataFrame(columns=indf.columns)
#     for idx, row in indf.iterrows():
#         if type(row.geometry) == Polygon:
#             outdf = outdf.append(row,ignore_index=True)
#         if type(row.geometry) == MultiPolygon:
#             multdf = gpd.GeoDataFrame(columns=indf.columns)
#             recs = len(row.geometry)
#             multdf = multdf.append([row]*recs,ignore_index=True)
#             for geom in range(recs):
#                 multdf.loc[geom,'geometry'] = row.geometry[geom]
#             outdf = outdf.append(multdf,ignore_index=True)
#     return outdf