"""
    This file contains all the helper functions that created in order to perform
    pre query or post query operations in mongo

    And Also contains Plotting helper functions in order to visualize our results
"""

import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import shapely.geometry as sg
from shapely.geometry import Polygon
from shapely.geometry import LineString
import json

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
    world = gpd.read_file("geospatial/EuropeanCoastline/Europe Coastline (Polygone).shp")
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