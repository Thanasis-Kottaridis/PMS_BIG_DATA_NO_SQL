import geopandas as gpd
from shapely.geometry import Polygon
import numpy as np
import mongoDBManager

# def getPolyGrid():
#      points = gpd.read_file('points.shp')
#      xmin,ymin,xmax,ymax =  points.total_bounds
#      width = 2000
#      height = 1000
#      rows = int(np.ceil((ymax-ymin) /  height))
#      cols = int(np.ceil((xmax-xmin) / width))
#      XleftOrigin = xmin
#      XrightOrigin = xmin + width
#      YtopOrigin = ymax
#      YbottomOrigin = ymax- height
#      polygons = []
#      for i in range(cols):
#         Ytop = YtopOrigin
#         Ybottom =YbottomOrigin
#         for j in range(rows):
#             polygons.append(Polygon([(XleftOrigin, Ytop), (XrightOrigin, Ytop), (XrightOrigin, Ybottom), (XleftOrigin, Ybottom)]))
#             Ytop = Ytop - height
#             Ybottom = Ybottom - height
#         XleftOrigin = XleftOrigin + width
#         XrightOrigin = XrightOrigin + width
#
#     grid = gpd.GeoDataFrame({'geometry':polygons})
#     grid.to_file("grid.shp")


def getPolyGrid2(poly, theta):
    # points = gpd.read_file('geospatial/EuropeanCoastline/Europe Coastline (Polygone).shp')

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
    # print(polygons)
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