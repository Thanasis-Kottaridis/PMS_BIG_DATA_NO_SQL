import geopandas as gpd
from shapely.geometry import Polygon
import numpy as np
import matplotlib.pyplot as plt
import mongoDBManager


def createAXNFigure() :
    # geopandas basic world map with out details
    # world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    world = gpd.read_file("geospatial/EuropeanCoastline/Europe Coastline (Polygone).shp")
    world.to_crs(epsg=4326, inplace=True)  # convert axes tou real world coordinates

    ax = world.plot(figsize=(10, 6))
    plt.axis([-20, 15, 40, 60])  # set plot bounds
    return ax


def getPolyGrid(poly, theta):
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