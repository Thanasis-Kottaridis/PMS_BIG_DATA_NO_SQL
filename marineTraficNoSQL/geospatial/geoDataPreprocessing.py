import geopandas as gpd
import os
import mongo.mongoConnector as connector
from descartes import PolygonPatch
import matplotlib.pyplot as plt
import mongo.mongoUtils as mongoUtils



datasetPath = "/Users/thanoskottaridis/Documents/metaptixiako_files/main lectures/noSQL/apalaktiki ergasia/dataSet"
csvBasePath = "datasetCSV"
jsonBasePath = "datasetJSON"

my_list = os.listdir(datasetPath)
print(my_list)


def shpToCSV():
    # iterate a directory
    for root, directories, files in os.walk(datasetPath):
        # get all files from nested directory
        for filename in files:
            # checks if filetype is shp
            if filename.endswith("shp"):
                print(root)
                print(filename)
                # finds csv name
                csvfile_path = filename.replace(".shp", ".csv")
                csvfile_path = csvBasePath+'/'+csvfile_path

                print(csvfile_path)
                # Loads shape file
                shapefile = gpd.read_file(root)
                # Save it as CSV in projects datasetCSV directory
                shapefile.to_csv(csvfile_path, index=False, mode='w')


def shpToJson():
    # iterate a directory
    for root, directories, files in os.walk(datasetPath) :
        # get all files from nested directory
        for filename in files :
            # checks if filetype is shp
            if filename.endswith("shp") :
                print(root)
                print(filename)
                # finds Json name
                jsonfile_path = filename.replace(".shp", ".json")
                jsonfile_path = jsonBasePath + '/' + jsonfile_path

                print(jsonfile_path)
                # Loads shape file
                shapefile = gpd.read_file(root)
                # Save it as Json in projects datasetJSON directory
                shapefile.to_file(jsonfile_path, driver='GeoJSON')


def createGridForTargetSeas():
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.world_seas

    results = list(collection.find())

    # print sea polygons
    ax = mongoUtils.createAXNFigure()
    BLUE = '#6699cc'
    GRAY = '#999999'

    grid_list = []

    for poly in results:
        poly_boundaries = {
            "type" : "Polygon",
            "coordinates" : [poly["geometry"]["coordinates"][0]]
        }
        grid = mongoUtils.getPolyGrid(poly_boundaries, theta=10)
        ax.add_patch(
            PolygonPatch(poly["geometry"], fc=BLUE, ec=BLUE, alpha=0.5, zorder=2, label="Trajectories Within Polygon"))

        for cell in grid["geometry"]:

            grid_list.append(
                {
                    "seaID": poly["_id"],
                    "geometry": cell.__geo_interface__
                }
            )

            ax.add_patch(
                PolygonPatch(cell, fc=GRAY, ec=GRAY, alpha=0.5, zorder=2))

        print(len(grid_list))

    print(len(grid_list))
    print(grid_list[0])

    plt.ylabel("Latitude")
    plt.xlabel("Longitude")
    plt.show()

    return grid_list


if __name__ == '__main__':
    # shpToCSV()
    shpToJson()
