import geopandas as gpd
import os

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


if __name__ == '__main__':
    # shpToCSV()
    shpToJson()
