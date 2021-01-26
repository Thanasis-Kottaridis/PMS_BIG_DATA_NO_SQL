from mongo.query import relationalQueries
from mongo.query import spatialQueries
from mongo.query import spatioTemporalQueries
from mongo.query import distanceJoinQueries
from mongo.query import trajectoryQueries

import dataPreprocessing_2
import json
import mongo.mongoSetUp as mongoSetUp
from geospatial import geoDataPreprocessing
import mortonCodeManager
import numpy as np

main_options = ['1', '2', '3', '4', '5', '0']


def main_menu():
    print("\n")
    print('|-------------- PMS MARINE TRAFFIC-----------------|')
    print('|                                                  |')
    print('| 1.  Relational queries                           |')
    print('| 2.  Spatial queries                              |')
    print('| 3.  Spatio-temporal queries                      |')
    print('| 4.  Distance Join Queries                        |')
    print('| 5.  Trajectory queries                           |')
    print('|                                                  |')
    print('| 0.  Exit                                         |')
    print('|--------------------------------------------------|')
    return input('Your choice: ')


if __name__ == '__main__' :
    choice = -1
    while choice not in main_options :
        choice = main_menu()

        # check if user choose relational queries
        if choice == '1' :
            print("--------------You choose Relational queries--------------")
            relationalQueries.executeRelationalQuery()
        elif choice == '2':
            print("--------------You choose Spatial queries--------------")
            spatialQueries.executeSpatialQuery()
        elif choice == '3' :
            print("--------------You choose Spatio-temporal queries--------------")
            spatioTemporalQueries.executeSpatioTemporalQuery()
        elif choice == '4' :
            print("--------------You choose Distance Join Queries--------------")
            distanceJoinQueries.executeDistanceJoinQuery()
        elif choice == '5' :
            print("--------------You choose Trajectory queries--------------")
            trajectoryQueries.executeTrajectoryQuery()


    # # insert ais_navigation data to mongo
    # ais_navigation_json = dataPreprocessing_2.preprocessAisDynamic()
    #
    # # insert ports geo point data in mongo
    # # 1) load json file from datasetJSON
    # # 2) upload it
    # with open("geospatial/datasetJSON/port.json") as f :
    #     data = json.load(f)
    #     mongoSetUp.insertPortData(data["features"])
    #
    # # insert ports full details data in mongo
    # # 1) load json file from datasetJSON
    # # 2) upload it
    # with open("geospatial/datasetJSON/WPI.json") as f :
    #     data = json.load(f)
    #     mongoSetUp.insertFullDetailedPortsData(data["features"])
    #
    # # insert world seas data in mongo
    # # 1) load json file from datasetJSON
    # # 2) upload it
    # with open("geospatial/datasetJSON/World_Seas_IHO_v2.json") as f :
    #     data = json.load(f)
    #     mongoSetUp.insertWorldSeas(data)
    #
    # # insert ports data in mongo
    # # 1) load json file from datasetJSON
    # # 2) upload it
    # with open("geospatial/datasetJSON/Fishing Ports.json") as f :
    #     data = json.load(f)
    #     mongoSetUp.insertFishingPortData(data)
    #
    # # mongoDBManager.insertData(ais_navigation_json)
    #
    # # EXTRACT A DUMMY MORTON CODE
    # # TODO CREATE MORTON EXTENSION FOR CONVERTING
    # # morton = mortonCodeManager.EncodeMorton4D(np.uint32(5), np.uint32(9), np.uint32(1), np.uint32(7))
    # # print(morton)
    # # print("X", mortonCodeManager.DecodeMorton4DX(morton))
    # # print("Y", mortonCodeManager.DecodeMorton4DY(morton))
    # # print("Z", mortonCodeManager.DecodeMorton4DZ(morton))
    # # print("T", mortonCodeManager.DecodeMorton4Dt(morton))
    #
    #
    # """
    #     Create countries collection
    # """
    # # get country data
    # countries = dataPreprocessing_2.fetchMMSICountryData(isForAIS=False)
    # # converting into list
    #
    # countries = [countries[key] for key in countries.keys()]
    # print(countries)
    #
    # # upload to mongo
    # mongoSetUp.insertCountries(countries)
    #
    # # insert ports data in mongo
    # # 1) load json file from datasetJSON
    # # 2) upload it
    # with open("json_data/test_poly.json") as f :
    #     data = json.load(f)
    #     mongoSetUp.insertTestPolyData(data)
    #
    # """
    # Create grid for target seas and link it to ais documents
    # """
    # # generate map grid
    # geoDataPreprocessing.createGridForTargetSeas()
    #
    # # link grid to documents
    # mongoSetUp.linkGridToDocuments()
    #
    # # insert ports data in mongo
    # # 1) calculate grids for target seas
    # # 2) upload it
    # grid_list = geoDataPreprocessing.createGridForTargetSeas()
    # mongoSetUp.insertMapGrid(grid_list)



