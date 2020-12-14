import dataPreprocessing
import json
import mongoDBManager
import mortonCodeManager
import numpy as np

if __name__ == '__main__' :
    # insert ais_navigation data to mongo
    ais_navigation_json = dataPreprocessing.preprocessAisDynamic()

    # insert ports geo point data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    # with open("geospatial/datasetJSON/port.json") as f :
    #     data = json.load(f)
    #     mongoDBManager.insertPortData(data["features"])

    # insert ports full details data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    # with open("geospatial/datasetJSON/WPI.json") as f :
    #     data = json.load(f)
    #     mongoDBManager.insertFullDetailedPortsData(data["features"])

    # insert world seas data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    # with open("geospatial/datasetJSON/World_Seas_IHO_v2.json") as f :
    #     data = json.load(f)
    #     mongoDBManager.insertWorldSeas(data)

    # insert ports data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    # with open("geospatial/datasetJSON/Fishing Ports.json") as f :
    #     data = json.load(f)
    #     mongoDBManager.insertFishingPortData(data)

    # mongoDBManager.insertData(ais_navigation_json)

    # EXTRACT A DUMMY MORTON CODE
    # TODO CREATE MORTON EXTENSION FOR CONVERTING
    # morton = mortonCodeManager.EncodeMorton4D(np.uint32(5), np.uint32(9), np.uint32(1), np.uint32(7))
    # print(morton)
    # print("X", mortonCodeManager.DecodeMorton4DX(morton))
    # print("Y", mortonCodeManager.DecodeMorton4DY(morton))
    # print("Z", mortonCodeManager.DecodeMorton4DZ(morton))
    # print("T", mortonCodeManager.DecodeMorton4Dt(morton))
