import mongo.mongoConnector as connector
from geospatial import geoDataPreprocessing
import dataPreprocessing_2
import json
import geopandas as gpd
from shapely.geometry import shape


def insertAISData(insertDoc, isMany=True, isTemp=False) :
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    if isTemp:
        collection = db.ais_navigation_temp
    else:
        collection = db.ais_navigation

    # insert data based on whether it is many or not
    if isMany :
        collection.insert_many(insertDoc)
    else :
        collection.insert_one(insertDoc)

    # printData(collection)


def insertPortData(insertDoc, isMany=True) :
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.world_port_geo

    # insert data based on whether it is many or not
    if isMany :
        collection.insert_many(insertDoc)
    else :
        collection.insert_one(insertDoc)


def insertFishingPortData(insertDoc, isMany=True) :
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.fishing_port

    # insert data based on whether it is many or not
    if isMany :
        collection.insert(insertDoc)
    else :
        collection.insert_one(insertDoc)


def insertTestPolyData(insertDoc, isMany=True) :
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.query_polygons

    # insert data based on whether it is many or not
    if isMany :
        collection.insert_many(insertDoc)
    else :
        collection.insert_one(insertDoc)


def insertFullDetailedPortsData(insertDoc, isMany=True) :
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.world_port_information

    # insert data based on whether it is many or not
    if isMany :
        collection.insert_many(insertDoc)
    else :
        collection.insert_one(insertDoc)


def insertWorldSeas(data, isMany=True) :
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.world_seas

    # print(len(data["features"][""]))
    insertDoc = []
    targetSeas = ["Celtic Sea",
                 "Bay of Biscay",
                 "English Channel",
                 "Bristol Channel",
                 "St. George's Channel"
                 ]

    for sea in data["features"] :
        properties = sea["properties"]
        if properties["NAME"] in targetSeas:
            print(properties["NAME"])
            insertDoc.append(sea)

    # insert data based on whether it is many or not
    if isMany :
        collection.insert_many(insertDoc)
    else :
        collection.insert_one(insertDoc)


def insertMapGrid(insertDoc, isMany=True):
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.target_map_grid

    # insert data based on whether it is many or not
    if isMany :
        collection.insert_many(insertDoc)
    else :
        collection.insert_one(insertDoc)


def insertCountries(insertDoc, isMany=True) :
    connection, db = connector.connectMongoDB()

    # creating or switching to ais_navigation collection
    collection = db.countries

    # insert data based on whether it is many or not
    if isMany :
        collection.insert_many(insertDoc)
    else :
        collection.insert_one(insertDoc)


def linkGridToDocuments():
    """{ "_id" : {  }, "min" : 1443650401, "max" : 1459461599 }
    """
    connection, db = connector.connectMongoDB()

    # get grids
    collection = db.target_map_grid
    map_grids = list(collection.find())

    # fix geometry for geopandas
    for grid in map_grids:
        grid["geometry"] = shape(grid["geometry"])

    # get gopandas dataframe
    map_grids_df = gpd.GeoDataFrame(map_grids)

    # creating or switching to ais_navigation collection
    collection = db.ais_navigation_temp

    print("connected")

    # db.ais_navigation2.find({"ts": {"$gte": 1443650401, "$lt": 1444309200}})

    min_ts = 1443650401 #1444309200
    max_ts = 1459461599
    step = int((max_ts-min_ts)/50)  # 1 week approximately
    count = 0

    # TODO UNCOMMENT FOR TEST
    # results = list(collection.find())
    # count += len(results)
    # __findGridCalculations__(map_grids_df, results)

    # TODO COMMENT FOR TEST
    for ts in range(min_ts+step, max_ts + step, step):
        results = list(collection.find({"ts": {"$gte": ts - step, "$lt": ts}}))
        count += len(results)
        __findGridCalculations__(map_grids_df, results)

    print(count)


def __findGridCalculations__(map_grids_df, ais_batch):
    results_dict = {}
    pings = []
    for ping in ais_batch :
        results_dict[ping["_id"]] = ping
        pings.append({"_id" : ping["_id"], "geometry" : shape(ping["location"])})

    pings_df = gpd.GeoDataFrame(pings)
    pings_df = pings_df.rename(columns={'location' : 'geometry'})
    pings_per_grid = gpd.sjoin(pings_df, map_grids_df, how="inner", op='intersects')
    pings_per_grid = pings_per_grid.drop(['geometry', 'index_right', 'seaID'], axis=1)
    pings_per_grid = pings_per_grid.set_index("_id_left")

    for index, row in pings_per_grid.iterrows() :
        results_dict[index]["grid_id"] = row['_id_right']

    list_of_pings = [value for value in results_dict.values()]
    print(list_of_pings[0])
    print(pings_per_grid.head(5))
    print(pings_per_grid.columns)

    # upload data to mongo
    insertAISData(list_of_pings)


def mongoSetUp():
    # insert ais_navigation data to mongo
    dataPreprocessing_2.preprocessAisDynamic()

    # generate json files from original datasets shp's
    # geoDataPreprocessing.shpToJson()

    # insert ports geo point data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    with open("../geospatial/datasetJSON/port.json") as f :
        data = json.load(f)
        insertPortData(data["features"])

    # insert ports full details data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    with open("../geospatial/datasetJSON/WPI.json") as f :
        data = json.load(f)
        insertFullDetailedPortsData(data["features"])

    # insert world seas data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    with open("../geospatial/datasetJSON/World_Seas_IHO_v2.json") as f :
        data = json.load(f)
        insertWorldSeas(data)

    # insert ports data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    with open("../geospatial/datasetJSON/Fishing Ports.json") as f :
        data = json.load(f)
        insertFishingPortData(data)


    """
        Create countries collection
    """
    # get country data
    countries = dataPreprocessing_2.fetchMMSICountryData(isForAIS=False)
    # converting into list

    countries = [countries[key] for key in countries.keys()]
    print(countries)

    # upload to mongo
    insertCountries(countries)

    # insert ports data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    with open("../json_data/test_poly.json") as f :
        data = json.load(f)
        insertTestPolyData(data)

    """
    Create grid for target seas and link it to ais documents
    """

    # generate map grid
    # 1) calculate grids for target seas
    # 2) upload it
    grid_list = geoDataPreprocessing.createGridForTargetSeas()
    insertMapGrid(grid_list)

    # link grid to documents
    linkGridToDocuments()


def mongoSetUpServer():
    # insert ports geo point data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    with open("../geospatial/datasetJSON/port.json") as f :
        data = json.load(f)
        insertPortData(data["features"])

    # insert ports full details data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    with open("../geospatial/datasetJSON/WPI.json") as f :
        data = json.load(f)
        insertFullDetailedPortsData(data["features"])

    # insert world seas data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    with open("../geospatial/datasetJSON/World_Seas_IHO_v2.json") as f :
        data = json.load(f)
        insertWorldSeas(data)

    # insert fishing ports data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    with open("../geospatial/datasetJSON/Fishing Ports.json") as f :
        data = json.load(f)
        insertFishingPortData(data)

    """
        Create countries collection
    """
    # get country data
    countries = dataPreprocessing_2.fetchMMSICountryData(isForAIS=False)
    # converting into list

    countries = [countries[key] for key in countries.keys()]
    print(countries)

    # upload to mongo
    insertCountries(countries)

    # insert ports data in mongo
    # 1) load json file from datasetJSON
    # 2) upload it
    with open("../json_data/test_poly.json") as f :
        data = json.load(f)
        insertTestPolyData(data)


if __name__ == '__main__':
    if connector.isLocal:
        mongoSetUp()
    else:
        mongoSetUpServer()

