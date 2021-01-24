"""
     Bullet 3: Spatio-temporal queries
    Range, k-nn, distance join queries

    query1: Find all ships that moved in range from 10 to 30 sea miles from Burst port for one day time range
    for 2 hours time interval
    (to many ping it took a lot of time to find them all
    first run --- 408.56871485710144 seconds ---
    second run --- 220.98046803474426 seconds --- (great improvement))

    query2: find k closest ship sigmas to Brest port (k=100) in one day interval

    query3: find trajectories for all ships with france flag in Celtic Sea  for one hour interval
    (den etrekse pote poli megali perioxi + oti argi pou argei gia tin simea)
    times:
    time to get Celtic Sea from world seas collection --- 0.040419816970825195 seconds ---
    time to get all greek ships mmsi--- 0.052060842514038086 seconds ---
    time to get trajectories in spatio temporal box--- 1.3465988636016846 seconds ---
"""

# my packages
from mongo.query import relationalQueries
from mongo import mongoUtils as utils


main_options = ['1', '2', '3', '0']


def spatialQueries_menu():
    print("\n")
    print('|---------------- Spatial queries Menu -------------------|')
    print('|                                                         |')
    print('| 1.  Find all ships that moved in range from             |')
    print('|     10 to 30 sea miles from Burst port                  |')
    print('|     for one day time range for 2 hours time interval    |')
    print('|     one day time range for 2 hours time interval        |')
    print('| 2.  find k closest ship sigmas to Brest                 |')
    print('|     port (k=100) in one day interval                    |')
    print('| 3.  find trajectories for all ships with france         |')
    print('|     flag in Celtic Sea  for one hour interval           |')
    print('|                                                         |')
    print('| 0.  Exit                                                |')
    print('|---------------------------------------------------------|')
    return input('Your choice: ')


def executeRelationalQuery():
    choice = -1
    while choice not in main_options :
        choice = spatialQueries_menu()

        if choice == '1' :
            # query 1
            print("--------------You choose 1--------------")
            port_point = relationalQueries.findPort()
            geoNearAgg = {
                "$geoNear" : {"near" : {"type" : "Point", "coordinates" : port_point['geometry']['coordinates'][0]},
                              "distanceField" : "dist.calculated",
                              "minDistance" : utils.nautical_mile_in_meters * 10,
                              "maxDistance" : utils.nautical_mile_in_meters * 30,
                              "spherical" : True, "key" : "location"}}

            matchAgg = {"$match" : {'ts' : {"$gte" : 1448988894, "$lte" : 1448988894 + (2 * utils.one_hour_in_unix_time)}}}

            utils.findPointsForMatchAggr(geoNearAgg, matchAgg, doPlot=True,
                                   queryTitle="Find all ships that moved in range from 10 to 30 sea miles from Burst port for 2 hours interval")

        elif choice == '2' :
            # query 2
            print("--------------You choose 2--------------")
            port_point = relationalQueries.findPort()
            point = {"type" : "Point", "coordinates" : port_point['geometry']['coordinates'][0]}
            geoNearAgg = {"$geoNear" : {"near" : point,
                                        "distanceField" : "dist.calculated",
                                        # "minDistance" : nautical_mile_in_meters * 10,
                                        "maxDistance" : utils.nautical_mile_in_meters * 30,
                                        "spherical" : True, "key" : "location"}}

            matchAgg = {"$match" : {'ts' : {"$gte" : 1448988894, "$lte" : 1449075294}}}

            utils.findPointsForMatchAggr(geoNearAgg, matchAgg, k_near=100, doPlot=True,
                                   queryTitle="find k closest ship sigmas to a point (k=100) in one day interval")

        elif choice == '3' :
            print("--------------You choose 3--------------")
            poly = relationalQueries.findPolyFromSeas()
            shipMMSI = relationalQueries.getShipsByCountry(["France"])
            matchAggregation = {"$match" : {'mmsi' : {'$in' : shipMMSI.tolist()},
                                            "location" : {"$geoWithin" : {"$geometry" : poly["geometry"]}},
                                            'ts' : {"$gte" : 1448988894,
                                                    "$lte" : 1448988894 + (6 * utils.one_hour_in_unix_time)}}}
            utils.findTrajectoriesForMatchAggr(matchAggregation, doPlot=True, withPoly=poly["geometry"], logResponse=True)


