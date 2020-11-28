import psycopg2
import json


# this func is used for executing query on postgress sql it takes the query as parameter and returns cursor data
def executeQuery(query) :
    try :
        # CONNECTING TO POSTGRES (use your credentials here)
        connection = psycopg2.connect(user="thanoskottaridis",
                                      password="Test1234!",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="doi105281zenodo1167595")
        cursor = connection.cursor()

        cursor.execute(query)
        print("Selecting rows using cursor.fetchall")
        column_names = [column[0] for column in cursor.description]
        query_records = cursor.fetchall()

        return column_names, query_records

    except (Exception, psycopg2.Error) as error :
        print("Error while fetching data from PostgreSQL", error)
        return None, None


# create json object from AIS Collection
def preprocessDynamicData(dynamic_data, column_names) :
    ais_collection = []

    # converts query records into dict
    for row in dynamic_data :
        main_data = dict(zip(column_names[:4], row[:4]))
        navigational_status = dict(zip(column_names[4:6], row[4:6]))
        navigational_metadata = dict(zip(column_names[4:], row[4:]))
        main_data["nav_status"] = navigational_status
        main_data["nav_metadata"] = navigational_metadata
        ais_collection.append(main_data)

    # prints last main_data dict using json dumps in order to check the format
    print(json.dumps(main_data, sort_keys=True, indent=4))

    return ais_collection


# creates a dict/json object with the ship metadata
# def preprocessShipMetaData():
#
#     # sets mmsi which is row[0] in our data as key and ands main data into dict
#

if __name__ == '__main__' :
    # first fetch all dynamicData
    # but we order the columns in order to get all the main attributes at first 4 columns
    # and the navigational_metadata form rest columns
    column_names, dynamic_data = executeQuery(
        "SELECT mmsi, lat, lon, ts, id_status, definition, turn, speed, course, heading, geom"
        " FROM ais_data.dynamic_ships D INNER JOIN ais_status_codes_types.navigational_status S"
        " ON D.status = S.id_status ORDER BY mmsi"
        " LIMIT 100"
        )
    print(column_names)
    print(len(dynamic_data))

    # convert dynamic data into a desire formatted dict
    ais_collection = preprocessDynamicData(dynamic_data, column_names)
    print("prin collection count: \n\n")
    print(ais_collection)

    # fetch ship metadata extracted from static_ships using postgres
    # ship_meta_column_names, ship_metadata = executeQuery(
    #     ""
    #     )
