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

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
        return None, None


# create json object from AIS Collection
def preprocessDynamicData(dynamic_data, column_names) :
    ais_collection = []

    # converts query records into list of dicts
    for row in dynamic_data :
        main_data = dict(zip(column_names[:4], row[:4]))
        navigational_status = dict(zip(column_names[4 :6], row[4 :6]))
        navigational_metadata = dict(zip(column_names[4 :], row[4 :]))
        main_data["nav_status"] = navigational_status
        main_data["nav_metadata"] = navigational_metadata
        ais_collection.append(main_data)

    # prints last main_data dict using json dumps in order to check the format
    print(json.dumps(main_data, sort_keys=True, indent=4))

    return ais_collection


# creates a dict/json object with the ship metadata
def preprocessShipMetaData(ship_meta, ship_meta_columns, ship_types, ship_types_columns):
    ship_meta_dict = {}
    ship_types_dict = {}

    # firstly we convert types into a dict of dicts using id_detailedtype as key on ship_types_dict
    # which is column 0 in our query results and we know it is unique
    for row in ship_types:
        ship_types_dict[row[0]] = dict(zip(ship_types_columns, row))

    print(json.dumps(ship_types_dict, sort_keys=True, indent=4))

    # converts query records into dict of dicts
    for row in ship_meta:
        # convert all columns into a dict except the last which is the shiptype
        ship_data = dict(zip(ship_meta_columns[:- 1], row[:- 1]))
        # adding to ship data the type using last column as key on ship_types_dict
        try:
            ship_data["ship_type"] = ship_types_dict[row[-1]]
        except:
            print("INVALID SHIP TYPE: ", row[-1])

        # sets mmsi which is row[0] in our data as key and ands main data into dict
        ship_meta_dict[row[0]] = ship_data
    print(json.dumps(ship_data, sort_keys=True, indent=4))
    return  ship_meta_dict


if __name__ == '__main__':
    # first fetch all dynamicData
    # but we order the columns in order to get all the main attributes at first 4 columns
    # and the navigational_metadata form rest columns
    # TODO UNCOMMENT THIS :P
    # column_names, dynamic_data = executeQuery(
    #     "SELECT mmsi, lat, lon, ts, id_status, definition, turn, speed, course, heading, geom"
    #     " FROM ais_data.dynamic_ships D INNER JOIN ais_status_codes_types.navigational_status S"
    #     " ON D.status = S.id_status ORDER BY mmsi"
    #     " LIMIT 100"
    # )
    # print(column_names)
    # print(len(dynamic_data))
    #
    # # convert dynamic data into a desire formatted dict
    # ais_collection = preprocessDynamicData(dynamic_data, column_names)
    # print("prin collection count: \n\n")
    # print(ais_collection)
    # TODO ----------------------------------

    # fetch ship metadata extracted from static_ships using postgres
    ship_meta_column_names, ship_metadata = executeQuery(
        """
        with static_metadata as (
	        SELECT DISTINCT ON (sourcemmsi) sourcemmsi, imo, callsign, shipname, shiptype
	        FROM  ais_data.static_ships
	    ) SELECT mmsi, imo, callsign, shipname, shiptype
	    FROM (select distinct mmsi from ais_data.dynamic_ships ) as MMSI 
	    INNER JOIN static_metadata meta ON MMSI.mmsi = meta.sourcemmsi
        """
    )

    print(ship_meta_column_names)
    print(len(ship_metadata))

    # fetch ship type with details
    ship_type_column_names, ship_types = executeQuery(
        """
        SELECT  D.id_detailedtype, D.detailed_type, T.id_shiptype, T.type_name, T.ais_type_summary
        FROM ais_status_codes_types.ship_types T INNER JOIN ais_status_codes_types.ship_types_detailed D
        ON T.id_shiptype = D.id_shiptype
        """
    )

    print(ship_type_column_names)
    print(len(ship_types))

    ship_metadata_dict = preprocessShipMetaData(ship_metadata,ship_meta_column_names, ship_types, ship_type_column_names)
    print(json.dumps(ship_metadata_dict, sort_keys=True, indent=4))

