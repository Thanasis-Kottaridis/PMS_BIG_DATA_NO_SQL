/* fetch navigational dynamic data with detaild navigational status*/
SELECT mmsi, lat, lon, ts, turn, speed, course, heading, geom, status
FROM ais_data.dynamic_ships D 
-- INNER JOIN ais_status_codes_types.navigational_status S
-- ON D.status = S.id_status
-- ORDER BY mmsi 
LIMIT 100

SELECT * FROM ais_status_codes_types.navigational_status


SELECT COUNT(mmsi = null) FROM ais_data.dynamic_ships /*5505 kati tetoio KAI OLA EXOUN MMSI*/
SELECT COUNT(DISTINCT sourcemmsi) FROM ais_data.static_ships /*4842*/

/*ship metadata for 4842*/
SELECT DISTINCT ON (sourcemmsi) sourcemmsi, imo, callsign, shipname, shiptype
		FROM  ais_data.static_ships
		where sourcemmsi = '37100300'

 
/*find all common mmsi from dynamics and static
	we have 3774 mutual mmsi opote mporoume na exoume ship metadata fia auta ta plia
*/
with ship_metadata as (
	with static_metadata as (
	SELECT DISTINCT ON (sourcemmsi) sourcemmsi, imo, callsign, shipname, shiptype
	FROM  ais_data.static_ships
	) select mmsi, imo, callsign, shipname, shiptype
	from (select distinct mmsi from ais_data.dynamic_ships ) as MMSI inner join static_metadata meta on MMSI.mmsi = meta.sourcemmsi
)SELECT SM.mmsi, SM.imo, SM.callsign, SM.shipname, SM.shiptype, T.id_detailedtype, T.detailed_type, T.id_shiptype 
FROM ship_metadata SM INNER JOIN (SELECT * FROM ais_status_codes_types.ship_types_detailed ) AS T
ON SM.shiptype = T.id_detailedtype





/*get ship type and ship types detailed
TODO:- CHECK IF WE CAN USE THIS*/
SELECT  D.id_detailedtype, D.detailed_type, T.id_shiptype, T.type_name, T.ais_type_summary
FROM ais_status_codes_types.ship_types T INNER JOIN ais_status_codes_types.ship_types_detailed D
ON T.id_shiptype = D.id_shiptype

SELECT * FROM ais_status_codes_types.ship_types
SELECT * FROM ais_status_codes_types.ship_types_detailed


SELECT * 
FROM ais_status_codes_types.mmsi_country_codes










