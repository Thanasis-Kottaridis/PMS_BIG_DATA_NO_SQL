/* fetch navigational dynamic data with detaild navigational status*/
SELECT mmsi, lat, lon, ts, id_status, definition, turn, speed, course, heading, geom
FROM ais_data.dynamic_ships D INNER JOIN ais_status_codes_types.navigational_status S
ON D.status = S.id_status
ORDER BY mmsi 
LIMIT 100

SELECT * FROM ais_status_codes_types.navigational_status


SELECT COUNT(mmsi = null) FROM ais_data.dynamic_ships /*5505 kati tetoio KAI OLA EXOUN MMSI*/
SELECT COUNT(DISTINCT sourcemmsi) FROM ais_data.static_ships /*4842*/

/*ship metadata for 4842*/
SELECT DISTINCT ON (sourcemmsi) sourcemmsi, imo, callsign, shipname, shiptype
		FROM  ais_data.static_ships

 
/*find all common mmsi from dynamics and static
	we have 3774 mutual mmsi opote mporoume na exoume ship metadata fia auta ta plia
*/
with static_metadata as (
	SELECT DISTINCT ON (sourcemmsi) sourcemmsi, imo, callsign, shipname, shiptype
		FROM  ais_data.static_ships
) select mmsi, source
from (select distinct mmsi from ais_data.dynamic_ships ) as MMSI inner join static_metadata meta on MMSI.mmsi = meta.sourcemmsi











