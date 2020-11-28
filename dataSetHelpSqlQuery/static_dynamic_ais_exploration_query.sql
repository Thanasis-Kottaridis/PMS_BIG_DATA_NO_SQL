/* ------------MERIKA ANAGNORISTIKA QUERY GIA TA DATA-----------
	1) POSES EGGRAFES EXOUME SINOLIKA-> 1.078.617
	2) poses eggrafes exoun null mmsi -> 0 (AUTO EINAI POLI KALO)
	2) POSA DIAFORETIKA PLIA EXOUME -> 4842
	3) POSA APO AUTA TA PLIA IPARXOUME MESA STON ANFR (just in case) 697
*/
SELECT count(DISTINCT T.sourcemmsi)
FROM ais_data.static_ships T, vesselregister.anfr_vessel_list VL
WHERE T.sourcemmsi = VL.mmsi


/*-----------MERIKA ANAGNORISTIKA QUERY------------
	1) POSES EGGRAFES EXEI O anfr_vessel_list -> 180817
	2) POSA PLIA ME MMSI EXW SINOLIKA -> 82065 OLA DISTINCT
	3) GIA POSA APO AUTA EXW STATIC STIGMA -> 697
	4) GIA POSA PLIA EXW IMO_NUMBER -> 1379, 1150 me not null mmsi
*/
with imo_static_ships as(
	WITH imo_ships as (
		SELECT mmsi, cast(imo_number as int)
		FROM vesselregister.anfr_vessel_list VL
		WHERE imo_number is not null AND mmsi is not null
	)/* Pame na doume posa apo auta iparxoun sta static navigational -> 51 */
	SELECT DISTINCT T.sourcemmsi
	FROM ais_data.static_ships T, imo_ships IMO
	WHERE T.imo = IMO.imo_number
)
/*vlepoume lipon oti ta 45 apo ta 51 pou mporoume na vroume me imo apo ton static data 
mporoume na ta vroume me to mmsi ara lipon iparxoun 6 plia gia ta opoia exw stigma kai 
mporw na vrw details gia auta mono me to IMO*/
SELECT COUNT(*)
FROM imo_static_ships IMO, (SELECT DISTINCT T.sourcemmsi
FROM ais_data.static_ships T, vesselregister.anfr_vessel_list VL
WHERE T.sourcemmsi = VL.mmsi) AS T
where T.sourcemmsi = imo.sourcemmsi


/* --------------MERIKA ANAGNORISTIKA QUERY GIA DYNAMIC SHIPS--------------------
1) POSES EGGRAFES EXW 19.035.630
2) exw 5055 disnct mmsi apo ta opoia: mono ta 860 iparxoun sto ANFR
2) ti pliroforia antlw apo to status
*/
SELECT mmsi, max(to_timestamp(cast(ts  as bigint))::date), min(to_timestamp(cast(ts  as bigint))::date), count(mmsi)
FROM ais_data.dynamic_ships T
group by mmsi


select 



/*-------------- PROSPATHIA SISXETISMOU STATIC KAI DYNAMIC SHIP ------------
	1) PREPEI NA PROSPATHISW NA VRW TROPO NA SIXETISW TA STATIC KAI TA DYNAMIC SHIPS
	GNWRIZW OTI: mporw na vrisxw dynamic dedomena gia ena plio pou exei dosei static sima apo to mmsi
*/





















