/* ----------------QUERY 1------------------
	skopos einai na vroume posta vessels tou EU (me monadiko IRCS= callsign)
	simetexoun sta AIS static_ships, kai an gia auta exoume kapoia tranjactories
*/
WITH valid_fishing_vessels_static as (
	WITH valid_fishing_vessels as(
		/* STEP 1 get all distinct ircs 28676 VESSELS  */
		SELECT DISTINCT ON (ircs) id, cfr, vesselname, ircs
		FROM vesselregister.eu_fishingvessels
	) /*STEP 2 find all ocarences of valid fishing vessels in static_ships 
		Ola ta static ships simata pou exoume parei einai 37335
		Ta opoia proerxonte apo molis 53 diaforetika callsigns, episis exoume kai 53 diaforetika sourcemmsi
		ARA: paratiroume oti i sisxetisi ton fishing vessels sourcemmsi kai callsign einai 1-1
		ipologizoume kai enan counter gia na doume posa static stigmata pirame ana plio
	  */
	SELECT DISTINCT ON (VFS.IRCS) callsign ,sourcemmsi, shipname ,shiptype, eta, destination, vesselname, Counter.callsign_count
	FROM (ais_data.static_ships S INNER JOIN valid_fishing_vessels VFS on S.callsign = VFS.IRCS),
	(SELECT VFS2.IRCS , count(VFS2.IRCS) AS callsign_count
	   FROM ais_data.static_ships S INNER JOIN valid_fishing_vessels VFS2 on S.callsign = VFS2.IRCS
	   GROUP BY VFS2.IRCS )AS Counter
	WHERE COUNTER.IRCS = VFS.IRCS
)
/* 	STEP 3 
	FIND all fishingVessels that have dynamic data from dynami_ships.
	Paratiroume oti exoume sileksei sinolika 409192 apo ta parapano 53 
	plia pou ipologisame oti exoun dosei stigma san static
*/
SELECT *
FROM ais_data.dynamic_ships D, valid_fishing_vessels_static VFVS
where D.mmsi = VFVS.sourcemmsi

/* --------------QUERY 2 -------------------
	Skopos einai na vroume poia apo ta plia tou fishing vessels pou xrisimopoiounte 
	sta static ships einai kai sta anfr_vessel_list.
	kratame step 1 kai 2 apo to proigoumeno query pou mas epistrefoun ta 53 fishing ships pou
	exoun dosei static stigma
*/
WITH valid_fishing_vessels_static as (
	WITH valid_fishing_vessels as(
		/* STEP 1 get all distinct ircs 28676 VESSELS  */
		SELECT DISTINCT ON (ircs) id, cfr, vesselname, ircs
		FROM vesselregister.eu_fishingvessels
	) /*STEP 2 find all occurrences of valid fishing vessels in static_ships 
		37335 occurrences from 53 fishing vessels
		ipologizoume kai enan counter gia na doume posa static stigmata pirame ana plio
	  */
	SELECT DISTINCT ON (VFS.IRCS) callsign ,sourcemmsi, shipname ,shiptype, eta, destination, vesselname, Counter.callsign_count
	FROM (ais_data.static_ships S INNER JOIN valid_fishing_vessels VFS on S.callsign = VFS.IRCS),
	(SELECT VFS2.IRCS , count(VFS2.IRCS) AS callsign_count
	   FROM ais_data.static_ships S INNER JOIN valid_fishing_vessels VFS2 on S.callsign = VFS2.IRCS
	   GROUP BY VFS2.IRCS )AS Counter
	WHERE COUNTER.IRCS = VFS.IRCS
)/* STEP3
	Check how many of 53 valic ships with static occurrences exists in anfr_vessel_list
	The result is that all 53 fishing ships exists in anfr_vessel_list
	ARA! To kalo einai oti gia osa plia exoume static stigma einai kai sta anfr_vessel_list
  */ 
 SELECT *
 FROM vesselregister.anfr_vessel_list ANFR, valid_fishing_vessels_static VFVS
 WHERE ANFR.callsign = VFVS.callsign

	
/* --------------QUERY 3 -------------------
	Skopos einai na vroume poia apo ta plia tou fishing vessels iparxoun sto anfr_vessel_list
	einai perisotera apo ta 53 pou exoun dosei static stigma?
*/
WITH valid_fishing_vessels_static as (
	WITH valid_fishing_vessels as(
		/* STEP 1 get all distinct ircs 28676 VESSELS  */
		SELECT DISTINCT ON (ircs) id, cfr, vesselname, ircs
		FROM vesselregister.eu_fishingvessels
	) /*
		SETP 2 find all occurrences of valid fishing vessels in anfr_vessel_list
		Total fishing vessels in anfr_vessel_list 5562 and 4863 with not null mmsi
		but now we have to find out if the 53
	as we saw in query2 only 53 exists in static_ships
	*/
	SELECT DISTINCT ON (ANFR.id) ANFR.id, ship_name, mmsi, shiptype, callsign
	FROM vesselregister.anfr_vessel_list  ANFR, valid_fishing_vessels VFV
	WHERE ANFR.callsign = VFV.ircs /*AND ANFR.mmsi IS NOT NULL*/
)/* STEP 3 
	 now we have to find out if the 53 fishing vessels found at QUERY 2 Are the only 
	 fishing vessels in static_ships
	 SOS! vlepoume oti an psaksoume ston static_ships ta fishing ships pou vrikame
	 ston anfr_vessel_list (5562) pernoume 359 plia ta opoia exoun dosei stigma 
	 ENO AN! psaksoume me D.callsign = VFVS.callsign pernoume mono ta 53 pou peirame kai apo to query 2
	 EPISIS SOS AN TREKSOUME TO PARAKATO SELECT PATATIROUME OTI: ta 53 preriexonte mesa sta 359 pou vrikame
	 ara simperenoume oti oti pliroforia theloume sxetika me ta navigational data mporoume na tin paroume
	 apo ton anfr_vessel_list. ARA! ekremei sxediastiki apofasi an kratisoume kai ta fishing i oxi
*/
SELECT *
FROM (SELECT DISTINCT ON (VFVS.id) VFVS.id, VFVS.mmsi, D.callsign, VFVS.callsign
FROM ais_data.static_ships D, valid_fishing_vessels_static VFVS
where D.sourcemmsi = VFVS.mmsi /*OR D.callsign = VFVS.callsign*/ ) T1,
(SELECT DISTINCT ON (VFVS.id) VFVS.id, VFVS.mmsi, D.callsign, VFVS.callsign
FROM ais_data.static_ships D, valid_fishing_vessels_static VFVS
where D.callsign = VFVS.callsign /*OR D.callsign = VFVS.callsign*/ ) T2 
WHERE T1.id = T2.ID



SELECT DISTINCT ON (VFVS.id) VFVS.mmsi, D.callsign, VFVS.callsign
FROM ais_data.static_ships D, valid_fishing_vessels_static VFVS
where D.sourcemmsi = VFVS.mmsi /*OR D.callsign = VFVS.callsign*/ 
	
	
	
SELECT *
FROM (SELECT DISTINCT ON (VFVS.id) VFVS.id, VFVS.mmsi, D.callsign, VFVS.callsign
FROM ais_data.static_ships D, valid_fishing_vessels_static VFVS
where D.sourcemmsi = VFVS.mmsi /*OR D.callsign = VFVS.callsign*/ ) T1,
(SELECT DISTINCT ON (VFVS.id) VFVS.id, VFVS.mmsi, D.callsign, VFVS.callsign
FROM ais_data.static_ships D, valid_fishing_vessels_static VFVS
where D.callsign = VFVS.callsign /*OR D.callsign = VFVS.callsign*/ ) T2 
WHERE T1.id = T2.ID
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	