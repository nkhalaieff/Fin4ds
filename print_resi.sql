DELIMITER $$

USE `bse_fin4ds`$$

DROP PROCEDURE IF EXISTS `print_resi`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `print_resi`()
BEGIN
DECLARE tbl_name VARCHAR(255);
/*Clear out the old tables*/
TRUNCATE TABLE resi_usage_summary;
TRUNCATE TABLE dropped_offercodes;
	
/*Backup the Existing Usage Table*/
SET @tbl_name = CONCAT('usg_fcast_', DATE_FORMAT(NOW(), '%Y%m%d%h%m%s'));
SET @sql_str1 = CONCAT('CREATE TABLE ', @tbl_name, ' LIKE usg_fcast;');
SET @sql_str2 = CONCAT('INSERT INTO ', @tbl_name, ' SELECT * FROM usg_fcast;');
PREPARE stmt1 FROM @sql_str1;
EXECUTE stmt1;
PREPARE stmt2 FROM @sql_str2;
EXECUTE stmt2;
/* 2014-05-08: Added Dynamic Drop statment for usg_fcast backups */
SELECT CONCAT(S.sqlString, 
	GROUP_CONCAT(S.TABLE_NAME),
	';')
FROM (
	SELECT R.sqlString,
		R.TABLE_NAME,
		CAST(LEFT(R.DATETIMESTAMP, 6) AS SIGNED) AS PERIOD,
		YEAR(NOW()) * 100 + MONTH(NOW()) AS PERIODNOW,
		PERIOD_DIFF(YEAR(NOW()) * 100 + MONTH(NOW()), CAST(LEFT(R.DATETIMESTAMP, 6) AS SIGNED)) AS periodDiff
	FROM (
		SELECT 'DROP TABLE IF EXISTS ' AS sqlString,
			T.TABLE_NAME,
			RIGHT(T.TABLE_NAME, LENGTH(T.TABLE_NAME) - LENGTH(SUBSTRING_INDEX(T.TABLE_NAME, '_', 2)) - 1) AS DATETIMESTAMP
		FROM INFORMATION_SCHEMA.TABLES T
		WHERE T.TABLE_NAME REGEXP 'usg_fcast_[0-9]'
		) AS R	
	WHERE PERIOD_DIFF(YEAR(NOW()) * 100 + MONTH(NOW()), CAST(LEFT(R.DATETIMESTAMP, 6) AS SIGNED)) > 3
	) S
GROUP BY S.sqlString
INTO @sql_str3
	;
	
IF @sql_str3 IS NOT NULL THEN
	PREPARE stmt3 FROM @sql_str3;
	EXECUTE stmt3;
END IF;
	
/*  UPDATE All Contracts Associated to recalc = 1 */
DROP TABLE IF EXISTS 
	campaign_update
	;
CREATE TEMPORARY TABLE campaign_update AS
SELECT DISTINCT
	campaign_id
FROM resi_contracts
WHERE recalc = 1
	AND campaign_id != -1
	;
	
UPDATE resi_contracts
SET recalc = 1
WHERE campaign_id IN (
	SELECT campaign_id
	FROM campaign_update)
	;
COMMIT;
/*Build the summary table*/
INSERT INTO resi_usage_summary
SELECT	NULL,
	LC.offercode,
	LC.ldc,
	CONCAT(C.campaign, ROUND(NOW(), 0)) account, 
	'Residential',
	RC.rate_code,
	LC.product,
	'2012-07-01',
	'2099-12-31',
	mon_cal.yr, 
	mon_cal.mo, 
	SUM(kwh * CASE 
			WHEN LEAST(RC.enddate, RC.dropdate) <= RC.startdate THEN 0
			WHEN mon_cal.yr + mon_cal.mo/100 = YEAR(RC.startdate) + MONTH(RC.startdate)/100 THEN s_pr
			WHEN mon_cal.yr + mon_cal.mo/100 = LEAST(YEAR(RC.dropdate) + MONTH(RC.dropdate)/100, YEAR(RC.enddate) + MONTH(RC.enddate)/100) THEN e_pr
		 ELSE 1 END) AS kwh,
	SUM(on_kwh * CASE 
			WHEN LEAST(RC.enddate, RC.dropdate) <= RC.startdate THEN 0
			WHEN mon_cal.yr + mon_cal.mo/100 = YEAR(RC.startdate) + MONTH(RC.startdate)/100 THEN s_pr
			WHEN mon_cal.yr + mon_cal.mo/100 = LEAST(YEAR(RC.dropdate) + MONTH(RC.dropdate)/100, YEAR(RC.enddate) + MONTH(RC.enddate)/100) THEN e_pr
		 ELSE 1 END) AS on_kwh,
	SUM(off_kwh * CASE 
			WHEN LEAST(RC.enddate, RC.dropdate) <= RC.startdate THEN 0
			WHEN mon_cal.yr + mon_cal.mo/100 = YEAR(RC.startdate) + MONTH(RC.startdate)/100 THEN s_pr
			WHEN mon_cal.yr + mon_cal.mo/100 = LEAST(YEAR(RC.dropdate) + MONTH(RC.dropdate)/100, YEAR(RC.enddate) + MONTH(RC.enddate)/100) THEN e_pr
		 ELSE 1 END) AS off_kwh,
	SUM(peak_demand * CASE 
			WHEN LEAST(RC.enddate, RC.dropdate) <= RC.startdate THEN 0
			WHEN mon_cal.yr + mon_cal.mo/100 = YEAR(RC.startdate) + MONTH(RC.startdate)/100 THEN s_pr
			WHEN mon_cal.yr + mon_cal.mo/100 = LEAST(YEAR(RC.dropdate) + MONTH(RC.dropdate)/100, YEAR(RC.enddate) + MONTH(RC.enddate)/100) THEN e_pr
		 ELSE 1 END) AS peak_demand,
	SUM(net_plc * CASE 
			WHEN LEAST(RC.enddate, RC.dropdate) <= RC.startdate THEN 0
			WHEN mon_cal.yr + mon_cal.mo/100 = YEAR(RC.startdate) + MONTH(RC.startdate)/100 THEN s_pr
			WHEN mon_cal.yr + mon_cal.mo/100 = LEAST(YEAR(RC.dropdate) + MONTH(RC.dropdate)/100, YEAR(RC.enddate) + MONTH(RC.enddate)/100) THEN e_pr
		 ELSE 1 END) AS net_plc,
	SUM(cap_plc * CASE 
			WHEN LEAST(RC.enddate, RC.dropdate) <= RC.startdate THEN 0
			WHEN mon_cal.yr + mon_cal.mo/100 = YEAR(RC.startdate) + MONTH(RC.startdate)/100 THEN s_pr
			WHEN mon_cal.yr + mon_cal.mo/100 = LEAST(YEAR(RC.dropdate) + MONTH(RC.dropdate)/100, YEAR(RC.enddate) + MONTH(RC.enddate)/100) THEN e_pr
		 ELSE 1 END) AS cap_plc,
	RC.dropdate,
	C.campaign
FROM resi_contracts RC
INNER JOIN mon_cal 
	ON mon_cal.yr + mon_cal.mo/100 BETWEEN YEAR(RC.startdate) + MONTH(RC.startdate)/100 
		AND LEAST(YEAR(RC.dropdate) + MONTH(RC.dropdate)/100, YEAR(RC.enddate) + MONTH(RC.enddate)/100)
INNER JOIN resi_usage 
	ON RC.row_id = resi_usage.row_id 
	AND resi_usage.mo = mon_cal.mo
INNER JOIN campaigns C 
	ON C.campaign_id = RC.campaign_id
	-- only insert contracts with no hold
	AND C.muni_agg_hold = 0
INNER JOIN live_contracts LC
	ON C.offercode = LC.offercode
WHERE RC.recalc = 1
GROUP BY LC.offercode, 
	C.campaign, 
	mon_cal.yr, 
	mon_cal.mo
ORDER BY LC.offercode, 
	C.campaign, 
	mon_cal.yr, 
	mon_cal.mo
	;
/*  	FPAI-V Smoothing
	Added: 2014-04-28
	
*/
UPDATE resi_usage_summary RUS,
	hub_xref HX,
	prod_cntrl PC,
	hedge_ratio_fpaiv HRF
SET 
	RUS.tot_usg = RUS.tot_usg * HRF.scale,
	RUS.on_pk_usg = RUS.on_pk_usg * HRF.scale,
	RUS.off_pk_usg = RUS.off_pk_usg * HRF.scale,
	RUS.pk_demand = RUS.pk_demand * HRF.scale,
	RUS.net_plc = RUS.net_plc * HRF.scale,
	RUS.cap_plc = RUS.cap_plc * HRF.scale
WHERE RUS.ldc = HX.ldc
	AND HX.rto = PC.rto
	AND RUS.product = PC.product
	AND HX.state = HRF.State
	AND PC.e_type = 5
	AND CASE WHEN PERIOD_DIFF(RUS.yr * 100 + RUS.mo, YEAR(NOW()) * 100 + MONTH(NOW())) BETWEEN 0 AND 6 
			THEN PERIOD_DIFF(RUS.yr * 100 + RUS.mo, YEAR(NOW()) * 100 + MONTH(NOW())) = HRF.Period
		WHEN PERIOD_DIFF(RUS.yr * 100 + RUS.mo, YEAR(NOW()) * 100 + MONTH(NOW())) > 6
			THEN -1 = HRF.Period
		ELSE 0 = 1
	END
	;
/*Flag for repricing*/
UPDATE live_contracts LC
JOIN resi_usage_summary RUS
	ON LC.offercode = RUS.offercode
SET LC.date_modified = NOW(),
	LC.reprice = 1
	;
/*Drop the offercode*/
INSERT INTO dropped_offercodes
SELECT offercode, '2099-12-31'
FROM resi_usage_summary
GROUP BY offercode;
	
CALL kill_offercodes(2);
/*Migrate the new usage*/
INSERT INTO usg_fcast
	SELECT 
		LC.fin4ds_id,
		NULL, 
		RUS.offercode, 
		RUS.ldc, 
		RUS.account, 
		RUS.acct_name,
		RUS.rate_code, 
		RUS.product, 
		'2012-06-30', 
		'2099-12-31', 
		0.70 * RUS.tot_usg / (24 * RUS.pk_demand * CASE
			WHEN RUS.mo IN (4, 6, 9, 11) THEN 30
			WHEN RUS.mo = 2 THEN IF(RUS.yr % 4 = 0, 29, 28)
			ELSE 31 END), 
		RUS.yr, 
		RUS.mo,
		RUS.yr * 100 + RUS.mo period,
		RUS.tot_usg,  
		RUS.on_pk_usg,
		RUS.off_pk_usg, 
		RUS.pk_demand, 
		RUS.net_plc, 
		RUS.cap_plc, 
		'2099-12-31', 
		NULL, 
		NULL, 
		NULL, 
		NULL, 
		NULL, 
		NULL,
		RUS.campaign_code,
		NULL
FROM resi_usage_summary RUS
JOIN live_contracts LC
	ON RUS.offercode = LC.offercode
ON DUPLICATE KEY UPDATE
	usg_fcast.fin4ds_id = LC.fin4ds_id,
	usg_fcast.ldc = RUS.ldc,
	usg_fcast.account  = RUS.account,
	usg_fcast.acct_name = RUS.acct_name,
	usg_fcast.rate_code  = RUS.rate_code,
	usg_fcast.product  = RUS.product,
	usg_fcast.startdate  = '2012-06-30',
	usg_fcast.enddate  = '2099-12-31',
	usg_fcast.load_factor  = 0.70 * RUS.tot_usg / (24 * RUS.pk_demand * CASE
			WHEN usg_fcast.mo IN (4, 6, 9, 11) THEN 30
			WHEN usg_fcast.mo = 2 THEN IF(usg_fcast.yr % 4 = 0, 29, 28)
			ELSE 31 END),
	usg_fcast.period = RUS.yr * 100 + RUS.mo,
	usg_fcast.tot_usg  = RUS.tot_usg,
	usg_fcast.on_pk_usg  = RUS.on_pk_usg,
	usg_fcast.off_pk_usg  = RUS.off_pk_usg,
	usg_fcast.pk_demand  = RUS.pk_demand,
	usg_fcast.net_plc  = RUS.net_plc,
	usg_fcast.cap_plc = RUS.cap_plc;
	
	
/*Flag as complete*/
UPDATE resi_contracts 
SET recalc = 0;
COMMIT;
		
END$$

DELIMITER ;