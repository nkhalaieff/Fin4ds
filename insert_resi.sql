DELIMITER $$

USE `bse_fin4ds`$$

DROP PROCEDURE IF EXISTS `insert_resi`$$

CREATE DEFINER=`nkhalaieff`@`%` PROCEDURE `insert_resi`()
BEGIN
DROP TABLE IF EXISTS AR;
CREATE TEMPORARY TABLE AR (
	acct_prod_id BIGINT PRIMARY KEY,
	enddate DATETIME
	)
	;
SELECT YEAR(DATE(NOW() + INTERVAL 6 MONTH)) * 100 + MONTH(DATE(NOW() + INTERVAL 6 MONTH)) INTO @Period;	
	
	
-- create temp table for insert hedge ratio into resi_contracts
CREATE TEMPORARY TABLE t AS 
(SELECT IR.acct_prod_id,
	MAX(HRR.date) d8,
	HX.state
FROM incoming_resi IR
JOIN hub_xref HX
	ON HX.LDC = IR.ldc
JOIN Hedge_id_resi HRR
	ON HRR.state = HX.state
WHERE HRR.date <= IR.date_provisioned
GROUP BY IR.acct_prod_id);
	
INSERT INTO AR
SELECT acct_prod_id,
	enddate
FROM resi_contracts
WHERE auto_renew = 1
	;
	
INSERT INTO resi_contracts
SELECT DISTINCT
	NULL,
	IR.acct_prod_id,
	IR.ldc,
	CASE WHEN ISNULL(C.campaign_id) = TRUE THEN -1 ELSE C.campaign_id END AS campaign_id,
	IR.campaign,
	IFNULL(IR.offer, ''),
	REPLACE(IR.account, '\'', ''),
	IR.startdate,
	CASE WHEN IR.enddate = '2050-01-01 00:00:00' THEN DATE(IR.startDate) + INTERVAL PERIOD_DIFF(@period, YEAR(IR.startDate) * 100 + MONTH(IR.startDate)) MONTH 
		ELSE IR.enddate END AS enddate,
	IR.dropdate,	
	CASE WHEN IR.enddate = '2050-01-01 00:00:00' THEN 1 ELSE 0 END AS auto_renew,
	IFNULL(IR.muni_agg, 'None') AS muni_agg,
	0 AS muni_agg_hold,
	NULL AS hu_id,
	NULL AS rate_code,
	NULL AS load_profile,
	NULL AS hu_date,
	IR.date_provisioned,
	NOW() AS date_added,
	NOW() AS date_modified,
	'2099-12-31 00:00:00' AS date_dropped,
	1 AS is_current,
	1 AS recalc,
	-- Added 6/21 for resi hedge strategy using hedge_ratio_resi table	
	ROUND(DATEDIFF(CASE WHEN IR.enddate = '2050-01-01 00:00:00' THEN DATE(IR.startDate) + INTERVAL PERIOD_DIFF(@period, YEAR(IR.startDate) * 100 + MONTH(IR.startDate)) MONTH 
				ELSE IR.enddate END
			, IR.startdate) / 30.5) term,
	t.HRR_ID
FROM incoming_resi IR
LEFT OUTER JOIN AR
	ON IR.acct_prod_id = AR.acct_prod_id
INNER JOIN hub_xref HX
	ON IR.ldc = HX.ldc
LEFT OUTER JOIN Campaigns C
	ON C.campaign = CASE WHEN IR.enddate = '2050-01-01 00:00:00' OR ISNULL(AR.acct_prod_id) = FALSE THEN 'FPAI-V' ELSE IR.campaign END
	AND IFNULL(C.offer, '') = CASE WHEN IR.enddate = '2050-01-01 00:00:00' OR ISNULL(AR.acct_prod_id) = FALSE THEN 'FPAI-V' ELSE IFNULL(IR.offer, '') END
	AND C.zone = HX.load_zon
LEFT JOIN t
	ON IR.acct_prod_id = t.acct_prod_id
WHERE IR.complete = 0
ON DUPLICATE KEY UPDATE
	resi_contracts.ldc = IR.ldc,
	resi_contracts.campaign_id = CASE WHEN ISNULL(C.campaign_id) = TRUE THEN -1 ELSE C.campaign_id END,
	resi_contracts.campaign = IR.campaign,
	resi_contracts.offer = IFNULL(IR.offer, ''),
	resi_contracts.startdate = IR.startdate,
	resi_contracts.enddate = CASE WHEN IR.enddate = '2050-01-01 00:00:00' THEN DATE(IR.startDate) + INTERVAL PERIOD_DIFF(@period, YEAR(IR.startDate) * 100 + MONTH(IR.startDate)) MONTH
					WHEN ISNULL(AR.acct_prod_id) = FALSE THEN AR.enddate
					ELSE IR.enddate END,
	resi_contracts.dropdate = IR.dropdate,	
	resi_contracts.date_modified = NOW(),
	resi_contracts.account = REPLACE(IR.account, '\'', ''),
	resi_contracts.HRR_ID = t.HRR_ID 
	;
	
UPDATE resi_contracts RC
INNER JOIN campaigns C
	ON RC.campaign = C.campaign
	AND RC.offer = C.offer
SET RC.campaign_id = C.campaign_id
WHERE RC.campaign_id = -1
	;
	
	
DROP TABLE IF EXISTS RT;
CREATE TEMPORARY TABLE RT AS
SELECT RC.Account, 
	RC.offer,
	LC.green_perc, 
	RC.startdate, 
	MAX(LEAST(RC.enddate,RC.dropdate)) green_end,
	C.campaign
FROM Resi_contracts RC
INNER JOIN campaigns C 
	ON RC.campaign_id = C.campaign_id
INNER JOIN live_contracts LC 
	ON C.offercode = LC.offercode
WHERE LC.green_perc > 0 
	AND RC.auto_renew = 0
GROUP BY RC.account;
ALTER TABLE RT ADD INDEX idx1(account);
DROP TABLE IF EXISTS RT1;
CREATE TEMPORARY TABLE RT1 AS
SELECT RC.Account, 
	RC.offer, 
	LC.green_perc, 
	RC.startdate, 
	RT.green_end,
	MAX(LEAST(RC.enddate,RC.dropdate)) brown_end,
	C.campaign
FROM RT
LEFT JOIN Resi_contracts RC
	ON RC.account = RT.account
	AND RC.auto_renew = 0
INNER JOIN campaigns C 
	ON RC.campaign_id = C.campaign_id
INNER JOIN live_contracts LC 
	ON C.offercode = LC.offercode
	AND LC.green_perc = 0	
GROUP BY RT.account
HAVING green_end > IFNULL(brown_end,0);
ALTER TABLE RT1 ADD INDEX idx1(account);
	
UPDATE resi_contracts RC
INNER JOIN RT1
	ON RC.account = RT1.account
	AND RC.startdate >= RT1.green_end
	AND RC.auto_renew = 1
	AND RC.startdate!= RC.enddate 
INNER JOIN hub_xref HX
	ON RC.ldc = HX.ldc
INNER JOIN (
	SELECT C.campaign_id,
		HX.load_zone
	FROM campaigns C
	INNER JOIN live_contracts LC
		ON C.offercode = LC.offercode
		AND LC.green_perc > 0
	INNER JOIN hub_xref HX
		ON LC.ldc = HX.ldc
	INNER JOIN prod_cntrl PC
		ON HX.rto = PC.rto
		AND LC.product = PC.product
		AND PC.e_type = 5
	GROUP BY HX.load_zone
	) A
	ON HX.load_zone = A.load_zone
SET RC.campaign_id = A.campaign_id
;
INSERT INTO `bse_ida`.`resi_accts` (Acct_No, LDC_ID)
SELECT fin.Acct_No,hx.LDC_ID
FROM
                (SELECT DISTINCT ldc, Account Acct_No
                FROM `bse_fin4ds`.`incoming_resi`
                WHERE DATE(enddate) > DATE(NOW())) fin
JOIN `bse_ida`.`hub_xref`  hx
                ON fin.LDC = hx.LDC
ON DUPLICATE KEY UPDATE LDC_ID = hx.LDC_ID
;
COMMIT;
    END$$

DELIMITER ;