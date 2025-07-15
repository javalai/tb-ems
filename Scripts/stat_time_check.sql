WITH t1 AS (
  SELECT hdsd.device_id, hdsd.device_type, hdsd.key_id, MAX(hdsd.stat_time) AS max_stat_time 
  FROM hq.hd_device_statistics_daily hdsd 
  -- WHERE hdsd.device_id = 'P5C_3' AND hdsd.device_type='E' AND hdsd.key_id='148'
  GROUP BY hdsd.device_id, hdsd.device_type, hdsd.key_id
  ORDER BY hdsd.device_id)
SELECT l.device_id, l.device_type, l.key_id, l.latest_stat_time, t1.max_stat_time
FROM hq.hd_device_stat_day_latest l
JOIN t1 ON t1.device_id=l.device_id AND t1.device_type=l.device_type AND t1.key_id=l.key_id
WHERE l.latest_stat_time <> t1.max_stat_time
;

