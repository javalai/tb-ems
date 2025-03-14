-- DROP PROCEDURE public.sp_update_energy_stat_day_sub(text);

CREATE OR REPLACE PROCEDURE public.sp_update_energy_stat_day_sub(IN p_device_id text)
 LANGUAGE plpgsql
AS $procedure$
    
DECLARE 

  ins_rows numeric;
  upd_rows numeric;

BEGIN
     
  RAISE NOTICE '開始處理 % 的日統計資料...', p_device_id;

  RAISE NOTICE '  先修正 % 的最後一筆資料的統計值...', p_device_id;    
  -- 通常最後一筆時統計資料，因為時間差的關係，有可能不完整，需要重新統計
  WITH stat_times AS (
    SELECT hdsd.device_id, hdsd.key_id,
      MAX(hdsd.stat_time) AS latest_stat_time
    FROM hd_device_statistics_daily hdsd
    JOIN key_dictionary kd ON kd.key_id = hdsd.key_id
    WHERE hdsd.device_type = 'E'
      AND kd."key" IN ('ConsumedEnergy', 'AVG_Active_Power', 'AVG_Reactive_Power',
                             'AVG_HZ', 'AVG_Voltage', 'AVG_Current', 'AVG_PF')
    GROUP BY hdsd.device_id, hdsd.key_id
  ),
  coeffs AS (
    SELECT hcp.param_id, hcp.param_name, hcp.float_value AS coeff, hcp.factory_id 
    FROM hd_config_param hcp 
  )
  UPDATE hd_device_statistics_daily AS hdsd1
    SET 
      dbl_stats = tb.dbl_stats,
      dbl_avg = tb.dbl_avg,
      dbl_min = tb.dbl_min,
      dbl_max = tb.dbl_max,
      long_stats = tb.long_stats, 
      long_avg = tb.long_avg,
      long_min = tb.long_min,
      long_max = tb.long_max,
      charge = tb.charge,
      peak_energy = tb.peak_energy,
      partial_peak_energy = tb.partial_peak_energy,
      off_peak_energy = tb.off_peak_energy,
      peak_charge = tb.peak_charge,
      partial_peak_charge = tb.partial_peak_charge,
      off_peak_charge = tb.off_peak_charge,
      kgco2e = tb.kgco2e
  FROM(
    SELECT 
      HD_DATE_TRUNC(hdsh.stat_time) AS truncated_stat_time,
      hdsh.entity_id,
      hdsh.device_id, 
      3 AS stat_type, -- 日統計
      hdsh.device_type,
      hdsh.key_id,
      CASE
        WHEN kd."key" = 'ConsumedEnergy' THEN SUM(hdsh.dbl_stats)
        ELSE AVG(hdsh.dbl_stats)
      END AS dbl_stats,
      AVG(hdsh.dbl_stats) AS dbl_avg,
      MIN(hdsh.dbl_stats) AS dbl_min,
      MAX(hdsh.dbl_stats) AS dbl_max,
      CASE
        WHEN kd."key" = 'ConsumedEnergy' THEN SUM(hdsh.long_stats)
        ELSE AVG(hdsh.long_stats)
      END AS long_stats,
      AVG(hdsh.long_stats) AS long_avg,
      MIN(hdsh.long_stats) AS long_min,
      MAX(hdsh.long_stats) AS long_max,
      SUM(CASE WHEN period_type='PE' AND kd."key" = 'ConsumedEnergy' THEN hdsh.dbl_stats ELSE 0 END) AS peak_energy,
      SUM(CASE WHEN period_type='PP' AND kd."key" = 'ConsumedEnergy' THEN hdsh.dbl_stats ELSE 0 END) AS partial_peak_energy,
      SUM(CASE WHEN period_type='OP' AND kd."key" = 'ConsumedEnergy' THEN hdsh.dbl_stats ELSE 0 END) AS off_peak_energy,
      CASE
        WHEN kd."key" = 'ConsumedEnergy' THEN SUM(hdsh.charge)
        ELSE NULL
      END AS charge,
      SUM(CASE WHEN period_type='PE' AND kd."key" = 'ConsumedEnergy' THEN hdsh.charge ELSE 0 END) AS peak_charge,
      SUM(CASE WHEN period_type='PP' AND kd."key" = 'ConsumedEnergy' THEN hdsh.charge ELSE 0 END) AS partial_peak_charge,
      SUM(CASE WHEN period_type='OP' AND kd."key" = 'ConsumedEnergy' THEN hdsh.charge ELSE 0 END) AS off_peak_charge,
      CASE
        WHEN kd."key" = 'ConsumedEnergy' THEN SUM(hdsh.dbl_stats)*ce.coeff
      END AS kgco2e
    FROM hd_device_statistics_hourly hdsh
    JOIN hd_device hd ON hd.device_id = hdsh.device_id
    JOIN coeffs ce ON ce.factory_id = hd.factory_id
    JOIN key_dictionary kd ON kd.key_id = hdsh.key_id
    LEFT JOIN stat_times st ON st.device_id = hdsh.device_id AND st.key_id = hdsh.key_id 
    WHERE hdsh.device_type = 'E'
      AND kd."key" IN ('ConsumedEnergy', 'AVG_Active_Power', 'AVG_Reactive_Power',
                        'AVG_HZ', 'AVG_Voltage', 'AVG_Current', 'AVG_PF')
      AND hdsh.device_id = p_device_id
      AND ce.param_name = '電力碳排係數'
      /* 日期判斷規則(假設時間為2024-12-10 09:00:00)；
        1. 09:00:00這時間必須在12/10沒錯
        2. 12/10 早上 07:59:59以前算在12/9
        3. 08:00:00以後算在12/10
      */
      AND hdsh.stat_time BETWEEN (st.latest_stat_time + INTERVAL '8 hours')
                             AND (st.latest_stat_time + INTERVAL '1 day 7 hours')
    GROUP BY 
      /* hdsh_stat_time,*/ truncated_stat_time, /*start_time, end_time,*/ hdsh.entity_id, hdsh.device_id, stat_type, hdsh.device_type, hdsh.key_id, kd."key", ce.coeff
    ORDER BY 
       /* hdsh_stat_time,*/ truncated_stat_time, /*start_time, end_time,*/ hdsh.entity_id, hdsh.device_id, stat_type, hdsh.device_type, hdsh.key_id
  ) tb
  WHERE hdsd1.device_type = tb.device_type
    AND hdsd1.key_id = tb.key_id
    AND hdsd1.device_id = tb.device_id
    AND hdsd1.stat_time = tb.truncated_stat_time
  ;
  GET DIAGNOSTICS upd_rows = ROW_COUNT;
  RAISE NOTICE '  修正 % 的最後一筆資料的統計值，共更新 % 筆。', p_device_id, upd_rows;

  RAISE NOTICE '  接下來計算 % 資料的統計值...', p_device_id; 
  -- 以下處理新的日統計資料
  INSERT INTO hd_device_statistics_daily (
    stat_time, entity_id, device_id, stat_type, device_type, key_id, 
    dbl_stats, dbl_avg, dbl_min, dbl_max, long_stats, long_avg, long_min, long_max,
    peak_energy, partial_peak_energy, off_peak_energy,
    charge, peak_charge, partial_peak_charge, off_peak_charge, kgco2e
  )
  WITH stat_times AS (
    SELECT hdsd.device_id, hdsd.key_id, MAX(stat_time) AS latest_stat_time
    FROM  hd_device_statistics_daily hdsd
    JOIN key_dictionary kd ON kd.key_id = hdsd.key_id
    WHERE hdsd.device_type ='E' 
      AND kd."key" IN ('ConsumedEnergy', 'AVG_Active_Power', 
                        'AVG_Reactive_Power', 'AVG_HZ', 'AVG_Voltage', 'AVG_Current', 'AVG_PF')
    GROUP BY hdsd.device_id, hdsd.key_id
  ),
  coeffs AS (
    SELECT hcp.param_id, hcp.param_name, hcp.float_value AS coeff, hcp.factory_id 
    FROM hd_config_param hcp 
  )
  SELECT 
    HD_DATE_TRUNC(hdsh.stat_time) AS truncated_stat_time,
    hdsh.entity_id,
    hdsh.device_id, 
    3 AS stat_type3, -- 日統計
    hdsh.device_type,
    hdsh.key_id,
    CASE
      WHEN kd."key" = 'ConsumedEnergy' THEN SUM(hdsh.dbl_stats)
      ELSE AVG(hdsh.dbl_stats) -- 除了耗電量，其他都是平均
    END AS dbl_stats,
    AVG(hdsh.dbl_stats) AS dbl_avg,
    MIN(hdsh.dbl_stats) AS dbl_min,
    MAX(hdsh.dbl_stats) AS dbl_max,
    CASE
      WHEN kd."key" = 'ConsumedEnergy' THEN SUM(hdsh.long_stats)
      ELSE AVG(hdsh.long_stats) -- 除了耗電量，其他都是平均
    END AS long_stats,
    AVG(hdsh.long_stats) AS long_avg,
    MIN(hdsh.long_stats) AS long_min,
    MAX(hdsh.long_stats) AS long_max,
    SUM(CASE WHEN period_type='PE' THEN hdsh.dbl_stats ELSE 0 END) AS peak_energy,
    SUM(CASE WHEN period_type='PP' THEN hdsh.dbl_stats ELSE 0 END) AS partial_peak_energy,
    SUM(CASE WHEN period_type='OP' THEN hdsh.dbl_stats ELSE 0 END) AS off_peak_energy,
    SUM(hdsh.charge) AS charge,
    SUM(CASE WHEN period_type='PE' THEN hdsh.charge ELSE 0 END) AS peak_charge,
    SUM(CASE WHEN period_type='PP' THEN hdsh.charge ELSE 0 END) AS partial_peak_charge,
    SUM(CASE WHEN period_type='OP' THEN hdsh.charge ELSE 0 END) AS off_peak_charge,
    CASE
      WHEN kd."key" = 'ConsumedEnergy' THEN SUM(hdsh.dbl_stats)*ce.coeff
    END AS kgco2e
  FROM hd_device hd
  JOIN hd_device_statistics_hourly hdsh ON hdsh.device_id = hd.device_id
  JOIN key_dictionary kd ON kd.key_id = hdsh.key_id
  JOIN coeffs ce ON ce.factory_id = hd.factory_id
  LEFT JOIN stat_times st ON st.device_id = hdsh.device_id AND st.key_id = hdsh.key_id
  WHERE hd.device_type ='E'
    AND kd."key" IN ('ConsumedEnergy', 'AVG_Active_Power', 
                      'AVG_Reactive_Power', 'AVG_HZ', 'AVG_Voltage', 'AVG_Current', 'AVG_PF')
    AND hd.device_id = p_device_id
    AND ce.param_name = '電力碳排係數' AND ce.factory_id = hd.factory_id
    /* 判斷規則(假設時間為2024-12-10 09:00:00)；
      1. 09:00:00這時間必須在12/10沒錯
      2. 12/10 早上 07:59:59以前算在12/9
      3. 08:00:00以後算在12/10
    */
    AND hdsh.stat_time > COALESCE (st.latest_stat_time + INTERVAL '1 day' + INTERVAL '8 hours', TO_TIMESTAMP('1911-01-01', 'YYYY-MM-DD'))
  GROUP BY 
    truncated_stat_time, /*hdsh.stat_time, latest_stat_time,*/ hdsh.entity_id, hdsh.device_id, stat_type3, hdsh.device_type, hdsh.key_id, kd."key", ce.coeff
  ORDER BY
    truncated_stat_time, hdsh.entity_id, hdsh.device_id, stat_type3, hdsh.device_type, hdsh.key_id
  ;

  GET DIAGNOSTICS ins_rows = ROW_COUNT;
  RAISE NOTICE '  新增 % 的統計資料，共新增 % 筆。', p_device_id, ins_rows;


END;
$procedure$
;
