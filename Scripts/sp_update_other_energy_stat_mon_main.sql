-- DROP PROCEDURE public.sp_update_other_energy_stat_mon_main();

-- 非電能源的月統計程式

CREATE OR REPLACE PROCEDURE public.sp_update_other_energy_stat_mon_main()
 LANGUAGE plpgsql
AS $procedure$

  DECLARE 
    v_device_cursor CURSOR FOR
      SELECT hd.device_id, hd.entity_id 
      FROM hd_device hd
      WHERE hd.entity_id IS NOT NULL 
        AND hd.device_type IN ('W', 'A', 'G', 'S')
      ;
    v_device_record RECORD;
    v_device_id varchar(10);

    affected_rows numeric;
  BEGIN
  
  -- 開啟游標
  OPEN v_device_cursor;
  
  -- 依序處理
  LOOP
    FETCH NEXT FROM v_device_cursor INTO v_device_record;
    EXIT WHEN NOT FOUND;
    v_device_id = v_device_record.device_id;
   
    RAISE NOTICE '開始處理非電能源 % 的月統計資料...', v_device_id;

    -- 以下處理新的月統計資料
    INSERT INTO hd_device_statistics_monthly(stat_time, device_id, stat_type, device_type, key_id, long_stats, dbl_stats, kgco2e)
    WITH epochs AS (
      SELECT hdsm.device_id, hdsm.key_id,
        MAX(hdsm.stat_time) + INTERVAL '1 month' - INTERVAL '1 second' AS latest_stat_time
      FROM hd_device_statistics_monthly hdsm
      JOIN key_dictionary kd ON kd.key_id = hdsm.key_id
      WHERE hdsm.device_type IN ('W', 'A', 'G', 'S')
        AND kd."key" IN (
          'water_consumed_volume', 'water_flow_rate',
          'air_consumed_volume', 'air_flow_rate',
          'gas_consumed_volume', 'gas_flow_rate',
          'steam_consumed_volume', 'steam_flow_rate'
        )
      GROUP BY hdsm.device_id, hdsm.key_id
    )
    SELECT 
      DATE_TRUNC('month', hdsd.stat_time) AS truncated_stat_time,
      hdsd.device_id, 
      4 AS stat_type, -- 月統計
      hdsd.device_type,
      hdsd.key_id,
      CASE 
        WHEN kd."key" IN ('water_flow_rate', 'gas_flow_rate', 'air_flow_rate', 'steam_flow_rate') 
          THEN AVG(hdsd.long_stats) -- 流量應該是取平均
        WHEN kd."key" IN ('water_consumed_volume', 'gas_consumed_volume', 'air_consumed_volume', 'steam_consumed_volume')
          THEN SUM(hdsd.long_stats) -- 耗用量應該是取加總
        ELSE NULL
      END AS long_stats,
      COALESCE(CASE 
        WHEN kd."key" IN ('water_flow_rate', 'gas_flow_rate', 'air_flow_rate', 'steam_flow_rate') 
          THEN AVG(hdsd.dbl_stats) -- 流量應該是浮點數取平均
        WHEN kd."key" IN ('water_consumed_volume', 'gas_consumed_volume', 'air_consumed_volume', 'steam_consumed_volume')
          THEN SUM(hdsd.dbl_stats) -- 耗用量應該是浮點數取加總
        ELSE NULL
      END, 0) AS dbl_stats,
      SUM(hdsd.kgco2e) AS kgco2e
    FROM hd_device_statistics_daily hdsd
    JOIN key_dictionary kd ON kd.key_id = hdsd.key_id
    LEFT JOIN epochs ep ON ep.device_id = hdsd.device_id AND ep.key_id = hdsd.key_id 
    WHERE hdsd.device_type IN ('W', 'A', 'G', 'S')
      AND kd."key" IN (
        'water_consumed_volume', 'water_flow_rate',
        'air_consumed_volume', 'air_flow_rate',
        'gas_consumed_volume', 'gas_flow_rate',
        'steam_consumed_volume', 'steam_flow_rate'
      )
      AND hdsd.device_id = v_device_id
      AND hdsd.stat_time >= COALESCE(ep.latest_stat_time, '1911-01-01')
    GROUP BY 
      truncated_stat_time, hdsd.device_id, stat_type, hdsd.device_type, hdsd.key_id, kd."key"
    ORDER BY 
      truncated_stat_time, hdsd.device_id, stat_type, hdsd.device_type, hdsd.key_id
    ON CONFLICT(stat_time, device_id, device_type, key_id) DO UPDATE SET
      long_stats = EXCLUDED.long_stats,
      dbl_stats = EXCLUDED.dbl_stats,
      kgco2e = EXCLUDED.kgco2e
    ;
    GET DIAGNOSTICS affected_rows = ROW_COUNT;
    RAISE NOTICE '新增或更新 % 的月統計資料，共異動 % 筆。', v_device_id, affected_rows;

  END LOOP;

  -- 關閉游標
  CLOSE v_device_cursor;

  END;
$procedure$
;
