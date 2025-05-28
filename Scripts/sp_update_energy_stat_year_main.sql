-- DROP PROCEDURE public.sp_update_energy_stat_year_main();

CREATE OR REPLACE PROCEDURE public.sp_update_energy_stat_year_main()
 LANGUAGE plpgsql
AS $procedure$

  DECLARE 
    v_device_cursor CURSOR FOR
      SELECT hd.device_id
      FROM hd_device hd
      WHERE hd.entity_id IS NOT NULL 
        AND hd.device_type = 'E'
      ORDER BY hd.device_id
      ;
    v_device_record RECORD;
    v_device_id VARCHAR(50);

    affected_rows NUMERIC;

  BEGIN

    -- 開啟游標
    OPEN v_device_cursor;

    -- 依序處理
    LOOP
      FETCH NEXT FROM v_device_cursor INTO v_device_record;
      EXIT WHEN NOT FOUND;
      v_device_id = v_device_record.device_id;
     
      RAISE NOTICE '開始處理 % 的年統計資料...', v_device_id;
      INSERT INTO hd_device_statistics_yearly (stat_time, device_id, stat_type, device_type, key_id, dbl_stats, 
                    long_stats, peak_energy, partial_peak_energy, off_peak_energy,
                    charge, peak_charge, partial_peak_charge, off_peak_charge, kgco2e)
      WITH epochs AS (
        SELECT hd.device_id, hdk.key_id,
          MAX(hdsy.stat_time) AS latest_stat_time
        FROM hd_device hd
        -- JOIN key_dictionary kd ON kd.key_id = hdsy.key_id
        JOIN hd_device_keys hdk  ON hdk.device_type = hd.device_type
        LEFT JOIN hd_device_statistics_yearly hdsy ON hdsy.device_id = hd.device_id      
        WHERE hdsy.device_type = 'E'
--          AND kd."key" IN ('ConsumedEnergy', 'AVG_Active_Power', 'AVG_Reactive_Power',
--                                 'AVG_HZ', 'AVG_Voltage', 'AVG_Current', 'AVG_PF')
        GROUP BY hd.device_id, hdk.key_id
      )
      SELECT 
        DATE_TRUNC('year', hdsm.stat_time) AS truncated_stat_time,
        hdsm.device_id, 
        5 AS stat_type5, --年統計
        hdsm.device_type,
        hdsm.key_id,
        CASE
          WHEN hdk.key_type = 'C' THEN SUM(hdsm.dbl_stats)
          ELSE AVG(hdsm.dbl_stats) -- 除了耗電量，其他都是平均
        END AS dbl_stats,
        CASE
          WHEN hdk.key_type = 'C' THEN SUM(hdsm.long_stats)
          ELSE AVG(hdsm.long_stats) -- 除了耗電量，其他都是平均
        END AS long_stats,
        SUM(hdsm.peak_energy) AS peak_energy,
        SUM(hdsm.partial_peak_energy) AS partial_peak_energy,
        SUM(hdsm.off_peak_energy) AS off_peak_energy,
        SUM(hdsm.charge) AS charge,
        SUM(hdsm.peak_charge) AS peak_charge,
        SUM(hdsm.partial_peak_charge) AS partial_peak_charge,
        SUM(hdsm.off_peak_charge) AS off_peak_charge,
        SUM(hdsm.kgco2e) AS kgco2e
      FROM hd_device_statistics_monthly hdsm
      JOIN hd_device_keys hdk  ON hdk.device_type = hdsm.device_type
      -- JOIN key_dictionary kd ON kd.key_id = hdsm.key_id
      LEFT JOIN epochs ep ON ep.device_id = hdsm.device_id AND ep.key_id = hdsm.key_id 
      WHERE hdsm.device_type ='E'
        AND hdsm.key_id = hdk.key_id
--        AND kd."key" IN ('ConsumedEnergy', 'AVG_Active_Power', 
--                        'AVG_Reactive_Power', 'AVG_HZ', 'AVG_Voltage', 'AVG_Current', 'AVG_PF')
        AND hdsm.device_id = v_device_id
        AND hdsm.stat_time > COALESCE (ep.latest_stat_time + INTERVAL '1 year' - INTERVAL '1 second', TO_TIMESTAMP('1911-01-01', 'YYYY-MM-DD'))
      GROUP BY 
        truncated_stat_time, hdsm.device_id, stat_type5, hdsm.device_type, hdsm.key_id, hdk."key_type"
      ORDER BY 
        truncated_stat_time, hdsm.device_id, stat_type5, hdsm.device_type, hdsm.key_id
      ON CONFLICT(stat_time, device_id, device_type, key_id) DO UPDATE SET
          long_stats = EXCLUDED.long_stats,
          dbl_stats = EXCLUDED.dbl_stats,
          peak_energy = EXCLUDED.peak_energy,
          partial_peak_energy = EXCLUDED.partial_peak_energy,
          off_peak_energy = EXCLUDED.off_peak_energy,
          charge = EXCLUDED.charge,
          peak_charge = EXCLUDED.peak_charge,
          partial_peak_charge = EXCLUDED.partial_peak_charge,
          off_peak_charge = EXCLUDED.off_peak_charge,
          kgco2e = EXCLUDED.kgco2e
      ;
  
      GET DIAGNOSTICS affected_rows = ROW_COUNT;
      RAISE NOTICE '  新增 % 的年統計資料，共新增 % 筆。', v_device_id, affected_rows;

      END LOOP;

      -- 關閉游標
      CLOSE v_device_cursor;  

      -- 捕捉異常並記錄錯誤，不中斷主迴圈
      RAISE NOTICE '所有電表的年統計處理完成。';
  
  END;
$procedure$
;
