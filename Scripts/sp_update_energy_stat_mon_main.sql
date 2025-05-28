-- DROP PROCEDURE public.sp_update_energy_stat_mon_main();

CREATE OR REPLACE PROCEDURE public.sp_update_energy_stat_mon_main()
 LANGUAGE plpgsql
AS $procedure$

  DECLARE 
    v_device_cursor CURSOR FOR
      SELECT hd.device_id
      FROM hd_device hd
      WHERE hd.entity_id IS NOT NULL 
        AND hd.device_type = 'E'
        AND hd.factory_id NOT IN ('KC','KT')
      ORDER BY hd.device_id
      ;
    v_device_record RECORD;
    v_device_id varchar(50);

    ins_rows numeric;
    upd_rows numeric;
  BEGIN

    -- 開啟游標
    OPEN v_device_cursor;
  
    -- 依序處理
    LOOP
      FETCH NEXT FROM v_device_cursor INTO v_device_record;
      EXIT WHEN NOT FOUND;
      v_device_id = v_device_record.device_id;
     
      RAISE NOTICE '開始處理 % 的月統計資料...', v_device_id;

      -- 以下處理新的月統計資料
      INSERT INTO hd_device_statistics_monthly (stat_time, device_id, stat_type, device_type, key_id, dbl_stats, 
                    peak_energy, partial_peak_energy, off_peak_energy,
                    charge, peak_charge, partial_peak_charge, off_peak_charge, kgco2e)
      WITH epochs AS (
        SELECT hd.device_id, hdk.key_id,
          MAX(hdsm.stat_time) AS latest_stat_time
        FROM hd_device hd
        JOIN hd_device_keys hdk  ON hdk.device_type = hd.device_type
        LEFT JOIN hd_device_statistics_monthly hdsm ON hdsm.device_id = hd.device_id
        WHERE hd.device_type = 'E'
        GROUP BY hd.device_id, hdk.key_id
      )
      SELECT 
        DATE_TRUNC('month', hdsd.stat_time) AS truncated_stat_time,
        hdsd.device_id, 
        4 AS stat_type4, --月統計
        hdsd.device_type,
        hdsd.key_id,
        CASE
          WHEN hdk.key_type = 'C' THEN SUM(hdsd.dbl_stats)
          ELSE AVG(hdsd.dbl_stats) -- 除了耗電量，其他都是平均
        END AS dbl_stats,
        SUM(hdsd.peak_energy) AS peak_energy,
        SUM(hdsd.partial_peak_energy) AS partial_peak_energy,
        SUM(hdsd.off_peak_energy) AS off_peak_energy,
        SUM(hdsd.charge) AS charge,
        SUM(hdsd.peak_charge) AS peak_charge,
        SUM(hdsd.partial_peak_charge) AS partial_peak_charge,
        SUM(hdsd.off_peak_charge) AS off_peak_charge,
        SUM(hdsd.kgco2e) AS kgco2e
      FROM hd_device_statistics_daily hdsd
      JOIN hd_device_keys hdk  ON hdk.device_type = hdsd.device_type
      LEFT JOIN epochs ep ON ep.device_id = hdsd.device_id AND ep.key_id = hdsd.key_id 
      WHERE hdsd.device_type ='E'
        AND hdsd.key_id = hdk.key_id
        AND hdsd.device_id = v_device_id
        AND hdsd.stat_time > COALESCE (ep.latest_stat_time + INTERVAL '1 month' - INTERVAL '1 second', TO_TIMESTAMP('1911-01-01', 'YYYY-MM-DD'))
      GROUP BY 
        truncated_stat_time, hdsd.device_id, stat_type4, hdsd.device_type, hdsd.key_id, hdk."key_type"
      ORDER BY 
        truncated_stat_time, hdsd.device_id, stat_type4, hdsd.device_type, hdsd.key_id
--      ON CONFLICT(stat_time, device_id, key_id, device_type) DO UPDATE SET
--            dbl_stats = EXCLUDED.dbl_stats,
--            peak_energy = EXCLUDED.peak_energy, 
--            partial_peak_energy = EXCLUDED.partial_peak_energy, 
--            off_peak_energy = EXCLUDED.off_peak_energy,
--            charge = EXCLUDED.charge,
--            peak_charge = EXCLUDED.peak_charge, 
--            partial_peak_charge = EXCLUDED.partial_peak_charge, 
--            off_peak_charge = EXCLUDED.off_peak_charge,
--            kgco2e = EXCLUDED.kgco2e
      ;
  
      GET DIAGNOSTICS ins_rows = ROW_COUNT;
      RAISE NOTICE '  新增 % 的月統計資料，共新增 % 筆。', v_device_id, ins_rows;

    END LOOP;

    -- 關閉游標
    CLOSE v_device_cursor;  

    -- 捕捉異常並記錄錯誤，不中斷主迴圈
    RAISE NOTICE '所有電表的月統計處理完成。';
  
  END;
$procedure$
;
