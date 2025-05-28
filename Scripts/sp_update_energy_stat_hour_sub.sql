-- DROP PROCEDURE public.sp_update_energy_stat_hour_sub(text);

CREATE OR REPLACE PROCEDURE public.sp_update_energy_stat_hour_sub(IN p_device_id text)
 LANGUAGE plpgsql
AS $procedure$

  DECLARE 

    v_record RECORD;
    v_next_record RECORD;
    v_energy_key_id INT;
    v_consumed_energy_key_id INT;
    is_last BOOLEAN := FALSE;
    v_start_time TIMESTAMP;
    v_duration FLOAT;

    v_ins_rows NUMERIC := 0;

    v_cursor CURSOR FOR 
      WITH coeffs AS (
        SELECT hcp.param_id, hcp.param_name, hcp.float_value AS coeff, hcp.factory_id 
        FROM hd_config_param hcp 
      )
      SELECT t.truncated_stat_time AS stat_time,
        t.device_id, 
        t.stat_type,
        t.device_type,
        t.key_id,
        t.dbl_stats,
        t.long_stats,
        CASE
          WHEN t.key_id = 554 THEN -- 只有耗電量才需要計費
            -- t.long_stats * public.fn_get_electricity_rate(t.truncated_stat_time)
            -- FIX: 2025-2-1 耗電量統一為 dbl_stats
            t.dbl_stats * public.fn_get_electricity_rate(t.truncated_stat_time)
          ELSE NULL
        END AS charge,
        CASE
          WHEN t.key_id = 554 THEN -- 只有耗電量才需要知道計費區間
            public.fn_get_period_type(t.truncated_stat_time)
          ELSE NULL
        END  AS period_type,
        t.long_avg,
        t.long_min,
        t.long_max,
        t.dbl_avg,
        t.dbl_min,
        t.dbl_max,
        t.dbl_stats*ce.coeff AS kgco2e,
        t.latest_stat_time
        
      FROM (
        SELECT 
          DATE_TRUNC('hour', hdsm.stat_time) AS truncated_stat_time,
          hdsm.device_id,
          hd.factory_id,
          2 AS stat_type, -- 時統計
          hdsm.device_type,
          hdsm.key_id,
          CASE
            WHEN kd."key" = 'ConsumedEnergy' THEN SUM(hdsm.dbl_stats)
            ELSE AVG(hdsm.dbl_stats) -- 除了耗電量，其他都是平均
          END AS dbl_stats,
          CASE
            WHEN kd."key" = 'ConsumedEnergy' THEN SUM(hdsm.long_stats)
            ELSE AVG(hdsm.long_stats) -- 除了耗電量，其他都是平均
          END AS long_stats,
          AVG(hdsm.long_stats) AS long_avg,
          MIN(hdsm.long_stats) AS long_min,
          MAX(hdsm.long_stats) AS long_max,
          AVG(hdsm.dbl_stats) AS dbl_avg,
          MIN(hdsm.dbl_stats) AS dbl_min,
          MAX(hdsm.dbl_stats) AS dbl_max,
          latest.latest_stat_time
    
        FROM hd_device_statistics_minutely hdsm
        JOIN hd_device hd ON hd.device_id = hdsm.device_id
        JOIN key_dictionary kd ON kd.key_id = hdsm.key_id 
        LEFT JOIN hd_device_stat_hour_latest latest ON latest.device_id=hdsm.device_id AND latest.device_type = hdsm.device_type AND latest.key_id = hdsm.key_id 
        WHERE hdsm.device_type ='E'
          AND kd."key" IN ('ConsumedEnergy', 'total_active_energy', 'AVG_Active_Power', 'AVG_Reactive_Power', 'AVG_HZ', 'AVG_Voltage', 'AVG_Current', 'AVG_PF')
          AND hdsm.device_id = p_device_id
          AND hdsm.stat_time > COALESCE (latest.latest_stat_time, TO_TIMESTAMP('1911-01-01', 'YYYY-MM-DD'))
        GROUP BY 
          truncated_stat_time, hdsm.device_id,hd.factory_id, stat_type, hdsm.device_type, hdsm.key_id, kd."key", latest.latest_stat_time
        ORDER BY 
          truncated_stat_time, hdsm.device_id, stat_type, hdsm.device_type, hdsm.key_id
      ) t
      JOIN coeffs ce ON ce.factory_id = t.factory_id
      WHERE ce.param_name = '電力碳排係數'
      ;

  BEGIN
    
    SELECT CLOCK_TIMESTAMP() INTO v_start_time;
    SELECT 0 INTO v_duration;

    RAISE NOTICE '準備新增 % 的耗用量時統計資料...', p_device_id;

    -- 開啟游標
    OPEN v_cursor;

    -- 先讀取第一筆
    FETCH v_cursor INTO v_record;
    
    -- 若第一筆沒有資料，直接結束
    IF NOT FOUND THEN
        RAISE NOTICE '表中無資料';
        CLOSE v_cursor;
        RETURN;
    END IF;

    LOOP

        -- 預先讀取下一筆資料
        FETCH v_cursor INTO v_next_record;

        -- 如果 `FOUND` 為 FALSE，表示這是最後一筆
        IF NOT FOUND THEN
            is_last := TRUE;
        END IF;

        INSERT INTO hd_device_statistics_hourly(stat_time, device_id, stat_type, device_type, key_id, 
                                                 dbl_stats, long_stats, charge, period_type,
                                                 long_avg, long_min, long_max, dbl_avg, dbl_min, dbl_max, kgco2e)
        VALUES (v_record.stat_time, v_record.device_id, v_record.stat_type, v_record.device_type, v_record.key_id, 
                v_record.dbl_stats, v_record.long_stats, v_record.charge, v_record.period_type,
                v_record.long_avg, v_record.long_min, v_record.long_max, v_record.dbl_avg, v_record.dbl_min, v_record.dbl_max, v_record.kgco2e)
        ON CONFLICT(stat_time, device_id, key_id, device_type) DO UPDATE SET
              dbl_stats = EXCLUDED.dbl_stats,
              long_stats = EXCLUDED.long_stats,
              charge = EXCLUDED.charge,
              period_type = EXCLUDED.period_type,
              long_avg = EXCLUDED.long_avg,
              long_min = EXCLUDED.long_min,
              long_max = EXCLUDED.long_max,
              dbl_avg = EXCLUDED.dbl_avg,
              dbl_min = EXCLUDED.dbl_min,
              dbl_max = EXCLUDED.dbl_max,
              kgco2e = EXCLUDED.kgco2e
        ;

        v_ins_rows := v_ins_rows + 1;


        IF is_last THEN
            RAISE NOTICE '  id: %, time: % -> 最後一筆!', v_record.device_id, v_record.stat_time;

            INSERT INTO public.hd_device_stat_hour_latest(device_id, device_type, key_id, latest_stat_time, latest_epoch)
            VALUES (
              v_record.device_id, 
              v_record.device_type, 
              v_record.key_id, --v_record.new_key_id,
              v_record.stat_time + INTERVAL '59 minutes 59 seconds',
              EXTRACT(EPOCH FROM v_record.stat_time + INTERVAL '59 minutes 59 seconds')*1000
            )
            ON CONFLICT(device_id, device_type, key_id) DO UPDATE SET
              latest_stat_time = EXCLUDED.latest_stat_time,
              latest_epoch = EXCLUDED.latest_epoch
            ;

        END IF;

        -- 如果是最後一筆，結束迴圈
        IF is_last THEN
            EXIT;
        END IF;

        -- 將下一筆資料轉為當前記錄
        v_record := v_next_record;

    END LOOP;    

    SELECT EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP()-v_start_time)) INTO v_duration;
    RAISE NOTICE '  新增 % 的耗用量時統計資料，共新增 % 筆，計時 % 秒。', p_device_id, v_ins_rows, v_duration;

    EXCEPTION
          WHEN OTHERS THEN
            -- 捕捉異常並記錄錯誤，不中斷主迴圈
            RAISE EXCEPTION ' 處理設備 % 時發生錯誤，資料時間: %，原因: %。', p_device_id, v_record.stat_time, SQLERRM;
    
  END;
$procedure$
;
