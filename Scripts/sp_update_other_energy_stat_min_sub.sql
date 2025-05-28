-- DROP PROCEDURE public.sp_update_other_energy_stat_min_sub(text);

CREATE OR REPLACE PROCEDURE public.sp_update_other_energy_stat_min_sub(IN p_device_id text)
 LANGUAGE plpgsql
AS $procedure$

  -- 更新除了非電能源的耗用量(consumption)統計
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
      SELECT t.stat_time, t.device_id, t.stat_type, t.device_type, t.key_id, t.consumption, 
        CASE
          WHEN hcp.float_value IS NOT NULL THEN t.consumption*hcp.float_value
        END AS kgco2e
      FROM (
        SELECT t2.device_id, t2.factory_id, t2.stat_time, t2.stat_type, t2.device_type, t2.key_id, t2.emission_coeff,
          CASE
            WHEN COALESCE(t2.dbl_v, 0) < COALESCE(LAG(t2.dbl_v) OVER (ORDER BY t2.entity_id, t2.stat_time), COALESCE(t2.dbl_v, 0)) THEN 0
            ELSE COALESCE(t2.dbl_v, 0) - COALESCE(LAG(t2.dbl_v) OVER (ORDER BY t2.entity_id, t2.stat_time), COALESCE(t2.dbl_v, 0)) 
          END AS consumption,
          ROW_NUMBER() OVER( ORDER BY t2.device_id, t2.stat_time, t2.stat_type, t2.device_type, t2.key_id) AS row_num
        FROM (
          SELECT 
            hd.device_id, hd.entity_id, hd.factory_id,
            DATE_TRUNC('minute', TO_TIMESTAMP(tk.ts/1000)) AS stat_time,
            1 AS stat_type, -- 分統計
            hd.device_type,
            hkc.consumption_key_id AS key_id,
            hkc.emission_coeff,
            MAX(tk.long_v) AS long_v,
            MAX(tk.dbl_v) AS dbl_v
          FROM hd_device hd
          JOIN ts_kv tk ON tk.entity_id =  hd.entity_id          
          JOIN hd_key_config hkc ON hkc.device_type = hd.device_type
          LEFT JOIN hd_device_stat_min_latest hdsml ON hdsml.device_id=hd.device_id AND hdsml.device_type = hd.device_type AND hdsml.key_id = hkc.consumption_key_id
          WHERE hd.device_id = p_device_id -- 
            AND tk."key" = hkc.accumulation_key_id
            AND tk.ts >= COALESCE(hdsml.latest_epoch, EXTRACT(EPOCH FROM TO_TIMESTAMP('1911-01-01', 'YYYY-MM-DD') )*1000)
            GROUP BY hd.device_id, hd.entity_id, stat_time, stat_type, hd.device_type, hkc.consumption_key_id, hkc.emission_coeff
            ORDER BY hd.device_id, hd.entity_id, stat_time, stat_type, hd.device_type, hkc.consumption_key_id
        ) t2  
      ) t
      LEFT JOIN hd_config_param hcp ON hcp.param_name = t.emission_coeff AND hcp.factory_id = t.factory_id
      WHERE t.row_num > 1
      ;

  BEGIN

    SELECT CLOCK_TIMESTAMP() INTO v_start_time;
    SELECT 0 INTO v_duration;

    RAISE NOTICE '準備新增 % 的分統計資料...', p_device_id;

    -- 打開游標
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

        -- 處理當前記錄

--        RAISE NOTICE '%, %, %, %, %, %, %, %', 
--          v_record.stat_time, 
--          v_record.device_id, 
--          v_record.stat_type, 
--          v_record.device_type, 
--          v_record.key_id, 
--          v_record.consumption, 
--          v_record.kgco2e;
        
        /* 為了統一資料類型以利計算，統計量均存入 dbl_stats */
        INSERT INTO hd_device_statistics_minutely(stat_time, device_id, stat_type, device_type, key_id, dbl_stats, kgco2e)
        VALUES (
          v_record.stat_time, 
          v_record.device_id, 
          v_record.stat_type, 
          v_record.device_type, 
          v_record.key_id, 
          v_record.consumption, 
          v_record.kgco2e
        )
        ON CONFLICT(stat_time, device_id, key_id, device_type) DO UPDATE SET
              dbl_stats = EXCLUDED.dbl_stats,
              kgco2e = EXCLUDED.kgco2e
        ;

        v_ins_rows := v_ins_rows + 1;

        IF is_last THEN
            RAISE NOTICE '  id: %, time: % -> 最後一筆!', v_record.device_id, v_record.stat_time;

            INSERT INTO public.hd_device_stat_min_latest(device_id, device_type, key_id, latest_stat_time, latest_epoch)
            VALUES (
              v_record.device_id, 
              v_record.device_type, 
              v_record.key_id,
              v_record.stat_time + INTERVAL '59 seconds',
              EXTRACT(EPOCH FROM v_record.stat_time + INTERVAL '59 seconds')*1000
            )
            ON CONFLICT(device_id, device_type, key_id) DO UPDATE SET
              latest_stat_time = EXCLUDED.latest_stat_time,
              latest_epoch = EXCLUDED.latest_epoch
            ;
--        ELSE
--            RAISE NOTICE 'id: %, name: %', v_record.id, v_record.name;
        END IF;

        -- 如果是最後一筆，結束迴圈
        IF is_last THEN
            EXIT;
        END IF;

        -- 將下一筆資料轉為當前記錄
        v_record := v_next_record;

    END LOOP;

    SELECT EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP()-v_start_time)) INTO v_duration;
    -- GET DIAGNOSTICS ins_rows = ROW_COUNT;
    RAISE NOTICE '  新增 % 的耗用量分統計資料，共新增 % 筆，計時 % 秒。', p_device_id, v_ins_rows, v_duration;

    EXCEPTION
      WHEN OTHERS THEN
        -- 捕捉異常並記錄錯誤，不中斷主迴圈
        RAISE EXCEPTION ' 處理設備 % 時發生錯誤，資料時間: %，原因: %。', p_device_id, v_record.stat_time, SQLERRM;
  END;
$procedure$
;
