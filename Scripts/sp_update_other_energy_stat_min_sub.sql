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
      SELECT 
        -- st.latest_stat_time,
        ec.bucket AS stat_time,
        hd.entity_id,
        hd.device_id, 
        1 AS stat_type, -- 日統計
        hd.device_type,
        hkc.consumption_key_id AS key_id,
        ec.energy_consumption AS consumption,
        ec.energy_consumption*hcp.float_value AS kgco2e
      FROM hd_device hd
      JOIN hd_key_config hkc ON hkc.device_type = hd.device_type
      LEFT JOIN hd_config_param hcp ON hcp.factory_id = hd.factory_id AND hcp.device_type = hd.device_type
      LEFT JOIN hd_device_stat_min_latest st ON st.device_id = hd.device_id AND st.device_type = hd.device_type AND st.key_id = hkc.consumption_key_id 
      JOIN fn_get_energy_consumption2(
          p_device_id,
          DATE_TRUNC('MINUTE', COALESCE(st.latest_stat_time, TO_TIMESTAMP('1911-01-01', 'YYYY-MM-DD')::TIMESTAMP))::TIMESTAMP WITHOUT TIME ZONE,
          NULL,
          'MINUTE') ec 
        ON ec.device_id = hd.device_id
      WHERE hd.device_id = p_device_id
      GROUP BY 
        stat_time, hd.device_id, stat_type, hd.device_type, hkc.consumption_key_id, ec.energy_consumption, hcp.float_value
      ORDER BY
        stat_time, hd.device_id, stat_type, hd.device_type, hkc.consumption_key_id
      ;

  BEGIN

    SELECT CLOCK_TIMESTAMP() INTO v_start_time;
    SELECT 0 INTO v_duration;

    RAISE NOTICE '準備新增非電能源 % 的分統計資料...', p_device_id;

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

        -- 處理當前記錄
        
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
    RAISE NOTICE '  新增非電能源 % 的耗用量分統計資料，共新增 % 筆，計時 % 秒。', p_device_id, v_ins_rows, v_duration;

    EXCEPTION
      WHEN OTHERS THEN
        -- 捕捉異常並記錄錯誤，不中斷主迴圈
        RAISE EXCEPTION ' 處理設備 % 時發生錯誤，資料時間: %，原因: %。', p_device_id, v_record.stat_time, SQLERRM;
  END;
$procedure$
;

COMMENT ON PROCEDURE sp_update_other_energy_stat_min_sub(TEXT)
IS '非電能源分統計副程式
參數：
  p_device_id TEXT - 裝置ID
說明：
  依據統計資料計算耗用量';