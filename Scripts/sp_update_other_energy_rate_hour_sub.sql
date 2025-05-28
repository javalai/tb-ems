-- DROP PROCEDURE public.sp_update_other_energy_rate_hour_sub(text);

CREATE OR REPLACE PROCEDURE public.sp_update_other_energy_rate_hour_sub(IN p_device_id text)
 LANGUAGE plpgsql
AS $procedure$

  DECLARE 

    v_rec RECORD;
    v_nrec RECORD;
    v_energy_key_id INT;
    v_consumed_energy_key_id INT;
    is_last BOOLEAN := FALSE;
    v_start_time TIMESTAMP;
    v_duration FLOAT;

    v_ins_rows NUMERIC := 0;

    v_cursor CURSOR FOR 
      SELECT 
        DATE_TRUNC('hour', TO_TIMESTAMP(tk.ts/1000)) AS stat_time,
        hd.device_id, 
        2 AS stat_type, -- 時統計
        hd.device_type,
        hkc.rate_key_id AS key_id,
        COALESCE(AVG(tk.dbl_v), 0) AS dbl_stats
      FROM hd_device hd
      JOIN ts_kv tk ON tk.entity_id = hd.entity_id
      JOIN hd_key_config hkc ON hkc.device_type = hd.device_type
      LEFT JOIN hd_device_stat_hour_latest latest ON latest.device_id=hd.device_id AND latest.device_type = hd.device_type AND latest.key_id = hkc.rate_key_id
      WHERE hd.device_id = p_device_id -- IN ('W0006', 'G0085') -- 
        AND tk."key" = hkc.rate_key_id -- 代表 XXX_flow_rate
        AND tk.ts >= EXTRACT(EPOCH FROM COALESCE(DATE_TRUNC('hour', latest.latest_stat_time), '1911-01-01'))*1000
      GROUP BY stat_time, hd.device_id, stat_type, hd.device_type, hkc.rate_key_id
      ORDER BY stat_time, hd.device_id, stat_type, hd.device_type, hkc.rate_key_id
      ;

  BEGIN
  
    SELECT CLOCK_TIMESTAMP() INTO v_start_time;
    SELECT 0 INTO v_duration;

    RAISE NOTICE '準備新增非電能源 % 的瞬間量時統計資料...', p_device_id;

    -- 開啟游標
    OPEN v_cursor;

    -- 先讀取第一筆
    FETCH v_cursor INTO v_rec;
    
    -- 若第一筆沒有資料，直接結束
    IF NOT FOUND THEN
        RAISE NOTICE '表中無資料';
        CLOSE v_cursor;
        RETURN;
    END IF;

    LOOP

        -- 預先讀取下一筆資料
        FETCH v_cursor INTO v_nrec;

        -- 如果 `FOUND` 為 FALSE，表示這是最後一筆
        IF NOT FOUND THEN
            is_last := TRUE;
        END IF;

        INSERT INTO hd_device_statistics_hourly(stat_time, device_id, stat_type, device_type, key_id, dbl_stats)
        VALUES (v_rec.stat_time, v_rec.device_id, v_rec.stat_type, v_rec.device_type, v_rec.key_id,v_rec.dbl_stats)
        ON CONFLICT(stat_time, device_id, key_id, device_type) DO UPDATE SET
              dbl_stats = EXCLUDED.dbl_stats      
        ;

        v_ins_rows := v_ins_rows + 1;

        -- 只要是A. 目前 v_cursor 的最後一筆
        -- 或者是B. 下一筆資料(v_nrec)的 device_id 或 key_id 有變，表示當前資料是同一個key_id的最後一筆
        -- 不管是A,B，都要記錄到 hd_device_stat_hour_latest 當中
        IF is_last OR (v_rec.device_id <> v_nrec.device_id OR v_rec.key_id <> v_nrec.key_id) THEN
            RAISE NOTICE '  id: %, time: % -> 最後一筆!', v_rec.device_id, v_rec.stat_time;

            INSERT INTO public.hd_device_stat_hour_latest(device_id, device_type, key_id, latest_stat_time, latest_epoch)
            VALUES (
              v_rec.device_id, 
              v_rec.device_type, 
              v_rec.key_id, --v_rec.new_key_id,
              v_rec.stat_time + INTERVAL '59 minutes 59 seconds',
              EXTRACT(EPOCH FROM v_rec.stat_time + INTERVAL '59 minutes 59 seconds')*1000
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
        v_rec := v_nrec;

    END LOOP;    

    SELECT EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP()-v_start_time)) INTO v_duration;
    RAISE NOTICE '  新增 % 的瞬間量時統計資料，共新增 % 筆，計時 % 秒。', p_device_id, v_ins_rows, v_duration;

    EXCEPTION
          WHEN OTHERS THEN
            -- 捕捉異常並記錄錯誤，不中斷主迴圈
            RAISE EXCEPTION ' 處理設備 % 時發生錯誤，資料時間: %，原因: %。', p_device_id, v_rec.stat_time, SQLERRM;

  END;
$procedure$
;
