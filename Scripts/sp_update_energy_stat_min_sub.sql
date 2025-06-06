-- DROP PROCEDURE public.sp_update_energy_stat_min_sub(text);

CREATE OR REPLACE PROCEDURE public.sp_update_energy_stat_min_sub(IN p_device_id text)
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
      WITH keyids AS (
        SELECT
         (SELECT key_id AS consumed_energy_key_id FROM key_dictionary WHERE "key"='ConsumedEnergy')
      ),
      coeffs AS (
        SELECT hcp.param_id, hcp.param_name, hcp.float_value AS coeff, hcp.factory_id 
        FROM hd_config_param hcp 
      )
      SELECT t.device_id, MAX(t.stat_time) AS stat_time, t.stat_type, t.consumed_energy, t.device_type, t.new_key_id, t.consumed_energy*ce.coeff AS kgco2e
      FROM (
        SELECT
          hd.device_id,
          hd.factory_id,
          DATE_TRUNC('minute', TO_TIMESTAMP(tk.ts/1000)) AS stat_time,
          1 AS stat_type, -- 分統計
          CASE
            WHEN COALESCE(tk.dbl_v, 0) < COALESCE(LAG(tk.dbl_v) OVER (ORDER BY tk.entity_id, tk.ts), COALESCE(tk.dbl_v, 0)) THEN 0
            ELSE COALESCE(tk.dbl_v, 0) - COALESCE(LAG(tk.dbl_v) OVER (ORDER BY tk.entity_id, tk.ts), COALESCE(tk.dbl_v, 0))
          END AS consumed_energy,
          hd.device_type,
          554 AS new_key_id,
          ROW_NUMBER() OVER() AS row_num         
        FROM ts_kv tk 
        JOIN hd_device hd ON hd.entity_id = tk.entity_id 
        JOIN key_dictionary kd ON kd.key_id = tk."key"
        LEFT JOIN hd_device_stat_min_latest hdsml ON hdsml.device_id=hd.device_id AND hdsml.device_type = hd.device_type AND hdsml.key_id = tk."key" -- kd.key_id
        WHERE hd.device_type ='E'
          AND kd."key" IN ('Energy')
          AND hd.device_id = p_device_id
          AND tk.ts > COALESCE(hdsml.latest_epoch, EXTRACT(EPOCH FROM TO_TIMESTAMP('1911-01-01', 'YYYY-MM-DD') )*1000)
        ORDER BY hd.device_id, stat_time, stat_type, consumed_energy, hd.device_type, new_key_id
      ) t
      JOIN coeffs ce ON ce.factory_id = t.factory_id
      WHERE t.row_num > 1
        AND ce.param_name = '電力碳排係數'
      GROUP BY t.device_id, stat_time, t.stat_type, t.consumed_energy, t.device_type, t.new_key_id, ce.coeff;


  BEGIN
    
    SELECT CLOCK_TIMESTAMP() INTO v_start_time;
    SELECT 0 INTO v_duration;

    RAISE NOTICE '準備新增 % 的耗用量分統計資料...', p_device_id;

    SELECT key_id INTO v_energy_key_id FROM key_dictionary WHERE "key"='Energy';
    SELECT key_id INTO v_consumed_energy_key_id FROM key_dictionary WHERE "key"='ConsumedEnergy';

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
        -- 新增 hd_device_statistics_minutely
        INSERT INTO hd_device_statistics_minutely(device_id, stat_time, stat_type, dbl_stats, device_type, key_id, kgco2e)
        VALUES (
          v_record.device_id, 
          v_record.stat_time, 
          v_record.stat_type, 
          v_record.consumed_energy, 
          v_record.device_type, 
          v_record.new_key_id, 
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
              v_energy_key_id, --v_record.new_key_id,
              v_record.stat_time + INTERVAL '57 seconds',
              EXTRACT(EPOCH FROM v_record.stat_time + INTERVAL '57 seconds')*1000
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
