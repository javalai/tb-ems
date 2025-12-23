-- DROP PROCEDURE public.sp_update_energy_stat_hour_sub(text);

/*
 根據 2025/3/16 協議，決定日統計才做瞬間物理量的統計(平均、最小、最大)，其他分統計、日統計、月統計、年統計都不做。
 考慮因素：
     1. 因為硬碟I/O過慢，必須減少對於 ts_kv 的讀取頻率。
     2. 目前戰情室儀表板只有針對日統計有顯示相關電壓、電流、功率、功因、頻率的平均值統計
     3. 能資源耗用量(電量、水量、空氣量、蒸氣量、燃氣量)的部分，資料來源還是從分統計、時統計一路加總而來。
 因本程式為能資源耗用量日統計，故仍是由時統計進行加總。
 由於2025/01之電力資料多有問題，故全數由2025/02/01之後開始起算，須將hd_device_stat_day_latest當中電力統計最後日期均定在2025/01/31 23:59:59
*/

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
      SELECT 
        ec.bucket AS stat_time,
        hd.entity_id,
        hd.device_id, 
        2 AS stat_type, -- 時統計
        hd.device_type,
        hkc.consumption_key_id AS key_id,
        ec.energy_consumption AS dbl_stats,
        ci.period_type,
        ci.charge_rate,
        ec.energy_consumption * ci.charge_rate AS charge,
        ec.energy_consumption * hcp.float_value AS kgco2e
      FROM hd_device hd
      JOIN hd_key_config hkc ON hkc.device_type = hd.device_type
      LEFT JOIN hd_config_param hcp ON hcp.factory_id = hd.factory_id AND hcp.device_type = hd.device_type
      LEFT JOIN hd_device_stat_day_latest st ON st.device_id = hd.device_id AND st.device_type = hd.device_type AND st.key_id = hkc.consumption_key_id 
      JOIN fn_get_energy_consumption2(
          hd.device_id,
          DATE_TRUNC('HOUR', COALESCE(st.latest_stat_time,  DATE '1911-01-01')),
          -- TIMESTAMP '2025-12-22 00:00:00',
          NULL,
          'HOUR') ec 
        ON ec.device_id = hd.device_id
      JOIN fn_get_charge_info(ec.bucket) ci ON ci.usage_time = ec.bucket
      WHERE hd.device_id = p_device_id -- 'E0043' 
      GROUP BY 
        stat_time, hd.device_id, stat_type, hd.device_type, hkc.consumption_key_id, ec.energy_consumption, ci.period_type, ci.charge_rate, hcp.float_value
      ORDER BY
        stat_time, hd.device_id, stat_type, hd.device_type, hkc.consumption_key_id
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
                                                 dbl_stats, charge, period_type, kgco2e)
        VALUES (v_record.stat_time, v_record.device_id, v_record.stat_type, v_record.device_type, v_record.key_id, 
                v_record.dbl_stats, v_record.charge, v_record.period_type, v_record.kgco2e)
        ON CONFLICT(stat_time, device_id, key_id, device_type) DO UPDATE SET
              dbl_stats = EXCLUDED.dbl_stats,
              charge = EXCLUDED.charge,
              period_type = EXCLUDED.period_type,
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

COMMENT ON PROCEDURE sp_update_energy_stat_hour_sub(TEXT)
IS '電能時統計副程式
參數：
  p_device_id TEXT - 裝置ID
說明：
  依據時統計資料計算耗用量與電費';


