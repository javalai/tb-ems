-- DROP PROCEDURE public.sp_update_energy_rate_day_sub(text);

CREATE OR REPLACE PROCEDURE public.sp_update_energy_rate_day_sub(IN p_device_id text)
 LANGUAGE plpgsql
AS $procedure$

-- 2025/03/16: 改成 Cursor 處理，並利用 DBLINK 以達成 Subtransaction Commit 效果。
    
DECLARE 

  v_DEBUG BOOL := 0;
  v_record RECORD;
  v_next_record RECORD;
  v_energy_key_id INT;
  v_consumed_energy_key_id INT;
  is_last BOOLEAN := FALSE;
  v_start_time TIMESTAMP;
  v_duration FLOAT;

  v_ins_rows NUMERIC := 0;

  -- 根據 2025/3/16 協議，決定日統計才做瞬間物理量的統計(平均、最小、最大)，其他分統計、日統計、月統計、年統計都不做。
  -- 考慮因素：
  --     1. 因為硬碟I/O過慢，必須減少對於 ts_kv 的讀取頻率。
  --     2. 目前戰情室儀表板只有針對日統計有顯示相關電壓、電流、功率、功因、頻率的平均值統計
  --     3. 能資源耗用量(電量、水量、空氣量、蒸氣量、燃氣量)的部分，資料來源還是從分統計、時統計一路加總而來。
  -- 因本程式為能資源耗用量日統計，故仍是由時統計進行加總。
  -- 由於2025/01之電力資料多有問題，故全數由2025/02/01之後開始起算，須將hd_device_stat_day_latest當中電力統計最後日期
  -- 均定在2025/02/01 07:59:59，但跑完分析後都會到最新日期。

  v_cursor CURSOR FOR 
    /*
     * key                             key_id
       147 = 'AVG_Voltage'  
       148 = 'AVG_Current'  
       153 = 'Reactive_Power'  變 628 = 'AVG_Reactive_Power'
       157 = 'AVG_PF'  
       158 = 'HZ'  變 629 = 'AVG_HZ'
       630 = 'Active_Power'  變 627 = 'AVG_Active_Power'
       566 = 'active_power'  變 627 = 'AVG_Active_Power'

       water_total_accumulated_volume  569
       air_today_accumulated_volume    596
       steam_total_accumulated_volume  608
       gas_total_accumulated_volume    612
       water_consumed_volume           626
       steam_consumed_volume           631
       air_consumed_volume             634
       gas_consumed_volume             635
     */
    SELECT
      -- st.latest_stat_time,
      HD_DATE_TRUNC(TO_TIMESTAMP(tk.ts/1000)) AS truncated_stat_day,
      hd.device_id, 
      3 AS stat_type, -- 日統計
      hd.device_type,
      -- tk."key",
--      CASE 
--        WHEN tk."key" = 153 /* Reactive_Power */  THEN 628 /* AVG_Reactive_Power */
--        WHEN tk."key" = 158 /* HZ */ THEN 629 /* AVG_HZ */
--        WHEN tk."key" = 630 /*Active_Power*/ THEN 627 /* AVG_Active_Power */
--        ELSE tk."key"
--      END AS rate_key_id,
      hakc.avg_key_id,
      AVG(tk.dbl_v) AS dbl_stats,
      MIN(tk.dbl_v) AS dbl_min,
      MAX(tk.dbl_v) AS dbl_max
    FROM hd_device hd
    JOIN ts_kv tk ON tk.entity_id = hd.entity_id
    JOIN key_dictionary kd ON kd.key_id = tk."key"
    JOIN hd_avg_key_config hakc ON hakc.rate_key_id = tk."key"
    LEFT JOIN hd_config_param hcp ON hcp.factory_id = hd.factory_id AND hcp.device_type = hd.device_type
    LEFT JOIN hd_device_stat_day_latest st ON st.device_id = hd.device_id AND st.device_type = hd.device_type AND st.key_id =  hakc.avg_key_id
    WHERE hd.device_id = p_device_id --'P22_1'
      AND hd.device_type IN ('E', 'W', 'G', 'A', 'S')
      AND kd."key" IN (
        /* 電力 */ 'AVG_Active_Power', 'AVG_Active_Power', 'AVG_HZ', 'AVG_Voltage', 'AVG_Current', 'AVG_PF',
        /* 其他 */ 'water_flow_rate', 'air_flow_rate', 'gas_flow_rate', 'steam_flow_rate'
      )
      AND tk.ts > EXTRACT(EPOCH FROM COALESCE(st.latest_stat_time, TO_TIMESTAMP('2025-02-01','YYYY-MM-DD')))*1000
    GROUP BY 
      truncated_stat_day, hd.device_id, stat_type, hd.device_type, hakc.avg_key_id, kd."key"
    ORDER BY
      truncated_stat_day, hd.device_id, stat_type, hd.device_type, hakc.avg_key_id
    ;


BEGIN
     
  IF v_DEBUG THEN RAISE NOTICE '開始處理 % 的能資源瞬間量日統計資料...', p_device_id; END IF;

  -- 通常最後一筆時統計資料，因為時間差的關係，有可能不完整，需要重新統計
  -- 以下處理新的日統計資料
  SELECT CLOCK_TIMESTAMP() INTO v_start_time;
  SELECT 0 INTO v_duration;

  IF v_DEBUG THEN RAISE NOTICE '準備新增 % 的能資源瞬間量日統計資料...', p_device_id; END IF;

  -- 打開 Cursor
  OPEN v_cursor;

  -- 先讀取第一筆
  FETCH v_cursor INTO v_record;
  
  -- 若第一筆沒有資料，直接結束
  IF NOT FOUND THEN
      RAISE NOTICE '表中無 % 的資料', p_device_id;
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

      INSERT INTO hd_device_statistics_daily (
        stat_time, device_id, stat_type, device_type, key_id, 
        dbl_stats, dbl_min, dbl_max
      )
      VALUES (
        /* 基本資料 */ v_record.truncated_stat_day, v_record.device_id, v_record.stat_type, v_record.device_type, v_record.avg_key_id, 
        /* DBL */ v_record.dbl_stats, v_record.dbl_min, v_record.dbl_max
      )
      -- 如果基本資料重複，則進行更新
      ON CONFLICT(stat_time, device_id, key_id, device_type) DO UPDATE SET
            dbl_stats = EXCLUDED.dbl_stats,
            dbl_min = EXCLUDED.dbl_min,
            dbl_max = EXCLUDED.dbl_max
      ;

      v_ins_rows := v_ins_rows + 1;

      -- 2025/05/23 Lawrence 
      -- 因為瞬間量統計一次會選取多種瞬間量，以至於is_last並非真的最後一筆，所以改為每筆更新一次hd_device_stat_day_latest
      -- 未來再想辦法優化
      -- IF is_last THEN
      --   RAISE NOTICE '  id: %, time: % -> 最後一筆!', v_record.device_id, v_record.truncated_stat_day;
      --
      --   INSERT INTO public.hd_device_stat_day_latest(device_id, device_type, key_id, latest_stat_time, latest_epoch)
      -- VALUES (
      --    v_record.device_id, 
      --    v_record.device_type, 
      --    v_record.avg_key_id,
      --    v_record.truncated_stat_day + INTERVAL '7 hours 59 minutes 59 seconds',
      --    EXTRACT(EPOCH FROM v_record.truncated_stat_day + INTERVAL '7 hours 59 minutes 59 seconds')*1000
      --  )
      --  ON CONFLICT(device_id, device_type, key_id) DO UPDATE SET
      --    latest_stat_time = EXCLUDED.latest_stat_time,
      --    latest_epoch = EXCLUDED.latest_epoch
      --  ;
      --
      -- END IF;

      INSERT INTO public.hd_device_stat_day_latest(device_id, device_type, key_id, latest_stat_time, latest_epoch)
      VALUES (
          v_record.device_id, 
          v_record.device_type, 
          v_record.avg_key_id,
          v_record.truncated_stat_day + INTERVAL '7 hours 59 minutes 59 seconds',
          EXTRACT(EPOCH FROM v_record.truncated_stat_day + INTERVAL '7 hours 59 minutes 59 seconds')*1000
      )
      ON CONFLICT(device_id, device_type, key_id) DO UPDATE SET
        latest_stat_time = EXCLUDED.latest_stat_time,
        latest_epoch = EXCLUDED.latest_epoch
      ;

      -- 如果是最後一筆，結束迴圈
      IF is_last THEN
          EXIT;
      END IF;

      -- 將下一筆資料轉為當前記錄
      v_record := v_next_record;

  END LOOP;    

  SELECT EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP()-v_start_time)) INTO v_duration;
  RAISE NOTICE '  新增 % 的瞬間量日統計資料，共新增 % 筆，計時 % 秒。', p_device_id, v_ins_rows, v_duration;

  EXCEPTION
    WHEN OTHERS THEN
      -- 捕捉異常並記錄錯誤，不中斷主迴圈
      RAISE EXCEPTION ' 處理設備 % 時發生錯誤，資料時間: %，原因: %。', p_device_id, v_record.truncated_stat_day, SQLERRM;


END;
$procedure$
;
