-- DROP PROCEDURE public.sp_update_energy_stat_day_sub(text);

-- 2025/03/16: 改成 Cursor 處理，並利用 DBLINK 以達成 Subtransaction Commit 效果。
/*
 根據 2025/3/16 協議，決定日統計才做瞬間物理量的統計(平均、最小、最大)，其他分統計、日統計、月統計、年統計都不做。
 考慮因素：
     1. 因為硬碟I/O過慢，必須減少對於 ts_kv 的讀取頻率。
     2. 目前戰情室儀表板只有針對日統計有顯示相關電壓、電流、功率、功因、頻率的平均值統計
     3. 能資源耗用量(電量、水量、空氣量、蒸氣量、燃氣量)的部分，資料來源還是從分統計、時統計一路加總而來。
 因本程式為能資源耗用量日統計，故仍是由時統計進行加總。
 由於2025/01之電力資料多有問題，故全數由2025/02/01之後開始起算，須將hd_device_stat_day_latest當中電力統計最後日期均定在2025/01/31 23:59:59
*/

CREATE OR REPLACE PROCEDURE public.sp_update_energy_stat_day_sub(IN p_device_id text)
 LANGUAGE plpgsql
AS $procedure$

    
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

  v_cursor CURSOR FOR 
    WITH coeffs AS (
      SELECT hcp.param_id, hcp.param_name, hcp.float_value AS coeff, hcp.factory_id 
      FROM hd_config_param hcp 
    )
    SELECT 
      HD_DATE_TRUNC(hdsh.stat_time) AS truncated_stat_day,
      hdsh.device_id, 
      3 AS stat_type, -- 日統計
      hdsh.device_type,
      hdsh.key_id,
      AVG(hdsh.dbl_stats) AS dbl_avg,
      MIN(hdsh.dbl_stats) AS dbl_min,
      MAX(hdsh.dbl_stats) AS dbl_max,
      SUM(hdsh.long_stats) AS long_stats,
      AVG(hdsh.long_stats) AS long_avg,
      MIN(hdsh.long_stats) AS long_min,
      MAX(hdsh.long_stats) AS long_max
    FROM hd_device hd
    JOIN hd_key_config hkc ON hkc.device_type = hd.device_type
    JOIN hd_device_statistics_hourly hdsh ON hdsh.device_id = hd.device_id
    LEFT JOIN hd_device_stat_day_latest st ON st.device_id = hdsh.device_id AND st.device_type = hdsh.device_type AND st.key_id = hdsh.key_id
    WHERE hdsh.key_id = hkc.consumption_key_id
      AND hd.device_id = p_device_id
      /* 判斷規則(假設時間為2024-12-10 09:00:00)；
        1. 09:00:00這時間必須在12/10沒錯
        2. 12/10 早上 07:59:59以前算在12/9
        3. 08:00:00以後算在12/10
      */
      AND hdsh.stat_time > COALESCE (st.latest_stat_time, TO_TIMESTAMP('1911-01-01', 'YYYY-MM-DD'))
    GROUP BY 
      truncated_stat_day, hdsh.device_id, stat_type, hdsh.device_type, hdsh.key_id
    ORDER BY
      truncated_stat_day, hdsh.device_id, stat_type, hdsh.device_type, hdsh.key_id
    ;


BEGIN
  
  IF v_DEBUG THEN RAISE NOTICE '開始處理 % 的耗用量日統計資料...', p_device_id; END IF;

  -- 通常最後一筆時統計資料，因為時間差的關係，有可能不完整，需要重新統計
  -- 以下處理新的日統計資料
  SELECT CLOCK_TIMESTAMP() INTO v_start_time;
  SELECT 0 INTO v_duration;

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

      INSERT INTO hd_device_statistics_daily (
        stat_time, device_id, stat_type, device_type, key_id, 
        /*dbl_stats, */dbl_avg, dbl_min, dbl_max, long_stats, long_avg, long_min, long_max --,
        /* peak_energy, partial_peak_energy, off_peak_energy,
        charge, peak_charge, partial_peak_charge, off_peak_charge, kgco2e*/
      )
      VALUES (
        /* 基本資料 */ v_record.truncated_stat_day, v_record.device_id, v_record.stat_type, v_record.device_type, v_record.key_id, 
        /* DBL */ /*v_record.dbl_stats, */v_record.dbl_avg, v_record.dbl_min, v_record.dbl_max, 
        /* LONG */ v_record.long_stats, v_record.long_avg, v_record.long_min, v_record.long_max --,
        /* 能源用量(電、水、空氣、瓦斯、蒸氣) */ 
        -- v_record.peak_energy, v_record.partial_peak_energy, v_record.off_peak_energy,
        /* 能源費用(電、水、瓦斯)，因為空氣與蒸氣目前沒有費率 */ 
        -- v_record.charge, v_record.peak_charge, v_record.partial_peak_charge, v_record.off_peak_charge -- , 
        /* 碳排(電、水、瓦斯)，因為空氣與蒸氣目前沒有碳排係數 */ 
        /*v_record.kgco2e*/
      )
      -- 如果基本資料重複，則進行更新
      ON CONFLICT(stat_time, device_id, key_id, device_type) DO UPDATE SET
--            dbl_stats = EXCLUDED.dbl_stats,
            dbl_avg = EXCLUDED.dbl_avg,
            dbl_min = EXCLUDED.dbl_min,
            dbl_max = EXCLUDED.dbl_max,
            long_stats = EXCLUDED.long_stats,
            long_avg = EXCLUDED.long_avg,
            long_min = EXCLUDED.long_min,
            long_max = EXCLUDED.long_max --,
--            peak_energy = EXCLUDED.peak_energy, 
--            partial_peak_energy = EXCLUDED.partial_peak_energy, 
--            off_peak_energy = EXCLUDED.off_peak_energy,
--            charge = EXCLUDED.charge,
--            peak_charge = EXCLUDED.peak_charge, 
--            partial_peak_charge = EXCLUDED.partial_peak_charge, 
--            off_peak_charge = EXCLUDED.off_peak_charge,
--            kgco2e = EXCLUDED.kgco2e
      ;

      v_ins_rows := v_ins_rows + 1;


--      IF is_last THEN
--          IF v_DEBUG THEN RAISE NOTICE '  id: %, time: % -> 最後一筆!', v_record.device_id, v_record.truncated_stat_day;  END IF;
--
--          INSERT INTO public.hd_device_stat_day_latest(device_id, device_type, key_id, latest_stat_time, latest_epoch)
--          VALUES (
--            v_record.device_id, 
--            v_record.device_type, 
--            v_record.key_id, --v_record.new_key_id,
--            v_record.truncated_stat_day + INTERVAL '7 hours 59 minutes 59 seconds',
--            EXTRACT(EPOCH FROM v_record.truncated_stat_day + INTERVAL '7 hours 59 minutes 59 seconds')*1000
--          )
--          ON CONFLICT(device_id, device_type, key_id) DO UPDATE SET
--            latest_stat_time = EXCLUDED.latest_stat_time,
--            latest_epoch = EXCLUDED.latest_epoch
--          ;
--
--      END IF;

      -- 如果是最後一筆，結束迴圈
      IF is_last THEN
          EXIT;
      END IF;

      -- 將下一筆資料轉為當前記錄
      v_record := v_next_record;

  END LOOP;    

  SELECT EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP()-v_start_time)) INTO v_duration;
  RAISE NOTICE '  新增 % 的耗用量日統計資料，共新增 % 筆，計時 % 秒。', p_device_id, v_ins_rows, v_duration;

  EXCEPTION
    WHEN OTHERS THEN
      -- 捕捉異常並記錄錯誤，不中斷主迴圈
      RAISE EXCEPTION ' 處理設備 % 時發生錯誤，資料時間: %，原因: %。', p_device_id, v_record.truncated_stat_day, SQLERRM;


END;
$procedure$
;

COMMENT ON PROCEDURE public.sp_update_energy_stat_day_sub(text) IS '能源日統計副程式
參數：
  p_device_id TEXT - 裝置ID
說明：
  依據統計資料計算瞬間量的平均值、最小值、最大值。';
