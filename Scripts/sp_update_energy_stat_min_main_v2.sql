CREATE OR REPLACE PROCEDURE public.sp_update_energy_stat_min_main_v2(p_start_ts TEXT, p_end_ts TEXT)
 LANGUAGE plpgsql
AS $procedure$

  DECLARE 
    v_device_cursor CURSOR FOR
      SELECT hd.device_id, hd.entity_id, hdsml.latest_stat_time
      FROM hd_device hd
      LEFT JOIN hd_device_stat_min_latest hdsml ON hdsml.device_id=hd.device_id AND hdsml.device_type = hd.device_type
      WHERE hd.entity_id IS NOT NULL 
        AND hd.device_type = 'E'
        AND hd.factory_id NOT IN ('KC', 'KT')
        AND hdsml.key_id = 142
        -- AND hdsml.latest_stat_time < (CURRENT_TIMESTAMP - INTERVAL '1 hour')
      ORDER BY hd.device_id
      ;
    v_device_record RECORD;
    v_device_id VARCHAR(50);
    v_entity_id UUID;

    v_overall_start_time TIMESTAMP;
    v_overall_duration FLOAT;
    v_start_time TIMESTAMP;
    v_duration FLOAT;

  BEGIN

    SELECT CLOCK_TIMESTAMP() INTO v_overall_start_time;
    SELECT 0 INTO v_overall_duration;

    -- Open cursor
    OPEN v_device_cursor;
    -- Fetch rows and return
    LOOP

      FETCH NEXT FROM v_device_cursor INTO v_device_record;
      EXIT WHEN NOT FOUND;
      v_device_id = v_device_record.device_id;
      v_entity_id = v_device_record.entity_id;

      SELECT CLOCK_TIMESTAMP() INTO v_start_time;
      SELECT 0 INTO v_duration;
     
      RAISE NOTICE '開始處理 % 的分統計資料...', v_device_id;
      
      -- 子交易塊處理個別設備
      BEGIN

--        SELECT CLOCK_TIMESTAMP() INTO v_start_time;
--        SELECT 0 INTO v_duration;

        PERFORM DBLINK('host=/var/run/postgresql port=5432 user=postgres password=kenda2415@ dbname=thingsboard',
          FORMAT('CALL public.sp_update_energy_stat_min_sub_v2( ''%s'', ''%s'', ''%s'' )', v_device_id, p_start_ts, p_end_ts));

--        SELECT EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP()-v_start_time)) INTO v_duration;
--        RAISE NOTICE '處理 % 的資料，計時 % 秒', v_device_id, v_duration;

      SELECT EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP()-v_start_time)) INTO v_duration;
      RAISE NOTICE '% 的分統計資料已處理完成，計時 % 秒！...', v_device_id, v_duration;

      -- 捕捉異常並記錄錯誤，不中斷主迴圈
      EXCEPTION
        WHEN query_canceled THEN
          RAISE NOTICE '當處理到 % 時，發生query_canceled錯誤！，繼續處理下一顆電表', v_device_id;

      END;
    END LOOP;

    
    CLOSE v_device_cursor;

    SELECT EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP()-v_overall_start_time)) INTO v_overall_duration;
    RAISE NOTICE '所有電表的分統計處理完成，計時 % 秒。', v_overall_duration;
    
  END;
$procedure$
;
