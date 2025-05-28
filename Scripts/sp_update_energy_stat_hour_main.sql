-- DROP PROCEDURE public.sp_update_energy_stat_hour_main();

CREATE OR REPLACE PROCEDURE public.sp_update_energy_stat_hour_main()
 LANGUAGE plpgsql
AS $procedure$

  DECLARE 
    v_device_cursor CURSOR FOR
      SELECT hd.device_id, hd.entity_id 
      FROM hd_device hd
      WHERE hd.entity_id IS NOT NULL 
        AND hd.device_type = 'E'
        AND hd.factory_id <> 'KC' AND hd.factory_id <> 'KT'
      ;
    v_device_record RECORD;
    v_device_id varchar(50);

    v_start_time TIMESTAMP;
    v_duration FLOAT;

  BEGIN
    
    -- 開啟游標
    OPEN v_device_cursor;

    -- 依序處理
    LOOP
      FETCH NEXT FROM v_device_cursor INTO v_device_record;
      EXIT WHEN NOT FOUND;
      v_device_id = v_device_record.device_id;
     
      RAISE NOTICE '開始處理 % 的時統計資料...', v_device_id;

      BEGIN
      
        SELECT CLOCK_TIMESTAMP() INTO v_start_time;
        SELECT 0 INTO v_duration;

        PERFORM DBLINK('host=/var/run/postgresql port=5432 user=postgres password=kenda2415@ dbname=thingsboard',
          FORMAT('CALL public.sp_update_energy_stat_hour_sub( ''%s'' )', v_device_id));

        SELECT EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP()-v_start_time)) INTO v_duration;

        RAISE NOTICE '處理 % 的資料，計時 % 秒', v_device_id, v_duration;

      EXCEPTION
        WHEN query_canceled THEN
          RAISE NOTICE '當處理到 % 時，發生query_canceled錯誤！，繼續處理下一顆電表', v_device_id;

      END;

    END LOOP;

    -- 關閉游標
    CLOSE v_device_cursor;

    -- 捕捉異常並記錄錯誤，不中斷主迴圈
    RAISE NOTICE '所有電表的時統計處理完成。';
    
  END;
$procedure$
;
