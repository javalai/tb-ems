-- DROP PROCEDURE public.sp_update_energy_stat_day_main();

CREATE OR REPLACE PROCEDURE public.sp_update_energy_stat_day_main()
 LANGUAGE plpgsql
AS $procedure$
    
DECLARE 
  v_device_cursor CURSOR FOR
    SELECT hd.device_id, hd.entity_id 
    FROM hd_device hd
    WHERE hd.entity_id IS NOT NULL 
      AND hd.device_type = 'E'
      AND hd.factory_id NOT IN ('KC', 'KT')
      AND hd.device_id LIKE 'E00%'
    ;
  v_device_record RECORD;
  v_device_id VARCHAR(50);
  v_entity_id UUID;

  ins_rows numeric;
  upd_rows numeric;

BEGIN
  -- Open cursor
  OPEN v_device_cursor;
  -- Fetch rows and return
  LOOP
    FETCH NEXT FROM v_device_cursor INTO v_device_record;
    EXIT WHEN NOT FOUND;
    v_device_id = v_device_record.device_id;
    v_entity_id = v_device_record.entity_id;
     
    RAISE NOTICE '開始處理 % 的日統計資料...', v_device_id;

    BEGIN

      PERFORM DBLINK('host=/var/run/postgresql port=5432 user=postgres dbname=thingsboard',
        FORMAT('CALL public.sp_update_energy_stat_day_sub( ''%s'' )', v_device_id));

    EXCEPTION
        WHEN query_canceled THEN
          RAISE NOTICE '當處理到 % 時，發生query_canceled錯誤！，繼續處理下一顆電表', v_device_id;
    END;

  END LOOP;

  -- Close cursor
  CLOSE v_device_cursor;

  -- 捕捉異常並記錄錯誤，不中斷主迴圈
  RAISE NOTICE '所有電表的日統計處理完成。';

END;
$procedure$
;
