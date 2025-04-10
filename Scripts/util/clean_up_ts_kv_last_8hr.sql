-- 說明：用來清除 public.ts_kv_last_8hr 內超過最後 8 小時以前的資料

-- SELECT entity_id, "key", TO_TIMESTAMP(ts/1000), bool_v, str_v, long_v, dbl_v, json_v 
DELETE
FROM public.ts_kv_last_8hr tkl8
WHERE tkl8.ts < EXTRACT(EPOCH FROM NOW() - INTERVAL '8 hours')*1000
;

DELETE FROM public.ts_kv_last_8hr tkl8
 WHERE tkl8.str_v LIKE '%|ERROR|%' OR  tkl8.str_v LIKE '%|DEBUG|%' OR tkl8.str_v LIKE '%|INFO|%' 
 ;

-- 底下這道SQL需要單獨運行
VACUUM (FULL, ANALYZE) public.ts_kv_last_8hr;