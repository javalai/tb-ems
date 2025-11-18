-- DROP FUNCTION public.fn_get_energy_consumption2(varchar, int4, timestamp, timestamp, varchar);

CREATE OR REPLACE FUNCTION public.fn_get_energy_consumption2(p_device_id character varying, p_key_id integer, p_start_time timestamp without time zone, p_end_time timestamp without time zone, p_field character varying)
 RETURNS TABLE(bucket timestamp without time zone, min_value double precision, next_min_value double precision, energy_consumption double precision)
 LANGUAGE plpgsql
AS $function$
DECLARE
    adjusted_end_time TIMESTAMP WITHOUT TIME ZONE;
    diff INTERVAL;
BEGIN

    -------------------------------------------------------------------
    -- 先計算時間差
    -------------------------------------------------------------------
    diff := p_end_time - p_start_time;

    -------------------------------------------------------------------
    -- 依 p_field 檢查輸入時間區間是否合法
    -------------------------------------------------------------------
    IF UPPER(p_field) = 'MINUTE' AND diff <= INTERVAL '1 minute' THEN
        RAISE EXCEPTION 
            USING MESSAGE = format(
                '錯誤: 當 p_field 為 MINUTE 時，p_end_time - p_start_time (%s) 的時間差必須大於 1 分鐘。',
                diff
            ),
            HINT = '增加 p_end_time 或 減少 p_start_time.',
            ERRCODE = '22023';  -- INVALID PARAMETER VALUE
    ELSIF UPPER(p_field) = 'HOUR' AND diff <= INTERVAL '1 hour' THEN
        RAISE EXCEPTION 
            USING MESSAGE = format(
　　　　　　　　　'錯誤: 當 p_field 為 HOUR 時，p_end_time - p_start_time (%s) 的時間差必須大於 1 小時。',
                diff
            ),
            HINT = '增加 p_end_time 或 減少 p_start_time.',
            ERRCODE = '22023';

    ELSIF UPPER(p_field) = 'DAY' AND diff <= INTERVAL '1 day' THEN
        RAISE EXCEPTION 
            USING MESSAGE = format(
                '錯誤: 當 p_field 為 DAY 時，p_end_time - p_start_time (%s) 的時間差必須大於 1 天。',
                diff
            ),
            HINT = '增加 p_end_time 或 減少 p_start_time.',
            ERRCODE = '22023';

    ELSIF UPPER(p_field) = 'MONTH' AND diff <= INTERVAL '1 month' THEN
        RAISE EXCEPTION 
            USING MESSAGE = format(
                '錯誤: 當 p_field 為 MONTH 時，p_end_time - p_start_time (%s) 的時間差必須大於 1 個月。',
                diff
            ),
            HINT = '增加 p_end_time 或 減少 p_start_time.',
            ERRCODE = '22023';

    ELSIF UPPER(p_field) = 'YEAR' AND diff <= INTERVAL '1 year' THEN
        RAISE EXCEPTION 
            USING MESSAGE = format(
                '錯誤: 當 p_field 為 YEAR 時，p_end_time - p_start_time (%s) 的時間差必須大於 1 年。',
                diff
            ),
            HINT = '增加 p_end_time 或 減少 p_start_time.',
            ERRCODE = '22023';
    END IF;

    -- 根據 p_field 調整 p_end_time
    adjusted_end_time := CASE
        WHEN UPPER(p_field) = 'MINUTE' THEN p_end_time + INTERVAL '1 month' -- 刻意拉長是避免因為資料斷線而發生統計錯誤
        WHEN UPPER(p_field) = 'HOUR'   THEN p_end_time + INTERVAL '1 month' -- 刻意拉長是避免因為資料斷線而發生統計錯誤
        WHEN UPPER(p_field) = 'DAY'    THEN p_end_time + INTERVAL '1 month' -- 刻意拉長是避免因為資料斷線而發生統計錯誤
        WHEN UPPER(p_field) = 'MONTH'  THEN p_end_time + INTERVAL '1 month'
        WHEN UPPER(p_field) = 'YEAR'   THEN p_end_time + INTERVAL '1 year'
        ELSE p_end_time -- 若無匹配，保持原值
    END;

    RETURN QUERY
    WITH boundary_values AS (
        SELECT
            DATE_TRUNC(p_field, TO_TIMESTAMP(tk.ts / 1000)) AS bucket,
            MIN(tk.dbl_v) AS min_v,
            MAX(tk.dbl_v) AS max_v
        FROM ts_kv tk
        JOIN hd_device hd ON hd.entity_id = tk.entity_id
        WHERE hd.device_id = p_device_id
          AND tk.key = p_key_id
          AND TO_TIMESTAMP(tk.ts / 1000) >= p_start_time
          AND TO_TIMESTAMP(tk.ts / 1000) <= adjusted_end_time
        GROUP BY DATE_TRUNC(p_field, TO_TIMESTAMP(tk.ts / 1000))
    ),
    leads AS (
        SELECT
            bv.bucket,
            bv.min_v AS min_value,
            -- LEAD(bv.min_v) OVER (ORDER BY bv.bucket) AS next_min_value,
            COALESCE(LEAD(bv.min_v) OVER (ORDER BY bv.bucket), max_v) AS  next_min_value
        FROM boundary_values bv
        WHERE bv.min_v IS NOT NULL
    )
    SELECT
        l.bucket::TIMESTAMP WITHOUT TIME ZONE,
        l.min_value,
        l.next_min_value,
        l.next_min_value - l.min_value AS energy_consumption
    FROM leads l
    /* WHERE l.min_value IS NOT NULL AND l.next_min_value IS NOT NULL */ 
    WHERE l.bucket < p_end_time
    ORDER BY l.bucket;
END;
$function$
;
