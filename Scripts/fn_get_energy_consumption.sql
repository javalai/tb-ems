DROP FUNCTION fn_get_energy_consumption;

CREATE OR REPLACE FUNCTION fn_get_energy_consumption(
    p_device_type CHAR(1),
	p_entity_id UUID,
    -- p_consumption_key_id INT4,
    p_grain TEXT DEFAULT 'day',
    p_start_time TIMESTAMP DEFAULT NULL,
    p_end_time TIMESTAMP DEFAULT NULL
)
RETURNS TABLE (
    period_start TIMESTAMP,
    energy_start DOUBLE PRECISION,
    energy_end DOUBLE PRECISION,
    consumption DOUBLE PRECISION,
    consumption_key_id INT4
) AS $$
DECLARE
    max_bucket TIMESTAMP;
BEGIN
    -- 找出目前可統計的最大區段
    SELECT 
		CASE
			WHEN p_grain = 'day' THEN MAX(HD_DATE_TRUNC(TO_TIMESTAMP(tk.ts / 1000)))::timestamp
			ELSE MAX(DATE_TRUNC(p_grain, TO_TIMESTAMP(tk.ts / 1000)))::timestamp
		END
    INTO max_bucket
    FROM ts_kv tk
	JOIN hd_key_config hkc ON hkc.device_type = p_device_type AND tk."key" = hkc.accumulation_key_id
    WHERE tk.entity_id = p_entity_id 
      AND hkc.device_type = p_device_type
--      AND hkc.consumption_key_id = p_consumption_key_id
    ;

    RETURN QUERY
    WITH raw_data AS (
        SELECT
            TO_TIMESTAMP(tk.ts / 1000)::timestamp AS ts,
            --DATE_TRUNC(p_grain, TO_TIMESTAMP(tk.ts / 1000))::timestamp AS bucket,
			CASE
				WHEN p_grain = 'day' THEN HD_DATE_TRUNC(TO_TIMESTAMP(tk.ts / 1000))::timestamp
				ELSE DATE_TRUNC(p_grain, TO_TIMESTAMP(tk.ts / 1000))::timestamp
			END AS bucket,
			hkc.consumption_key_id,
            dbl_v
        FROM ts_kv tk
        JOIN hd_key_config hkc ON hkc.device_type = p_device_type AND tk."key" = hkc.accumulation_key_id
	    WHERE tk.entity_id = p_entity_id 
	      AND hkc.device_type = p_device_type
--	      AND hkc.consumption_key_id = p_consumption_key_id
          AND tk.dbl_v IS NOT NULL
          AND tk.ts >= EXTRACT(EPOCH FROM p_start_time)*1000
    ),
    first_per_bucket AS (
        SELECT bucket, MIN(ts) AS ts_start
        FROM raw_data
        GROUP BY bucket
    ),
    bucket_map AS (
        SELECT
            bucket AS current_bucket,
            LEAD(bucket) OVER (ORDER BY bucket) AS next_bucket
        FROM first_per_bucket
    ),
    combined AS (
        SELECT
            fpb.bucket AS period_start,
            fpb.ts_start,
            bm.next_bucket,
            fpb_next.ts_start AS ts_next
        FROM first_per_bucket fpb
        JOIN bucket_map bm ON bm.current_bucket = fpb.bucket
        LEFT JOIN first_per_bucket fpb_next ON fpb_next.bucket = bm.next_bucket
        WHERE fpb.bucket <> max_bucket
    ),
    final_values AS (
        SELECT
            c.period_start,
			rs.consumption_key_id,
            rs.dbl_v AS energy_start,
            re.dbl_v AS energy_end,
            re.dbl_v - rs.dbl_v AS consumption
        FROM combined c
        JOIN raw_data rs ON rs.ts = c.ts_start
        JOIN raw_data re ON re.ts = c.ts_next
    ),
    latest_value AS (
        SELECT
            rd.bucket AS period_start,
            MIN(rd.ts) AS ts_start,
            MAX(rd.ts) AS ts_end
        FROM raw_data rd
        WHERE rd.bucket = max_bucket
        GROUP BY rd.bucket
    ),
    today_values AS (
        SELECT
            lv.period_start,
            rs.consumption_key_id,
            rs.dbl_v AS energy_start,
            re.dbl_v AS energy_end,
            re.dbl_v - rs.dbl_v AS consumption
        FROM latest_value lv
        JOIN raw_data rs ON rs.ts = lv.ts_start AND rs.bucket = lv.period_start
        JOIN raw_data re ON re.ts = lv.ts_end AND re.bucket = lv.period_start
    )

    -- 合併歷史與今日數據
    SELECT
        fv.period_start,
        fv.energy_start,
        fv.energy_end,
        fv.consumption,
        fv.consumption_key_id
    FROM final_values fv
    WHERE (p_start_time IS NULL OR fv.period_start >= p_start_time)
      AND (p_end_time IS NULL OR fv.period_start < p_end_time)

    UNION ALL

    SELECT
        tv.period_start,
        tv.energy_start,
        tv.energy_end,
        tv.consumption,
        tv.consumption_key_id
    FROM today_values tv
    WHERE (p_start_time IS NULL OR tv.period_start >= p_start_time)
      AND (p_end_time IS NULL OR tv.period_start < p_end_time)

    ORDER BY period_start;

END;
$$ LANGUAGE plpgsql;