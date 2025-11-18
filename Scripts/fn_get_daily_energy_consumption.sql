DROP FUNCTION fn_get_daily_energy_consumption;


CREATE OR REPLACE FUNCTION fn_get_daily_energy_consumption(
    device UUID,
    energy_key BIGINT DEFAULT 142,
    start_day DATE DEFAULT NULL,
    end_day DATE DEFAULT NULL
)
RETURNS TABLE (
    result_day DATE,
    energy_start DOUBLE PRECISION,
    energy_end DOUBLE PRECISION,
    daily_energy DOUBLE PRECISION
) AS $$
BEGIN
    RETURN QUERY
    WITH raw_data AS (
        SELECT
            to_timestamp(ts / 1000)::date AS log_day,
            ts,
            dbl_v
        FROM ts_kv
        WHERE
            entity_id = device
            AND key = energy_key
            AND dbl_v IS NOT NULL
    ),
    daily_starts AS (
        SELECT
            log_day,
            MIN(ts) AS ts_start
        FROM raw_data
        GROUP BY log_day
    ),
    next_day_starts AS (
        SELECT
            log_day AS current_day,
            LEAD(log_day) OVER (ORDER BY log_day) AS next_day
        FROM daily_starts
    ),
    combined AS (
        SELECT
            ds.log_day AS actual_day,
            ds.ts_start,
            nds.next_day,
            nd.ts_start AS ts_next
        FROM daily_starts ds
        JOIN next_day_starts nds ON ds.log_day = nds.current_day
        LEFT JOIN daily_starts nd ON nd.log_day = nds.next_day
    ),
    full_days AS (
        SELECT
            c.actual_day,
            rs.dbl_v AS energy_start,
            re.dbl_v AS energy_end,
            re.dbl_v - rs.dbl_v AS daily_energy
        FROM combined c
        JOIN raw_data rs ON rs.ts = c.ts_start AND rs.log_day = c.actual_day
        JOIN raw_data re ON re.ts = c.ts_next AND re.log_day = c.next_day
    ),
    today_latest AS (
        SELECT
            rd.log_day AS actual_day,
            MIN(rd.ts) AS ts_start,
            MAX(rd.ts) AS ts_end
        FROM raw_data rd
        WHERE rd.log_day = CURRENT_DATE
        GROUP BY rd.log_day
    ),
    today_energy AS (
        SELECT
            t.actual_day,
            rs.dbl_v AS energy_start,
            re.dbl_v AS energy_end,
            re.dbl_v - rs.dbl_v AS daily_energy
        FROM today_latest t
        JOIN raw_data rs ON rs.ts = t.ts_start AND rs.log_day = t.actual_day
        JOIN raw_data re ON re.ts = t.ts_end AND re.log_day = t.actual_day
    )
    SELECT
        fd.actual_day AS result_day,
        fd.energy_start,
        fd.energy_end,
        fd.daily_energy
    FROM full_days fd
    WHERE
        (start_day IS NULL OR fd.actual_day >= start_day)
        AND (end_day IS NULL OR fd.actual_day < end_day)

    UNION ALL

    SELECT
        te.actual_day AS result_day,
        te.energy_start,
        te.energy_end,
        te.daily_energy
    FROM today_energy te
    WHERE
        (start_day IS NULL OR te.actual_day >= start_day)
        AND (end_day IS NULL OR te.actual_day < end_day)

    ORDER BY result_day;
END;
$$ LANGUAGE plpgsql;