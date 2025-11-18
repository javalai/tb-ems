CREATE OR REPLACE FUNCTION fn_verify_energy_consumption(
    p_device_id VARCHAR,
    p_key_id INTEGER,
    p_start_time TIMESTAMP WITHOUT TIME ZONE,
    p_end_time TIMESTAMP WITHOUT TIME ZONE
)
RETURNS TABLE(stat_type TEXT, check_result TEXT)
LANGUAGE SQL
AS $$
  WITH validation_result AS (
    SELECT '時統計' AS stat_type,
           CASE WHEN COUNT(*) FILTER (WHERE match_result = 'N') > 0 THEN 'NG' ELSE 'OK' END AS check_result
    FROM (
      -- 驗證時統計
	  SELECT -- hour_stat.bucket, hour_stat.energy_consumption, SUM(min_stat.energy_consumption) AS "hour_sum",
        -- hour_stat.bucket::TIMESTAMP WITHOUT TIME ZONE,
        -- hour_stat.energy_consumption,
        SUM(min_stat.energy_consumption) AS sum_energy,
        CASE WHEN hour_stat.energy_consumption = SUM(min_stat.energy_consumption) THEN 'Y' ELSE 'N' END AS match_result
      FROM fn_get_energy_consumption(p_device_id, p_key_id, p_start_time, p_end_time, 'HOUR') hour_stat
      JOIN fn_get_energy_consumption(p_device_id, p_key_id, p_start_time, p_end_time, 'MINUTE') min_stat
        ON DATE_TRUNC('HOUR', min_stat.bucket) = hour_stat.bucket
      GROUP BY hour_stat.bucket, hour_stat.energy_consumption
    ) t

    UNION

    SELECT '日統計',
           CASE WHEN COUNT(*) FILTER (WHERE match_result = 'N') > 0 THEN 'NG' ELSE 'OK' END
    FROM (
      -- 驗證日統計
	  SELECT -- day_stat.bucket, day_stat.energy_consumption, SUM(hour_stat.energy_consumption) AS "day_sum", 
        -- day_stat.bucket::TIMESTAMP WITHOUT TIME ZONE,
        -- day_stat.energy_consumption,
        SUM(hour_stat.energy_consumption) AS sum_energy,
        CASE WHEN day_stat.energy_consumption = SUM(hour_stat.energy_consumption) THEN 'Y' ELSE 'N' END AS match_result
      FROM fn_get_energy_consumption(p_device_id, p_key_id, p_start_time, p_end_time, 'DAY') day_stat
      JOIN fn_get_energy_consumption(p_device_id, p_key_id, p_start_time, p_end_time, 'HOUR') hour_stat
        ON DATE_TRUNC('DAY', hour_stat.bucket) = day_stat.bucket
      GROUP BY day_stat.bucket, day_stat.energy_consumption
    ) t

    UNION

    SELECT '月統計',
           CASE WHEN COUNT(*) FILTER (WHERE match_result = 'N') > 0 THEN 'NG' ELSE 'OK' END
    FROM (
      -- 驗證月統計
	  SELECT -- month_stat.bucket, month_stat.energy_consumption, SUM(day_stat.energy_consumption) AS "month_sum", 
        -- month_stat.bucket::TIMESTAMP WITHOUT TIME ZONE,
        -- month_stat.energy_consumption,
        SUM(day_stat.energy_consumption) AS sum_energy,
        CASE WHEN month_stat.energy_consumption = SUM(day_stat.energy_consumption) THEN 'Y' ELSE 'N' END AS match_result
      FROM fn_get_energy_consumption(p_device_id, p_key_id, p_start_time, p_end_time, 'MONTH') month_stat
      JOIN fn_get_energy_consumption(p_device_id, p_key_id, p_start_time, p_end_time, 'DAY') day_stat
        ON DATE_TRUNC('MONTH', day_stat.bucket) = month_stat.bucket
      GROUP BY month_stat.bucket, month_stat.energy_consumption
    ) t

    UNION

    SELECT '年統計',
           CASE WHEN COUNT(*) FILTER (WHERE match_result = 'N') > 0 THEN 'NG' ELSE 'OK' END
    FROM (
      -- 驗證年統計
	  SELECT -- year_stat.bucket, year_stat.energy_consumption, SUM(month_stat.energy_consumption) AS "year_sum", 
        -- year_stat.bucket::TIMESTAMP WITHOUT TIME ZONE,
        -- year_stat.energy_consumption,
        SUM(month_stat.energy_consumption) AS sum_energy,
        CASE WHEN year_stat.energy_consumption = SUM(month_stat.energy_consumption) THEN 'Y' ELSE 'N' END AS match_result
      FROM fn_get_energy_consumption(p_device_id, p_key_id, p_start_time, p_end_time, 'YEAR') year_stat
      JOIN fn_get_energy_consumption(p_device_id, p_key_id, p_start_time, p_end_time, 'MONTH') month_stat
        ON DATE_TRUNC('YEAR', month_stat.bucket) = year_stat.bucket
      GROUP BY year_stat.bucket, year_stat.energy_consumption
    ) t
  )
  SELECT * FROM validation_result;
$$;