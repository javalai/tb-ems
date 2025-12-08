-- 週一~週五 尖峰 夏月
      SELECT '2025-09-01 16:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 16:00:00') AS fn_output, 'PE' AS expected) -- 113-10-16版, 夏月  , 一五, 尖峰 = 9.39
UNION SELECT '2025-09-01 21:59:59' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 21:59:59') AS fn_output, 'PE' AS expected) -- 113-10-16版, 夏月  , 一五, 尖峰 = 5.85  ??? 應該是 PP
UNION SELECT '2025-09-01 22:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 22:00:00') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 尖峰 = 5.85  ??? 應該是 PP

-- 週一~週五 半尖峰 夏月
UNION SELECT '2025-09-01 09:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 09:00:00') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-09-01 15:59:59' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 15:59:59') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-09-01 16:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 16:00:00') AS fn_output, 'PE' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85

UNION SELECT '2025-09-01 22:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 22:00:00') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-09-01 23:59:59' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 23:59:59') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-09-01 24:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 24:00:00') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 2.53

-- 週一~週五 半尖峰 非夏月
UNION SELECT '2025-12-01 06:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 06:00:00') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-12-01 10:59:59' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 10:59:59') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-12-01 11:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 11:00:00') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85

UNION SELECT '2025-12-01 14:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 14:00:00') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-12-01 23:59:59' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 23:59:59') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-12-01 24:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 24:00:00') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 2.53

-- 週一~週五 離峰 夏月
UNION SELECT '2025-09-01 00:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 00:00:00') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-09-01 08:59:59' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 08:59:59') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-09-01 09:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-01 09:00:00') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85

-- 週一~週五 離峰 非夏月
UNION SELECT '2025-12-01 00:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 00:00:00') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-12-01 05:59:59' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 05:59:59') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-12-01 06:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 06:00:00') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 2.53
UNION SELECT '2025-12-01 11:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 11:00:00') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-12-01 13:59:59' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 13:59:59') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 5.85
UNION SELECT '2025-12-01 14:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-01 14:00:00') AS fn_output, 'PP' AS expected) -- 113-10-16版, 夏月  , 一五, 半尖峰 = 2.53

-- 週六 半尖峰 夏月 
UNION SELECT '2025-09-06 09:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-06 09:00:00') AS fn_output, 'SPP' AS expected) -- 113-10-16版, 夏月  , 週六, 尖峰 = 2.6
UNION SELECT '2025-09-06 23:59:59' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-06 23:59:59') AS fn_output, 'SPP' AS expected) -- 113-10-16版, 夏月  , 週六, 尖峰 = 2.6
UNION SELECT '2025-09-06 24:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-06 24:00:00') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 週六, 尖峰 = 2.6
-- 週六 半尖峰 非夏月 
UNION SELECT '2025-12-06 06:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-06 06:00:00') AS fn_output, 'SPP' AS expected) -- 113-10-16版, 夏月  , 週六, 半尖峰 = 2.6
UNION SELECT '2025-12-06 10:59:59' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-06 10:59:59') AS fn_output, 'SPP' AS expected) -- 113-10-16版, 夏月  , 週六, 半尖峰 = 2.6
UNION SELECT '2025-12-06 11:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-06 11:00:01') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 週六, 半尖峰 = 2.6
UNION SELECT '2025-12-06 14:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-06 14:00:00') AS fn_output, 'SPP' AS expected) -- 113-10-16版, 夏月  , 週六, 半尖峰 = 2.53
UNION SELECT '2025-12-06 23:59:59' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-06 23:59:59') AS fn_output, 'SPP' AS expected) -- 113-10-16版, 夏月  , 週六, 半尖峰 = 2.53
UNION SELECT '2025-12-06 24:00:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-06 24:00:00') AS fn_output, 'OP' AS expected) -- 113-10-16版, 夏月  , 週六, 半尖峰 = 2.53

-- 週日及離峰日 夏月
UNION SELECT '2025-09-07 11:23:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-09-07 11:23:00') AS fn_output, 'OP' AS expected) -- 114-10-01版, 非夏月, 週六, 離峰 = 2.32
-- 週日及離峰日 非夏月
UNION SELECT '2025-12-07 11:23:00' AS "day", fn_output, expected, CASE WHEN fn_output = expected THEN 'Y' ELSE 'N' END AS verify FROM (SELECT fn_get_period_type('2025-12-07 11:23:00') AS fn_output, 'OP' AS expected) -- 114-10-01版, 非夏月, 週六, 離峰 = 2.32
;