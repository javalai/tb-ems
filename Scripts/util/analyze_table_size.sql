WITH s AS (
  SELECT table_name, table_type,
    pg_table_size(quoted_table_name) AS table_size,
    pg_indexes_size(quoted_table_name) AS indexes_size,
    pg_total_relation_size(quoted_table_name) AS total_size
  FROM (
    SELECT table_name, '"' || table_name || '"' AS quoted_table_name, table_type
    FROM information_schema.TABLES
    WHERE table_schema = 'public'
  )
),
c AS (
  SELECT relname AS table_name, reltuples AS row_counts
  FROM pg_class WHERE relkind = 'r'
)
SELECT s.table_name, s.table_type, s.table_size, s.indexes_size, s.total_size, c.row_counts, 
  CASE 
    WHEN c.row_counts > 0 THEN ROUND(s.total_size /  c.row_counts)
    ELSE 0
  END AS "row_size" 
FROM s LEFT JOIN c ON c.table_name = s.table_name
;