-- DROP FUNCTION public.fn_get_period_type(TIMESTAMP WITHOUT TIME ZONE);

CREATE OR REPLACE FUNCTION public.fn_get_period_type(ts TIMESTAMP WITHOUT TIME ZONE)
 RETURNS character
 LANGUAGE plpgsql
AS $function$

  DECLARE 

    period_type char(2);

  BEGIN

  SELECT
     CASE
        WHEN EXTRACT('dow' FROM ts) BETWEEN 1 AND 5 THEN -- 週一到週五
          CASE
            WHEN TO_CHAR(ts, 'MMDD') BETWEEN '0516' AND '1015' THEN -- 夏月:S
              CASE 
                WHEN EXTRACT('hour' FROM ts) BETWEEN 16 AND 22 THEN 'PE' -- 尖峰時間: PE
                WHEN (EXTRACT('hour' FROM ts) BETWEEN 09 AND 16) OR (EXTRACT('hour' FROM ts) BETWEEN 22 AND 24) THEN 'PP' -- 半尖峰時間: PP
                WHEN EXTRACT('hour' FROM ts) BETWEEN 00 AND 09 THEN 'OP' -- 離峰時間: OP
              END
            ELSE -- 非夏月:N
              CASE
                WHEN (EXTRACT('hour' FROM ts) BETWEEN 06 AND 11) OR (EXTRACT('hour' FROM ts) BETWEEN 14 AND 24) THEN 'PP' -- 半尖峰時間: PP
                WHEN (EXTRACT('hour' FROM ts) BETWEEN 00 AND 06) OR (EXTRACT('hour' FROM ts) BETWEEN 11 AND 14) THEN 'OP' -- 離峰時間: OP
              END
          END 
        WHEN EXTRACT('dow' FROM ts) = 6 THEN -- 週六: T
          CASE
            WHEN TO_CHAR(ts, 'MMDD') BETWEEN '0516' AND '1015' THEN -- 夏月:S
              CASE 
                WHEN (EXTRACT('hour' FROM ts) BETWEEN 09 AND 24) THEN 'SPP' -- 週六半尖峰時間: SPP
                WHEN EXTRACT('hour' FROM ts) BETWEEN 00 AND 09 THEN 'OP' -- 離峰時間: OP
              END
            ELSE -- 非夏月:N
              CASE
                WHEN (EXTRACT('hour' FROM ts) BETWEEN 06 AND 11) OR (EXTRACT('hour' FROM ts) BETWEEN 14 AND 24) THEN 'SPP' -- 週六半尖峰時間: SPP
                WHEN (EXTRACT('hour' FROM ts) BETWEEN 00 AND 06) OR (EXTRACT('hour' FROM ts) BETWEEN 11 AND 14) THEN 'OP' -- 離峰時間: OP
              END
          END    
      ELSE 'OP' -- 週日及離峰日
    END into period_type;

    RETURN period_type;

  END;
$function$
;
