-- DROP FUNCTION public.fn_get_period_type2(TIMESTAMP WITHOUT TIME ZONE);

CREATE OR REPLACE FUNCTION public.fn_get_period_type(ts TIMESTAMP WITHOUT TIME ZONE)
 RETURNS character
 LANGUAGE plpgsql
AS $function$

  -- NOTE: 這邊時間區間不要用 BETWEEN 判斷，會有誤判情況

  DECLARE 
    v_period_type CHAR(4);
    v_date DATE;
    v_dow INTEGER;
    v_hhmm CHAR(4);

  BEGIN

  v_date := CAST(ts AS DATE);
  v_dow := EXTRACT('dow' FROM ts);
  v_hhmm := TO_CHAR(ts, 'MMDD');

  SELECT
     CASE
        WHEN v_dow BETWEEN 1 AND 5 THEN -- 週一到週五
          CASE
            WHEN v_hhmm BETWEEN '0516' AND '1015' THEN -- 夏月:S
              CASE 
                WHEN ts >= v_date+'16:00'::TIME AND ts < v_date+'22:00'::TIME THEN 'PE' -- 尖峰時間: PE
                WHEN ts >= v_date+'09:00'::TIME AND ts < v_date+'16:00'::TIME
                  OR ts >= v_date+'22:00'::TIME AND ts < v_date+'24:00'::TIME THEN 'PP' -- 半尖峰時間: PP
                WHEN ts >= v_date+'00:00'::TIME AND ts < v_date+'09:00'::TIME THEN 'OP' -- 離峰時間: OP
              END
            ELSE -- 非夏月:N
              CASE
                WHEN (ts >= v_date+'06:00'::TIME AND ts < v_date+'11:00'::TIME) 
                  OR (ts >= v_date+'14:00'::TIME AND ts < v_date+'24:00'::TIME) THEN 'PP' -- 半尖峰時間: PP
                WHEN (ts >= v_date+'00:00'::TIME AND ts < v_date+'06:00'::TIME) 
                  OR (ts >= v_date+'11:00'::TIME AND ts < v_date+'14:00'::TIME) THEN 'OP' -- 離峰時間: OP
              END
          END 
        WHEN v_dow = 6 THEN -- 週六: T
          CASE
            WHEN v_hhmm BETWEEN '0516' AND '1015' THEN -- 夏月:S
              CASE 
                WHEN ts >= v_date+'09:00'::TIME AND ts < v_date+'24:00'::TIME THEN 'SPP' -- 週六半尖峰時間: SPP
                WHEN ts >= v_date+'00:00'::TIME AND ts < v_date+'09:00'::TIME THEN 'OP' -- 離峰時間: OP
              END
            ELSE -- 非夏月:N
              CASE
                WHEN (ts >= v_date+'06:00'::TIME AND ts < v_date+'11:00'::TIME ) 
                  OR (ts >= v_date+'14:00'::TIME AND ts < v_date+'24:00'::TIME) THEN 'SPP' -- 週六半尖峰時間: SPP
                WHEN (ts >= v_date+'00:00'::TIME AND ts < v_date+'06:00'::TIME ) 
                  OR (ts >= v_date+'11:00'::TIME AND ts < v_date+'14:00'::TIME) THEN 'OP' -- 離峰時間: OP
              END
          END    
      ELSE 'OP' -- 週日及離峰日
    END into v_period_type;

    RETURN v_period_type;

  END;
$function$
;
