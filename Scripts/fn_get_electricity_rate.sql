-- DROP FUNCTION public.fn_get_electricity_rate2(timestamp);

CREATE OR REPLACE FUNCTION public.fn_get_electricity_rate(ts TIMESTAMP WITHOUT TIME ZONE)
 RETURNS real
 LANGUAGE plpgsql
AS $function$
DECLARE
    electricity_rate real;

    v_day_type CHARACTER VARYING;
    v_season_type CHARACTER VARYING;
    v_period_type CHARACTER VARYING;

    v_dow INTEGER;
    v_mmdd CHARACTER VARYING;
    v_hour TIME;
    v_DEBUG BOOL := FALSE;

BEGIN
    -- 抽出時間基本欄位
    SELECT EXTRACT('dow' FROM ts) INTO v_dow;
    SELECT TO_CHAR(ts, 'MMDD') INTO v_mmdd;
	SELECT CAST(ts AS TIME) INTO v_hour;

	IF v_DEBUG THEN 
		RAISE NOTICE 'v_dow = %', v_dow;
		RAISE NOTICE 'v_mmdd = %', v_mmdd;
		RAISE NOTICE 'v_hour = %', v_hour;
    END IF;

    -- 判斷日別 / 季節別 / 時段別
    IF v_dow BETWEEN 1 AND 5 THEN -- 週一到週五
        v_day_type := 'O';

        IF v_mmdd BETWEEN '0516' AND '1015' THEN -- 夏月
            v_season_type := 'S';
            IF v_hour BETWEEN '16:00'::TIME AND '22:00'::TIME THEN
                v_period_type := 'PE'; -- 尖峰
            ELSIF (v_hour BETWEEN '09:00'::TIME AND '16:00'::TIME) OR (v_hour BETWEEN '22:00'::TIME AND '24:00'::TIME) THEN
                v_period_type := 'PP'; -- 半尖峰
            ELSE
                v_period_type := 'OP'; -- 離峰
            END IF;

        ELSE -- 非夏月
            v_season_type := 'N';
            IF (v_hour BETWEEN '06:00'::TIME AND '11:00'::TIME) OR (v_hour BETWEEN '14:00'::TIME AND '24:00'::TIME) THEN
                v_period_type := 'PP';
            ELSE
                v_period_type := 'OP';
            END IF;
        END IF;

    ELSIF v_dow = 6 THEN -- 週六
        v_day_type := 'T';

        IF v_mmdd BETWEEN '0516' AND '1015' THEN
            v_season_type := 'S';
            IF v_hour >= '09:00:00'::TIME AND v_hour < '24:00:00'::TIME THEN
                v_period_type := 'SPP';
            ELSE
                v_period_type := 'OP';
            END IF;
        ELSE
            v_season_type := 'N';
            IF (v_hour BETWEEN '06:00'::TIME AND '11:00'::TIME) OR (v_hour BETWEEN '14:00'::TIME AND '24:00'::TIME) THEN
                v_period_type := 'SPP';
            ELSE
                v_period_type := 'OP';
            END IF;
        END IF;

    ELSE -- 週日與離峰日
        v_day_type := 'S';
        v_season_type := 'A'; -- Always A for Sunday

        v_period_type := 'OP'; -- 全日離峰
    END IF;

	IF v_DEBUG THEN 
		RAISE NOTICE 'v_day_type = %', v_day_type;
		RAISE NOTICE 'v_season_type = %', v_season_type;
		RAISE NOTICE 'v_period_type = %', v_period_type;
    END IF;

    -- 查電價
    WITH energy_charge_rate AS (
      SELECT ec.*
      FROM energy_charge ec 
      WHERE ec.electricity_rate_id = (
      	SELECT MAX(er.id) FROM electricity_rate er 
      	WHERE er.effective_on <= ts
      )
    )
    SELECT 
        CASE
            WHEN v_day_type = 'O' OR v_day_type = 'T' THEN
                CASE v_season_type
                    WHEN 'S' THEN summer_rate
                    WHEN 'N' THEN non_summer_rate
                END
            WHEN v_day_type = 'S' THEN
                CASE v_season_type
                    WHEN 'S' THEN summer_rate
                    WHEN 'N' THEN non_summer_rate
                    WHEN 'A' THEN
                        CASE
                            WHEN v_mmdd BETWEEN '0516' AND '1015' THEN summer_rate
                            ELSE non_summer_rate
                        END
                END
        END INTO electricity_rate
    FROM energy_charge_rate ecr
    WHERE ecr.day_type = v_day_type
      AND ecr.season_type = v_season_type
      AND ecr.period_type = v_period_type;

    RETURN electricity_rate;
END;
$function$;