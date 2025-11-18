-- DROP FUNCTION public.fn_get_electricity_rate(timestamp);

CREATE OR REPLACE FUNCTION public.fn_get_electricity_rate(ts TIMESTAMP WITHOUT TIME ZONE)
 RETURNS real
 LANGUAGE plpgsql
AS $function$
  DECLARE

    electricity_rate real;

  BEGIN

    WITH energy_charge_rate AS (
      SELECT ec.*
      FROM electricity_rate er
      JOIN energy_charge ec ON ec.electricity_rate_id = er.id
      WHERE er.name = '特高壓三段式時間電價'
    ),
    rate_table AS (
    SELECT 
    -- 週一到週五
      -- 夏月
      (SELECT ecr.summer_rate      FROM energy_charge_rate ecr WHERE ecr.day_type='O' AND period_type='PE' AND season_type='S') AS O_PE_S_summer_rate,
      (SELECT ecr.summer_rate      FROM energy_charge_rate ecr WHERE ecr.day_type='O' AND period_type='PP' AND season_type='S') AS O_PP_S_summer_rate,
      (SELECT ecr.summer_rate      FROM energy_charge_rate ecr WHERE ecr.day_type='O' AND period_type='OP' AND season_type='S') AS O_OP_S_summer_rate,
      -- 非夏月
      (SELECT ecr.non_summer_rate  FROM energy_charge_rate ecr WHERE ecr.day_type='O' AND period_type='PP' AND season_type='N') AS O_PP_N_non_summer_rate,
      (SELECT ecr.non_summer_rate  FROM energy_charge_rate ecr WHERE ecr.day_type='O' AND period_type='OP' AND season_type='N') AS O_OP_N_non_summer_rate,
    -- 週六
      -- 夏月
      (SELECT ecr.summer_rate      FROM energy_charge_rate ecr WHERE ecr.day_type='T' AND period_type='PP' AND season_type='S') AS T_PP_S_summer_rate,
      (SELECT ecr.summer_rate      FROM energy_charge_rate ecr WHERE ecr.day_type='T' AND period_type='OP' AND season_type='S') AS T_OP_S_summer_rate,
      -- 非夏月
      (SELECT ecr.non_summer_rate  FROM energy_charge_rate ecr WHERE ecr.day_type='T' AND period_type='PP' AND season_type='N') AS T_PP_N_non_summer_rate,
      (SELECT ecr.non_summer_rate  FROM energy_charge_rate ecr WHERE ecr.day_type='T' AND period_type='OP' AND season_type='N') AS T_OP_N_non_summer_rate,
    -- 週日及離峰日     
      -- 夏月
      (SELECT ecr.summer_rate      FROM energy_charge_rate ecr WHERE ecr.day_type='S' AND period_type='OP' AND season_type='A') AS S_OP_S_summer_rate,
      -- 非夏月
      (SELECT ecr.non_summer_rate  FROM energy_charge_rate ecr WHERE ecr.day_type='S' AND period_type='OP' AND season_type='A') AS S_OP_N_non_summer_rate
    )
    SELECT
       CASE
          WHEN EXTRACT('dow' FROM ts) BETWEEN 1 AND 5 THEN -- 週一到週五
            CASE
              WHEN TO_CHAR(ts, 'MMDD') BETWEEN '0516' AND '1015' THEN -- 夏月:S
                CASE 
                  WHEN EXTRACT('hour' FROM ts) BETWEEN 16 AND 22 THEN -- 尖峰時間: PE
                    rt.O_PE_S_summer_rate
                  WHEN (EXTRACT('hour' FROM ts) BETWEEN 09 AND 16) OR (EXTRACT('hour' FROM ts) BETWEEN 22 AND 24) THEN -- 半尖峰時間: PP
                    rt.O_PP_S_summer_rate
                  WHEN EXTRACT('hour' FROM ts) BETWEEN 00 AND 09 THEN -- 離峰時間: OP
                    rt.O_OP_S_summer_rate 
                END
              ELSE -- 非夏月:N
                CASE
                  WHEN (EXTRACT('hour' FROM ts) BETWEEN 06 AND 11) OR (EXTRACT('hour' FROM ts) BETWEEN 14 AND 24) THEN -- 半尖峰時間: PP
                    rt.O_PP_N_non_summer_rate
                  WHEN (EXTRACT('hour' FROM ts) BETWEEN 00 AND 06) OR (EXTRACT('hour' FROM ts) BETWEEN 11 AND 14) THEN -- 離峰時間: OP
                    rt.O_OP_N_non_summer_rate
                END
            END 
          WHEN EXTRACT('dow' FROM ts) = 6 THEN -- 週六: T
            CASE
              WHEN TO_CHAR(ts, 'MMDD') BETWEEN '0516' AND '1015' THEN -- 夏月:S
                CASE 
                  WHEN (EXTRACT('hour' FROM ts) BETWEEN 09 AND 24) THEN -- 半尖峰時間: PP
                    rt.T_PP_S_summer_rate
                  WHEN EXTRACT('hour' FROM ts) BETWEEN 00 AND 09 THEN -- 離峰時間: OP
                    rt.T_OP_S_summer_rate
                END
              ELSE -- 非夏月:N
                CASE
                  WHEN (EXTRACT('hour' FROM ts) BETWEEN 06 AND 11) OR (EXTRACT('hour' FROM ts) BETWEEN 14 AND 24) THEN -- 半尖峰時間: PP
                    rt.T_PP_N_non_summer_rate
                  WHEN (EXTRACT('hour' FROM ts) BETWEEN 00 AND 06) OR (EXTRACT('hour' FROM ts) BETWEEN 11 AND 14) THEN -- 離峰時間: OP
                    rt.T_OP_N_non_summer_rate
                END
            END    
        ELSE -- 週日及離峰日
          CASE
            WHEN TO_CHAR(ts, 'MMDD') BETWEEN '0516' AND '1015' THEN -- 夏月:S
              rt.S_OP_S_summer_rate
            ELSE -- 非夏月:N
              rt.S_OP_N_non_summer_rate
          END
      END into electricity_rate
    FROM rate_table rt
    ;

    RETURN electricity_rate;

  END;
$function$
;

-- Permissions

ALTER FUNCTION public.fn_get_electricity_rate(timestamp) OWNER TO postgres;
GRANT ALL ON FUNCTION public.fn_get_electricity_rate(timestamp) TO postgres;
