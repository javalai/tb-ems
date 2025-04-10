hd_device_statistics_daily
hd_device_statistics_hourly
hd_device_statistics_minutely
hd_device_statistics_monthly
hd_device_statistics_yearly

ALTER TABLE public.hd_device_statistics_minutely DROP CONSTRAINT hd_device_stat_min_pk;
ALTER TABLE public.hd_device_statistics_hourly DROP CONSTRAINT hd_device_statistics_hourly_pk;
ALTER TABLE public.hd_device_statistics_daily DROP CONSTRAINT hd_device_statistics_daily_pk;
ALTER TABLE public.hd_device_statistics_monthly DROP CONSTRAINT hd_device_statistics_monthly_pk;
ALTER TABLE public.hd_device_statistics_yearly DROP CONSTRAINT hd_device_statistics_yearly_pk;

ALTER TABLE public.hd_device_statistics_minutely DROP COLUMN entity_id;
ALTER TABLE public.hd_device_statistics_hourly DROP COLUMN entity_id;
ALTER TABLE public.hd_device_statistics_daily DROP COLUMN entity_id;
ALTER TABLE public.hd_device_statistics_monthly DROP COLUMN entity_id;
ALTER TABLE public.hd_device_statistics_yearly DROP COLUMN entity_id;

ALTER TABLE public.hd_device_statistics_minutely ADD CONSTRAINT hd_device_stat_min_pk PRIMARY KEY (device_type,device_id,key_id,stat_time);
ALTER TABLE public.hd_device_statistics_hourly ADD CONSTRAINT hd_device_stat_hour_pk PRIMARY KEY (device_type,device_id,key_id,stat_time);
ALTER TABLE public.hd_device_statistics_daily ADD CONSTRAINT hd_device_stat_day_pk PRIMARY KEY (device_type,device_id,key_id,stat_time);
ALTER TABLE public.hd_device_statistics_monthly ADD CONSTRAINT hd_device_stat_mon_pk PRIMARY KEY (device_type,device_id,key_id,stat_time);
ALTER TABLE public.hd_device_statistics_yearly ADD CONSTRAINT hd_device_stat_year_pk PRIMARY KEY (device_type,device_id,key_id,stat_time);



