{{ config(schema="intermediate", alias="int_offense_with_weather") }}

select
  o.*,
  w.temp_max_c as daily_high_temp_c,
  w.precip_mm as daily_precip_mm
from {{ ref("int_portland_offense_zcta") }} o
left join {{ ref("stg_weather_daily") }} w
  on w.obs_date = coalesce(o.occur_date, o.report_date)
