{{ config(schema="intermediate", alias="int_weather_monthly_portland") }}

select
  date_trunc('month', obs_date)::date as month_start,
  avg(temp_max_c) as avg_temp_max_c,
  sum(precip_mm) as total_precip_mm
from {{ ref("stg_weather_daily") }}
group by 1
