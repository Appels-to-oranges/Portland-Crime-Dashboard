{{ config(schema="staging", alias="stg_weather_daily") }}

select
  obs_date,
  temp_max_c,
  precip_mm,
  _loaded_at
from {{ source("raw_reference", "weather_daily") }}
