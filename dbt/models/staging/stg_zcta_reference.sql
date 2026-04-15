{{ config(schema="staging", alias="stg_zcta_reference") }}

select
  nullif(trim(zcta), "") as zcta,
  intpt_lat,
  intpt_lon,
  _loaded_at
from {{ source("raw_reference", "zcta_reference") }}
where nullif(trim(zcta), "") is not null
  and intpt_lat is not null
  and intpt_lon is not null
