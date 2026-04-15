{{ config(schema="staging", alias="stg_zcta_geometry") }}

select
  nullif(trim(zcta), "") as zcta,
  geom,
  _loaded_at
from {{ source("raw_reference", "zcta_geometry") }}
where nullif(trim(zcta), "") is not null
  and geom is not null
