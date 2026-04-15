{{ config(schema="staging", alias="stg_acs_zcta") }}

select
  nullif(trim(zcta), '') as zcta,
  nullif(trim(acs_year), '') as acs_year,
  population,
  poverty_universe,
  poverty_count,
  median_household_income,
  case
    when poverty_universe > 0 and poverty_count is not null
      then 100.0 * poverty_count::double precision / poverty_universe::double precision
  end as poverty_rate_pct,
  _loaded_at
from {{ source("raw_reference", "acs_zcta") }}
where nullif(trim(zcta), '') is not null
