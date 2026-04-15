{{ config(schema="intermediate", alias="int_portland_offense_zcta") }}

{# Point-in-polygon against Census 2020 ZCTA boundaries (OR/WA subset). Requires PostGIS. #}
with o as (
  select * from {{ ref("stg_portland_offenses") }}
),
g as (
  select * from {{ ref("stg_zcta_geometry") }}
)

select
  o.case_number,
  o.neighborhood,
  o.offense_category,
  o.offense_type,
  o.council_district,
  o.offense_count,
  o.open_data_lat,
  o.open_data_lon,
  o.occur_date,
  o.report_date,
  o.report_month_year,
  o._loaded_at,
  p.zcta
from o
left join lateral (
  select gg.zcta
  from g gg
  where o.open_data_lat is not null
    and o.open_data_lon is not null
    and st_covers(
      gg.geom,
      st_setsrid(st_makepoint(o.open_data_lon, o.open_data_lat), 4326)
    )
  limit 1
) p on true
