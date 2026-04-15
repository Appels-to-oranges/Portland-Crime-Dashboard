{{ config(schema="staging", alias="stg_portland_offenses") }}

with src as (
  select * from {{ source("raw_portland", "offenses") }}
)

select
  nullif(trim(case_number), '') as case_number,
  nullif(trim(neighborhood), '') as neighborhood,
  nullif(trim(offense_category), '') as offense_category,
  nullif(trim(offense_type), '') as offense_type,
  nullif(trim(council_district), '') as council_district,
  case
    when trim(offense_count) ~ '^[0-9]+$' then trim(offense_count)::integer
  end as offense_count,
  case
    when trim(open_data_lat) ~ '^-?[0-9]+(\.[0-9]+)?$' then trim(open_data_lat)::double precision
  end as open_data_lat,
  case
    when trim(open_data_lon) ~ '^-?[0-9]+(\.[0-9]+)?$' then trim(open_data_lon)::double precision
  end as open_data_lon,
  case
    when nullif(trim(occur_date), '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
      then to_date(trim(occur_date), 'MM/DD/YYYY')
  end as occur_date,
  case
    when nullif(trim(report_date), '') ~ '^\d{1,2}/\d{1,2}/\d{4}$'
      then to_date(trim(report_date), 'MM/DD/YYYY')
  end as report_date,
  nullif(trim(report_month_year), '') as report_month_year,
  _loaded_at

from src
