{{ config(schema="marts", alias="mart_offense_monthly_zcta") }}

with base as (
  select
    date_trunc('month', coalesce(occur_date, report_date))::date as month_start,
    zcta,
    neighborhood,
    offense_category,
    offense_type,
    offense_count
  from {{ ref("int_portland_offense_zcta") }}
  where coalesce(occur_date, report_date) is not null
    and zcta is not null
)

select
  b.month_start,
  b.zcta,
  b.neighborhood,
  b.offense_category,
  b.offense_type,
  a.acs_year,
  a.population,
  a.poverty_universe,
  a.poverty_count,
  a.poverty_rate_pct,
  a.median_household_income,
  w.avg_temp_max_c as month_avg_high_temp_c,
  w.total_precip_mm as month_total_precip_mm,
  sum(coalesce(b.offense_count, 0)) as offense_count
from base b
left join {{ ref("stg_acs_zcta") }} a on b.zcta = a.zcta
left join {{ ref("int_weather_monthly_portland") }} w on b.month_start = w.month_start
group by
  b.month_start,
  b.zcta,
  b.neighborhood,
  b.offense_category,
  b.offense_type,
  a.acs_year,
  a.population,
  a.poverty_universe,
  a.poverty_count,
  a.poverty_rate_pct,
  a.median_household_income,
  w.avg_temp_max_c,
  w.total_precip_mm
