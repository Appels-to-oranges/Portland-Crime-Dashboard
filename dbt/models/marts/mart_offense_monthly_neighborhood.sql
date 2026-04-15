{{ config(schema="marts", alias="mart_offense_monthly_neighborhood") }}

select
  report_month_year,
  neighborhood,
  offense_category,
  offense_type,
  sum(coalesce(offense_count, 0)) as offense_count
from {{ ref("stg_portland_offenses") }}
where report_month_year is not null
group by 1, 2, 3, 4
