{{ config(schema="intermediate", alias="int_offense_by_hour") }}

with parsed as (
  select
    offense_category,
    offense_type,
    neighborhood,
    offense_count,
    occur_date,
    report_month_year,
    case
      when nullif(trim(occur_time), '') ~ '^\d{1,2}:\d{2}$'
        then split_part(trim(occur_time), ':', 1)::int
      when nullif(trim(occur_time), '') ~ '^\d{3,4}$'
        then left(lpad(trim(occur_time), 4, '0'), 2)::int
    end as occur_hour
  from {{ ref("stg_portland_offenses") }}
  where occur_date is not null
)

select
  occur_hour,
  offense_category,
  neighborhood,
  report_month_year,
  date_trunc('month', occur_date)::date as month_start,
  sum(coalesce(offense_count, 0)) as offense_count
from parsed
where occur_hour between 0 and 23
group by 1, 2, 3, 4, 5
