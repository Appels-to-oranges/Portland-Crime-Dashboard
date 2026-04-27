# Portland Crime Dashboard

End-to-end analytics pipeline that ingests Portland Police Bureau open offense data, enriches it with Census demographics, ZCTA geography, and weather, transforms everything through dbt into analytics marts, and serves an interactive dashboard.

Live at [pdxblockbrief.com](https://pdxblockbrief.com)

## Stack

- **Server:** Node.js / Express
- **Database:** PostgreSQL + PostGIS
- **Transform:** dbt (staging → intermediate → marts)
- **Ingest:** Census Bureau APIs, ZCTA shapefiles, ACS 5-year, Open-Meteo archive
- **Frontend:** Chart.js, Leaflet + CARTO basemaps

## Data Flow

1. `npm run ingest` — Downloads PPB offense CSVs, bulk loads into `raw.offenses` via `COPY`
2. `npm run ingest:reference` — Pulls Census ZCTA polygons, demographics (ACS), and daily weather; loads into `raw.*` tables
3. `npm run dbt` — Runs dbt: staging views → intermediate spatial joins (point-in-polygon) → mart tables with monthly aggregates
4. `npm run start` — Express serves the dashboard and JSON APIs

Or run the full pipeline: `npm run pipeline`

## Setup

Requires PostgreSQL with PostGIS and Python 3 (for dbt).

```
npm install
pip install -r dbt/requirements.txt
```

Set `DATABASE_URL` in your environment (e.g. `postgresql://user:pass@localhost:5432/pdx_crime`).

## Dashboard Features

- Offense trends over time with month-over-month and year-over-year KPIs
- Category and neighborhood breakdowns (click to filter)
- Temperature and precipitation correlation analysis (Pearson r)
- Seasonality heatmap
- Demographic bubble chart (poverty rate vs offenses per 1k)
- Choropleth map with quantile color breaks by ZCTA
