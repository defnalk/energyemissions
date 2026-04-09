# Data Dictionary

## `mart.mart_country_emissions`

| column | type | description |
|---|---|---|
| country_code | text | ISO 3166-1 alpha-2 country code. |
| country_name | text | Human-readable country name. |
| population | bigint | Country population (lookup). |
| year | int | Reporting year. |
| total_emissions_tonnes | double | Sum of verified emissions, tonnes CO₂e. |
| installation_count | int | Distinct installations contributing in the year. |
| emissions_per_capita | double | total_emissions_tonnes ÷ population. |
| yoy_pct_change | double | Year-over-year % change in emissions. |

## `mart.mart_sector_trends`

| column | type | description |
|---|---|---|
| sector | text | Industrial sector classification. |
| country_code | text | ISO country code. |
| year | int | Reporting year. |
| total_emissions_tonnes | double | Sector total, tonnes CO₂e. |
| installation_count | int | Distinct installations. |

## `mart.mart_top_emitters`

| column | type | description |
|---|---|---|
| installation_id | text | EUTL installation ID. |
| installation_name | text | Display name (encoding-fixed). |
| country_code | text | ISO country code. |
| sector | text | Sector classification. |
| year | int | Reporting year. |
| total_emissions_tonnes | double | Verified emissions for the year. |
| rank_in_year | int | 1 = largest emitter that year (top 100 retained). |

## `mart.mart_compliance_gap`

| column | type | description |
|---|---|---|
| country_code | text | ISO country code. |
| year | int | Reporting year. |
| allocated_tonnes | double | Sum of free allowances allocated. |
| surrendered_tonnes | double | Sum surrendered for compliance. |
| verified_tonnes | double | Sum of verified emissions. |
| compliance_gap_tonnes | double | allocated − verified. Positive = surplus. |
