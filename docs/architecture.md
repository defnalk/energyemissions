# Architecture

```mermaid
flowchart LR
    A[EU ETS CSV<br/>EEA portal or local fallback] --> B[ingest.sources]
    B --> C[Pandera validation<br/>ingest.schemas]
    C --> D[transform.clean<br/>encoding · units · dedup]
    D --> E[(Postgres raw)]
    E --> F[dbt staging]
    F --> G[dbt intermediate]
    G --> H[dbt marts]
    H --> I[Streamlit dashboard]
    H --> J[Analyst SQL]
    K[Prefect flow] -. orchestrates .-> B & C & D & E & F & G & H
```

The pipeline is a single Prefect flow (`orchestration.flows.energy_pipeline`)
that fans out per source table, validates each row with Pandera, applies
Python-side cleaning, bulk-COPYs into the `raw` schema, then hands off to dbt
for staging → intermediate → marts. Streamlit reads exclusively from `mart.*`.
