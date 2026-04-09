# Runbook

## Backfill a historical year

```bash
docker compose exec prefect-worker python -m orchestration.flows --run-once
```

Re-running the flow truncates the `raw.*` tables and reloads from source.
dbt incremental marts are rebuilt as full tables, so a single run is enough.

## Add a new source

1. Add a download URL to `.env` and `ingest.sources.load_table`.
2. Add a Pandera schema in `ingest/schemas.py`.
3. Add a cleaning function in `transform/clean.py`.
4. Add a `raw.<name>` table in a new migration `004_*.sql`.
5. Add `stg_<name>.sql` and surface it in the marts.
6. Add a row to `_staging__sources.yml`.

## Debug a failed flow

1. Open the Prefect UI at <http://localhost:4200>, click the failed run.
2. Inspect logs by task — each task is tagged (`ingest`, `transform`,
   `warehouse`, `quality`).
3. Check `raw.rejected_rows` for quarantined data:
   `select * from raw.rejected_rows order by _loaded_at desc limit 100;`
4. Re-run a single task with `python -m orchestration.flows --run-once`.

## Recreate the warehouse from scratch

```bash
make down && make up && make seed
```
