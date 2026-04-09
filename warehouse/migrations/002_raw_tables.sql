CREATE TABLE IF NOT EXISTS raw.installations (
    installation_id   TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    country_code      TEXT NOT NULL,
    sector            TEXT NOT NULL,
    latitude          DOUBLE PRECISION,
    longitude         DOUBLE PRECISION,
    _loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    _source_file      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.emissions (
    installation_id   TEXT NOT NULL,
    year              INTEGER NOT NULL,
    activity_type     TEXT NOT NULL,
    verified_tonnes   DOUBLE PRECISION NOT NULL,
    reporting_date    DATE,
    _loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    _source_file      TEXT NOT NULL,
    PRIMARY KEY (installation_id, year, activity_type)
);

CREATE TABLE IF NOT EXISTS raw.allowances (
    installation_id   TEXT NOT NULL,
    year              INTEGER NOT NULL,
    allocated_tonnes  DOUBLE PRECISION NOT NULL,
    surrendered_tonnes DOUBLE PRECISION NOT NULL,
    _loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    _source_file      TEXT NOT NULL,
    PRIMARY KEY (installation_id, year)
);

CREATE TABLE IF NOT EXISTS raw.country_codes (
    country_code  TEXT PRIMARY KEY,
    country_name  TEXT NOT NULL,
    population    BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.rejected_rows (
    id           BIGSERIAL PRIMARY KEY,
    source_table TEXT NOT NULL,
    payload      JSONB NOT NULL,
    error        TEXT NOT NULL,
    _loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
