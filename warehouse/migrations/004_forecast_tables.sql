CREATE TABLE IF NOT EXISTS mart.mart_emissions_forecast (
    country_code     TEXT NOT NULL,
    year             INTEGER NOT NULL,
    forecast_tonnes  DOUBLE PRECISION NOT NULL,
    lower_band       DOUBLE PRECISION NOT NULL,
    upper_band       DOUBLE PRECISION NOT NULL,
    model            TEXT NOT NULL,
    _generated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    _source_file     TEXT NOT NULL,
    PRIMARY KEY (country_code, year, model)
);

CREATE TABLE IF NOT EXISTS mart.mart_emissions_anomalies (
    country_code            TEXT NOT NULL,
    year                    INTEGER NOT NULL,
    total_emissions_tonnes  DOUBLE PRECISION NOT NULL,
    yoy_pct                 DOUBLE PRECISION,
    z_score                 DOUBLE PRECISION,
    severity                TEXT NOT NULL,
    _generated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (country_code, year)
);

CREATE INDEX IF NOT EXISTS ix_forecast_country ON mart.mart_emissions_forecast (country_code);
CREATE INDEX IF NOT EXISTS ix_anomalies_severity ON mart.mart_emissions_anomalies (severity);
