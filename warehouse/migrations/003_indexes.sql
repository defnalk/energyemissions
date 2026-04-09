CREATE INDEX IF NOT EXISTS ix_emissions_year ON raw.emissions (year);
CREATE INDEX IF NOT EXISTS ix_emissions_inst ON raw.emissions (installation_id);
CREATE INDEX IF NOT EXISTS ix_allowances_year ON raw.allowances (year);
CREATE INDEX IF NOT EXISTS ix_allowances_inst ON raw.allowances (installation_id);
CREATE INDEX IF NOT EXISTS ix_installations_country ON raw.installations (country_code);
CREATE INDEX IF NOT EXISTS ix_installations_sector ON raw.installations (sector);
