"""Generate a synthetic but realistic EU ETS sample dataset.

Real EUTL data is published by the European Environment Agency at
https://www.eea.europa.eu/en/datahub/datahubitem-view/9d04d6c1-d8cf-44ff-aa5e-c1c5f8c7c9de
but the URL is unstable. This module produces a deterministic ~50k-row
fixture matching the EUTL schema for installations, emissions, and allowances.
"""

from __future__ import annotations

import csv
import random
from pathlib import Path

SECTORS = [
    "Power & Heat",
    "Cement",
    "Iron & Steel",
    "Refineries",
    "Chemicals",
    "Pulp & Paper",
    "Aviation",
    "Glass",
    "Ceramics",
]
COUNTRIES = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR",
    "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL",
    "PL", "PT", "RO", "SK", "SI", "ES", "SE", "NO", "IS", "LI",
]
YEARS = list(range(2013, 2024))


def generate(out_dir: Path, n_installations: int = 5000, seed: int = 42) -> None:
    """Generate installations, emissions, and allowances CSVs."""
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    installations: list[dict[str, object]] = []
    for i in range(n_installations):
        country = rng.choice(COUNTRIES)
        sector = rng.choice(SECTORS)
        installations.append(
            {
                "installation_id": f"EU{i:06d}",
                "name": f"{sector} Plant {i} – {country}",  # non-ASCII char
                "country_code": country,
                "sector": sector,
                "latitude": round(rng.uniform(35.0, 70.0), 4),
                "longitude": round(rng.uniform(-10.0, 30.0), 4),
            }
        )

    with (out_dir / "installations.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(installations[0].keys()))
        writer.writeheader()
        writer.writerows(installations)

    with (out_dir / "emissions.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["installation_id", "year", "activity_type", "verified_tonnes", "reporting_date"]
        )
        for inst in installations:
            base = rng.uniform(5_000, 2_000_000)
            for year in YEARS:
                drift = 1.0 - (year - 2013) * 0.015 + rng.uniform(-0.1, 0.1)
                writer.writerow(
                    [
                        inst["installation_id"],
                        year,
                        "combustion",
                        round(max(0.0, base * drift), 2),
                        f"{year + 1}-03-31",
                    ]
                )

    with (out_dir / "allowances.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["installation_id", "year", "allocated_tonnes", "surrendered_tonnes"]
        )
        for inst in installations:
            base = rng.uniform(5_000, 2_000_000)
            for year in YEARS:
                allocated = round(base * rng.uniform(0.7, 1.1), 2)
                surrendered = round(allocated * rng.uniform(0.85, 1.15), 2)
                writer.writerow([inst["installation_id"], year, allocated, surrendered])


if __name__ == "__main__":
    generate(Path(__file__).parent / "data")
    print("Sample data generated.")
