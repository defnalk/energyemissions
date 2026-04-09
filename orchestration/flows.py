"""Top-level Prefect flow: ingest → validate → clean → load → dbt build → notify."""

from __future__ import annotations

import argparse
import sys
import traceback

import structlog
from prefect import flow

from orchestration.alerts import send_alert
from orchestration.tasks import (
    clean_table,
    download_source,
    load_country_codes,
    load_to_raw,
    run_dbt,
    validate_raw,
)

log = structlog.get_logger(__name__)

TABLES = ("installations", "emissions", "allowances")


@flow(name="energy_pipeline", log_prints=True)
def energy_pipeline() -> dict[str, int]:
    """Run the full EU ETS pipeline end to end.

    Returns:
        Mapping of source table name to rows loaded.
    """
    try:
        load_country_codes()
        loaded: dict[str, int] = {}
        for name in TABLES:
            raw = download_source(name)
            validated = validate_raw(name, raw)
            cleaned = clean_table(name, validated)
            loaded[name] = load_to_raw(name, cleaned)

        run_dbt("deps")
        run_dbt("build --target dev")
        return loaded
    except Exception as exc:
        send_alert(
            subject="energy_pipeline FAILED",
            body=f"{exc}\n\n{traceback.format_exc()}",
        )
        raise


def main() -> int:
    """CLI entry point for ad-hoc and ``make seed`` invocations."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-once", action="store_true", help="Run the flow once and exit.")
    args = parser.parse_args()

    if args.run_once:
        result = energy_pipeline()
        print("loaded:", result)
        return 0
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
