"""Cron schedules for the energy pipeline flow."""

from __future__ import annotations

from prefect.client.schemas.schedules import CronSchedule

WEEKLY_MONDAY_6AM_UTC = CronSchedule(cron="0 6 * * 1", timezone="UTC")
