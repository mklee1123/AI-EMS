"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-04-16
"""
from __future__ import annotations

from pathlib import Path

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

_SCHEMA = Path(__file__).resolve().parents[3] / "db" / "schema.sql"


def upgrade() -> None:
    bind = op.get_bind()
    cur = bind.connection.cursor()
    cur.execute(_SCHEMA.read_text(encoding="utf-8"))
    bind.connection.commit()


def downgrade() -> None:
    bind = op.get_bind()
    cur = bind.connection.cursor()
    cur.execute("""
        DROP FUNCTION IF EXISTS cleanup_old_data();
        DROP TABLE IF EXISTS device_status_cache;
        DROP TABLE IF EXISTS daily_summary;
        DROP TABLE IF EXISTS temp_hourly;
        DROP TABLE IF EXISTS temp_1min;
        DROP TABLE IF EXISTS readings;
        DROP TABLE IF EXISTS devices;
    """)
    bind.connection.commit()
