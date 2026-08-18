"""Ajoute ingestion_log (derniere_ingestion pour GET /health, section 4.2).

Revision ID: 0004
Revises: 0003
Create Date: 2026-08-18

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ingestion_log",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("termine_a", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_ingestion_log"),
    )


def downgrade() -> None:
    op.drop_table("ingestion_log")
