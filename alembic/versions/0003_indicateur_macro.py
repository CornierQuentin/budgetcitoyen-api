"""Ajoute indicateur_macro (PIB nominal, population), pour les indicateurs
"par habitant" du frontend.

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-17

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "indicateur_macro",
        sa.Column("annee", sa.Integer(), nullable=False),
        sa.Column("pib_courant", sa.Numeric(15, 2), nullable=True),
        sa.Column("population", sa.BigInteger(), nullable=True),
        sa.Column("source_pib_url", sa.Text(), nullable=True),
        sa.Column("source_population_url", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("annee", name="pk_indicateur_macro"),
    )


def downgrade() -> None:
    op.drop_table("indicateur_macro")
