"""Support de l'ingestion ETL reelle: code_mission sur mission, dette_pib nullable.

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-16

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Code LOLF (2 lettres) de la mission: cle de rapprochement stable entre
    # annees, absente des formats source les plus anciens -> nullable.
    op.add_column("mission", sa.Column("code_mission", sa.String(10), nullable=True))
    op.create_index("ix_mission_code_mission", "mission", ["code_mission"])

    # Hors perimetre de cette passe d'ingestion (pas de donnees INSEE dette/PIB).
    op.alter_column("annee_budget", "dette_pib", existing_type=sa.Numeric(5, 2), nullable=True)


def downgrade() -> None:
    op.alter_column("annee_budget", "dette_pib", existing_type=sa.Numeric(5, 2), nullable=False)
    op.drop_index("ix_mission_code_mission", table_name="mission")
    op.drop_column("mission", "code_mission")
