"""Ajoute depense_fiscale (niches fiscales, annexe "Voies et moyens" Tome II
du PLF), domaine independant de missions/depenses/recettes.

Revision ID: 0005
Revises: 0004
Create Date: 2026-08-19

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "depense_fiscale",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("annee", sa.Integer(), nullable=False),
        sa.Column("numero", sa.String(), nullable=False),
        sa.Column("categorie", sa.String(), nullable=False),
        sa.Column("sous_categorie", sa.String(), nullable=False),
        sa.Column("sous_sous_categorie", sa.String(), nullable=True),
        sa.Column("libelle", sa.String(), nullable=False),
        sa.Column("beneficiaire", sa.String(), nullable=False),
        sa.Column("montant_millions", sa.Numeric(12, 1), nullable=True),
        sa.Column(
            "statut_montant",
            sa.Enum(
                "chiffre",
                "epsilon",
                "non_calculable",
                "aucun_effet",
                name="statut_montant_depense_fiscale",
            ),
            nullable=False,
        ),
        sa.Column("methode_chiffrage", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_depense_fiscale"),
    )
    op.create_index("ix_depense_fiscale_annee", "depense_fiscale", ["annee"])
    op.create_index("ix_depense_fiscale_numero", "depense_fiscale", ["numero"])


def downgrade() -> None:
    op.drop_index("ix_depense_fiscale_numero", table_name="depense_fiscale")
    op.drop_index("ix_depense_fiscale_annee", table_name="depense_fiscale")
    op.drop_table("depense_fiscale")
    sa.Enum(name="statut_montant_depense_fiscale").drop(op.get_bind(), checkfirst=True)
