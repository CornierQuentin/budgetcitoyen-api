"""Reconstruit marche_public (donnees essentielles de la commande publique).

Le stub initial (6 colonnes: acheteur_siret/titulaire/objet/montant/
date_signature) n'a jamais ete peuple par aucun ETL - rien a preserver, cette
migration DROP puis CREATE plutot qu'une sequence d'ALTER. Ajoute l'extension
`pg_trgm` (recherche texte sur `objet_recherche` a 689k lignes) - premiere
extension Postgres utilisee dans ce projet, contrib standard installable sans
droits superuser sur la quasi-totalite des Postgres manages.

Revision ID: 0006
Revises: 0005
Create Date: 2026-08-19

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    op.drop_table("marche_public")

    op.create_table(
        "marche_public",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("marche_id_source", sa.String(64), nullable=False),
        sa.Column("nature", sa.String(), nullable=True),
        sa.Column("objet", sa.Text(), nullable=False),
        sa.Column("objet_recherche", sa.Text(), nullable=False),
        sa.Column("codecpv", sa.String(16), nullable=False),
        sa.Column("codecpv_division", sa.String(2), nullable=False),
        sa.Column("procedure", sa.String(), nullable=True),
        sa.Column("acheteur_siret", sa.String(20), nullable=False),
        sa.Column("titulaire_siret", sa.String(64), nullable=False),
        sa.Column("titulaire_id_type", sa.String(), nullable=True),
        sa.Column("dureemois", sa.Integer(), nullable=True),
        sa.Column("datenotification", sa.Date(), nullable=False),
        sa.Column("datepublicationdonnees", sa.Date(), nullable=True),
        sa.Column("montant", sa.Numeric(15, 2), nullable=False),
        sa.Column("formeprix", sa.String(), nullable=True),
        sa.Column("offresrecues", sa.Integer(), nullable=True),
        sa.Column("marcheinnovant", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_marche_public"),
    )
    op.create_index("ix_marche_public_marche_id_source", "marche_public", ["marche_id_source"])
    op.create_index("ix_marche_public_codecpv_division", "marche_public", ["codecpv_division"])
    op.create_index("ix_marche_public_acheteur_siret", "marche_public", ["acheteur_siret"])
    op.create_index("ix_marche_public_datenotification", "marche_public", ["datenotification"])
    op.create_index("ix_marche_public_montant", "marche_public", ["montant"])
    op.create_index(
        "ix_marche_public_objet_recherche_trgm",
        "marche_public",
        ["objet_recherche"],
        postgresql_using="gin",
        postgresql_ops={"objet_recherche": "gin_trgm_ops"},
    )


def downgrade() -> None:
    op.drop_table("marche_public")
    # `pg_trgm` n'est PAS supprimee ici: une future fonctionnalite pourrait
    # deja en dependre, la dropper au downgrade serait dangereux si partagee.
    op.create_table(
        "marche_public",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("acheteur_siret", sa.String(14), nullable=False),
        sa.Column("titulaire", sa.String(255), nullable=False),
        sa.Column("objet", sa.Text(), nullable=False),
        sa.Column("montant", sa.Numeric(15, 2), nullable=False),
        sa.Column("date_signature", sa.Date(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_marche_public"),
    )
