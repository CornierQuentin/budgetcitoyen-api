"""Schema initial: annee_budget, mission, programme, action, depense, recette,
mission_alias, marche_public.

Revision ID: 0001
Revises:
Create Date: 2026-08-15

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "annee_budget",
        sa.Column("annee", sa.Integer(), nullable=False),
        sa.Column("depenses_nettes", sa.Numeric(15, 2), nullable=False),
        sa.Column("recettes_nettes", sa.Numeric(15, 2), nullable=False),
        sa.Column("deficit", sa.Numeric(15, 2), nullable=False),
        sa.Column("dette_pib", sa.Numeric(5, 2), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("annee", name="pk_annee_budget"),
    )

    op.create_table(
        "mission",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("slug", sa.String(255), nullable=False),
        sa.Column("nom_normalise", sa.String(255), nullable=False),
        sa.Column("nom_officiel", sa.String(255), nullable=False),
        sa.Column("annee", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_mission"),
        sa.UniqueConstraint("slug", "annee", name="uq_mission_slug"),
    )
    op.create_index("ix_mission_slug", "mission", ["slug"])

    op.create_table(
        "programme",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("mission_id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(50), nullable=False),
        sa.Column("nom", sa.String(255), nullable=False),
        sa.Column("annee", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_programme"),
        sa.ForeignKeyConstraint(
            ["mission_id"], ["mission.id"], name="fk_programme_mission_id_mission"
        ),
    )

    op.create_table(
        "action",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("programme_id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(50), nullable=False),
        sa.Column("nom", sa.String(255), nullable=False),
        sa.Column("annee", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_action"),
        sa.ForeignKeyConstraint(
            ["programme_id"], ["programme.id"], name="fk_action_programme_id_programme"
        ),
    )

    op.create_table(
        "depense",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("action_id", sa.Integer(), nullable=False),
        sa.Column("ae", sa.Numeric(15, 2), nullable=False),
        sa.Column("cp", sa.Numeric(15, 2), nullable=False),
        sa.Column("annee", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_depense"),
        sa.ForeignKeyConstraint(["action_id"], ["action.id"], name="fk_depense_action_id_action"),
    )

    op.create_table(
        "recette",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("annee", sa.Integer(), nullable=False),
        sa.Column(
            "type",
            sa.Enum("IR", "TVA", "IS", "TICPE", "AUTRES", name="type_recette"),
            nullable=False,
        ),
        sa.Column("montant_brut", sa.Numeric(15, 2), nullable=False),
        sa.Column("montant_net", sa.Numeric(15, 2), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_recette"),
        sa.UniqueConstraint("annee", "type", name="uq_recette_annee"),
    )

    op.create_table(
        "mission_alias",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("nom_csv", sa.String(255), nullable=False),
        sa.Column("mission_id", sa.Integer(), nullable=False),
        sa.Column("annee_debut", sa.Integer(), nullable=False),
        sa.Column("annee_fin", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id", name="pk_mission_alias"),
        sa.ForeignKeyConstraint(
            ["mission_id"], ["mission.id"], name="fk_mission_alias_mission_id_mission"
        ),
    )

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


def downgrade() -> None:
    op.drop_table("marche_public")
    op.drop_table("mission_alias")
    op.drop_table("recette")
    op.drop_table("depense")
    op.drop_table("action")
    op.drop_table("programme")
    op.drop_index("ix_mission_slug", table_name="mission")
    op.drop_table("mission")
    op.drop_table("annee_budget")
