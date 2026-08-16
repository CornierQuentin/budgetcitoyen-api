"""Chargement (upsert) des donnees normalisees en base.

`upsert_missions` et `upsert_recettes` s'appuient sur les contraintes
d'unicite existantes (`mission(slug, annee)`, `recette(annee, type)`) via
`INSERT ... ON CONFLICT DO UPDATE`, ce qui les rend idempotents nativement.

`programme`, `action` et `depense` n'ont pas de contrainte d'unicite dans le
schema actuel (granularite fine geree par la cle primaire uniquement): pour
rester idempotent sur des relances repetees, `upsert_depenses` recharge une
annee dans son integralite (delete puis insert des lignes `annee = :annee`)
plutot que de tenter un upsert ligne a ligne sans cle stable.
"""

import logging
from collections.abc import Sequence

from sqlalchemy import delete, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from api.etl.normalize import DepenseAggregat, MissionAliasRow, MissionYearRow, RecetteAggregat
from api.models.action import Action
from api.models.annee_budget import AnneeBudget
from api.models.depense import Depense
from api.models.mission import Mission
from api.models.mission_alias import MissionAlias
from api.models.programme import Programme
from api.models.recette import Recette

logger = logging.getLogger(__name__)


async def upsert_missions(
    db: AsyncSession, rows: Sequence[MissionYearRow]
) -> dict[tuple[str, int], int]:
    """Insere ou met a jour les missions, upsert idempotent sur (slug, annee).

    Retourne le mapping {(slug, annee): mission_id} pour les lignes fournies.
    """
    if not rows:
        return {}

    values = [
        {
            "slug": r.slug,
            "nom_normalise": r.nom_normalise,
            "nom_officiel": r.nom_officiel,
            "annee": r.annee,
            "code_mission": r.code_mission,
        }
        for r in rows
    ]
    insert_stmt = pg_insert(Mission).values(values)
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[Mission.slug, Mission.annee],
        set_={
            "nom_normalise": insert_stmt.excluded.nom_normalise,
            "nom_officiel": insert_stmt.excluded.nom_officiel,
            "code_mission": insert_stmt.excluded.code_mission,
        },
    ).returning(Mission.id, Mission.slug, Mission.annee)
    result = await db.execute(upsert_stmt)
    mapping = {(row.slug, row.annee): row.id for row in result}
    logger.info("missions upsertees: %d", len(mapping))
    return mapping


async def upsert_mission_aliases(
    db: AsyncSession, rows: Sequence[tuple[MissionAliasRow, int]]
) -> None:
    """Reconstruit integralement la table mission_alias a partir des alias fournis.

    `rows` contient des couples (alias, mission_id) deja resolus. La table
    est entierement recalculee a chaque run (pas de cle naturelle stable
    pour un upsert cible), ce qui reste idempotent puisqu'elle est purement
    derivee des donnees de depenses.
    """
    await db.execute(delete(MissionAlias))
    if not rows:
        return
    values = [
        {
            "nom_csv": alias.nom_csv,
            "mission_id": mission_id,
            "annee_debut": alias.annee_debut,
            "annee_fin": alias.annee_fin,
        }
        for alias, mission_id in rows
    ]
    await db.execute(insert(MissionAlias), values)
    logger.info("mission_alias reconstruits: %d", len(values))


async def upsert_depenses(
    db: AsyncSession,
    annee: int,
    aggregats: Sequence[tuple[DepenseAggregat, int]],
) -> int:
    """Recharge les programmes/actions/depenses d'une annee a partir des agregats.

    `aggregats` contient des couples (DepenseAggregat, mission_id) deja
    resolus. Supprime d'abord toutes les lignes `annee = :annee` existantes
    (depense puis action puis programme, dans cet ordre a cause des FK), puis
    reinsere les programmes, actions et depenses correspondants. Retourne le
    nombre de lignes `depense` inserees.
    """
    await db.execute(delete(Depense).where(Depense.annee == annee))
    await db.execute(delete(Action).where(Action.annee == annee))
    await db.execute(delete(Programme).where(Programme.annee == annee))

    if not aggregats:
        return 0

    programme_libelles: dict[tuple[int, str], str] = {}
    for agg, mission_id in aggregats:
        programme_libelles[(mission_id, agg.programme_code)] = agg.programme_libelle

    programme_values = [
        {"mission_id": mission_id, "code": code, "nom": nom, "annee": annee}
        for (mission_id, code), nom in programme_libelles.items()
    ]
    result = await db.execute(
        insert(Programme).returning(Programme.id, Programme.mission_id, Programme.code),
        programme_values,
    )
    programme_ids = {(row.mission_id, row.code): row.id for row in result}

    action_libelles: dict[tuple[int, str], str] = {}
    for agg, mission_id in aggregats:
        programme_id = programme_ids[(mission_id, agg.programme_code)]
        action_libelles[(programme_id, agg.action_code)] = agg.action_libelle

    action_values = [
        {"programme_id": programme_id, "code": code, "nom": nom, "annee": annee}
        for (programme_id, code), nom in action_libelles.items()
    ]
    result = await db.execute(
        insert(Action).returning(Action.id, Action.programme_id, Action.code),
        action_values,
    )
    action_ids = {(row.programme_id, row.code): row.id for row in result}

    depense_values = []
    for agg, mission_id in aggregats:
        programme_id = programme_ids[(mission_id, agg.programme_code)]
        action_id = action_ids[(programme_id, agg.action_code)]
        depense_values.append({"action_id": action_id, "ae": agg.ae, "cp": agg.cp, "annee": annee})

    await db.execute(insert(Depense), depense_values)
    logger.info(
        "annee %d: %d programmes, %d actions, %d depenses charges",
        annee,
        len(programme_values),
        len(action_values),
        len(depense_values),
    )
    return len(depense_values)


async def upsert_recettes(db: AsyncSession, aggregats: Sequence[RecetteAggregat]) -> None:
    """Insere ou met a jour les recettes, upsert idempotent sur (annee, type)."""
    if not aggregats:
        return
    values = [
        {
            "annee": a.annee,
            "type": a.type,
            "montant_brut": a.montant_brut,
            "montant_net": a.montant_net,
        }
        for a in aggregats
    ]
    stmt = pg_insert(Recette).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Recette.annee, Recette.type],
        set_={
            "montant_brut": stmt.excluded.montant_brut,
            "montant_net": stmt.excluded.montant_net,
        },
    )
    await db.execute(stmt)
    logger.info("recettes upsertees: %d", len(values))


async def recalculer_annee_budget(
    db: AsyncSession, annee: int, source_url: str
) -> AnneeBudget | None:
    """Recalcule l'agregat AnneeBudget d'une annee, si depenses ET recettes existent.

    - `depenses_nettes` = somme des `Depense.cp` (credits de paiement) de l'annee.
    - `recettes_nettes` = somme de TOUS les `Recette.montant_net` de l'annee,
      tous types confondus (IR/TVA/IS/TICPE/AUTRES).
    - `deficit` = depenses_nettes - recettes_nettes: c'est le deficit
      budgetaire de l'Etat (recettes - depenses du budget general), PAS le
      deficit "Maastricht" au sens INSEE (perimetre plus large incluant les
      administrations publiques locales et de securite sociale).
    - `dette_pib` reste a None: hors perimetre de cette passe d'ingestion.

    Retourne None (et ne cree/modifie rien) si l'annee n'a pas encore a la
    fois des depenses et des recettes chargees.
    """
    depenses_count = await db.scalar(
        select(func.count()).select_from(Depense).where(Depense.annee == annee)
    )
    recettes_count = await db.scalar(
        select(func.count()).select_from(Recette).where(Recette.annee == annee)
    )
    if not depenses_count or not recettes_count:
        logger.info(
            "annee %d: agregat annee_budget non calcule (depenses=%d, recettes=%d)",
            annee,
            depenses_count or 0,
            recettes_count or 0,
        )
        return None

    depenses_total = float(
        await db.scalar(
            select(func.coalesce(func.sum(Depense.cp), 0)).where(Depense.annee == annee)
        )
    )
    recettes_total = float(
        await db.scalar(
            select(func.coalesce(func.sum(Recette.montant_net), 0)).where(Recette.annee == annee)
        )
    )
    deficit = depenses_total - recettes_total

    stmt = pg_insert(AnneeBudget).values(
        annee=annee,
        depenses_nettes=depenses_total,
        recettes_nettes=recettes_total,
        deficit=deficit,
        dette_pib=None,
        source_url=source_url,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[AnneeBudget.annee],
        set_={
            "depenses_nettes": stmt.excluded.depenses_nettes,
            "recettes_nettes": stmt.excluded.recettes_nettes,
            "deficit": stmt.excluded.deficit,
            "source_url": stmt.excluded.source_url,
            # dette_pib n'est volontairement pas ecrase: hors perimetre ici,
            # une valeur renseignee par un futur backfill ne doit pas etre
            # effacee par une relance de cette passe.
        },
    )
    await db.execute(stmt)
    result = await db.execute(select(AnneeBudget).where(AnneeBudget.annee == annee))
    annee_budget = result.scalar_one()
    logger.info(
        "annee %d: annee_budget recalcule (depenses=%.0f, recettes=%.0f, deficit=%.0f)",
        annee,
        depenses_total,
        recettes_total,
        deficit,
    )
    return annee_budget
