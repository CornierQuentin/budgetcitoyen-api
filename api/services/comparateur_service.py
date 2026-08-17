"""Logique metier du comparateur d'annees budgetaires."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.recette import Recette, TypeRecette
from api.schemas.budget import AnneeBudgetResponse
from api.schemas.comparateur import ComparateurResponse, MissionDeltaItem, RecetteDeltaItem
from api.services.budget_service import obtenir_annee
from api.services.mission_service import totaux_depenses_par_mission


def _delta_relatif_pct(montant_a: float, montant_b: float) -> float | None:
    """Variation relative en % de montant_a vers montant_b, None si montant_a == 0."""
    if montant_a == 0:
        return None
    return (montant_b - montant_a) / montant_a * 100


async def _comparer_missions(
    db: AsyncSession, annee_a: int, annee_b: int
) -> list[MissionDeltaItem]:
    """Compare les depenses (CP) par mission entre deux annees, appariees par slug."""
    totaux_a = await totaux_depenses_par_mission(db, annee_a)
    totaux_b = await totaux_depenses_par_mission(db, annee_b)

    items: list[MissionDeltaItem] = []
    for slug in set(totaux_a) | set(totaux_b):
        nom_a, montant_a = totaux_a.get(slug, (None, 0.0))
        nom_b, montant_b = totaux_b.get(slug, (None, 0.0))
        nom = nom_b or nom_a or slug
        items.append(
            MissionDeltaItem(
                slug=slug,
                nom=nom,
                montant_a=montant_a,
                montant_b=montant_b,
                delta_absolu=montant_b - montant_a,
                delta_relatif_pct=_delta_relatif_pct(montant_a, montant_b),
            )
        )

    items.sort(key=lambda item: item.delta_absolu, reverse=True)
    return items


async def _comparer_recettes(
    db: AsyncSession, annee_a: int, annee_b: int
) -> list[RecetteDeltaItem]:
    """Compare les recettes fiscales nettes par type entre deux annees.

    Les recettes n'etant ingerees que pour un sous-ensemble d'annees (2024-2025
    au moment de cette ecriture), l'absence de donnees pour l'une des deux
    annees se traduit par des champs a None sur la ligne concernee, jamais par
    une erreur: le comparateur reste utilisable meme si une seule des deux
    annees dispose de recettes.
    """
    stmt = select(Recette).where(Recette.annee.in_([annee_a, annee_b]))
    result = await db.execute(stmt)
    recettes = list(result.scalars().all())

    montants_a: dict[TypeRecette, float] = {}
    montants_b: dict[TypeRecette, float] = {}
    for recette in recettes:
        if recette.annee == annee_a:
            montants_a[recette.type] = float(recette.montant_net)
        if recette.annee == annee_b:
            montants_b[recette.type] = float(recette.montant_net)

    items: list[RecetteDeltaItem] = []
    for type_recette in TypeRecette:
        montant_a = montants_a.get(type_recette)
        montant_b = montants_b.get(type_recette)
        delta_absolu: float | None = None
        delta_relatif_pct: float | None = None
        if montant_a is not None and montant_b is not None:
            delta_absolu = montant_b - montant_a
            delta_relatif_pct = _delta_relatif_pct(montant_a, montant_b)
        items.append(
            RecetteDeltaItem(
                type=type_recette,
                montant_a=montant_a,
                montant_b=montant_b,
                delta_absolu=delta_absolu,
                delta_relatif_pct=delta_relatif_pct,
            )
        )
    return items


async def comparer_annees(db: AsyncSession, annee_a: int, annee_b: int) -> ComparateurResponse:
    """Compare les agregats budgetaires de deux annees, par mission et par type de recette."""
    budget_a = await obtenir_annee(db, annee_a)
    budget_b = await obtenir_annee(db, annee_b)

    missions = await _comparer_missions(db, annee_a, annee_b)
    recettes = await _comparer_recettes(db, annee_a, annee_b)

    return ComparateurResponse(
        annee_a=AnneeBudgetResponse.model_validate(budget_a),
        annee_b=AnneeBudgetResponse.model_validate(budget_b),
        ecart_depenses=float(budget_b.depenses_nettes) - float(budget_a.depenses_nettes),
        ecart_recettes=float(budget_b.recettes_nettes) - float(budget_a.recettes_nettes),
        ecart_deficit=float(budget_b.deficit) - float(budget_a.deficit),
        missions=missions,
        recettes=recettes,
    )
