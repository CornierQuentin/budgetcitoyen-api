"""Logique metier du comparateur d'annees budgetaires."""

from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas.budget import AnneeBudgetResponse
from api.schemas.comparateur import ComparateurResponse
from api.services.budget_service import obtenir_annee


async def comparer_annees(db: AsyncSession, annee_a: int, annee_b: int) -> ComparateurResponse:
    """Compare les agregats budgetaires de deux annees."""
    budget_a = await obtenir_annee(db, annee_a)
    budget_b = await obtenir_annee(db, annee_b)

    return ComparateurResponse(
        annee_a=AnneeBudgetResponse.model_validate(budget_a),
        annee_b=AnneeBudgetResponse.model_validate(budget_b),
        ecart_depenses=float(budget_b.depenses_nettes) - float(budget_a.depenses_nettes),
        ecart_recettes=float(budget_b.recettes_nettes) - float(budget_a.recettes_nettes),
        ecart_deficit=float(budget_b.deficit) - float(budget_a.deficit),
    )
