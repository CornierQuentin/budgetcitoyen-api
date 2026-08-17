"""Logique metier liee aux agregats budgetaires annuels."""

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.core.errors import ProblemDetailException
from api.models.annee_budget import AnneeBudget


async def lister_annees(db: AsyncSession) -> list[AnneeBudget]:
    """Retourne la liste des annees budgetaires disponibles, triees par annee."""
    result = await db.execute(select(AnneeBudget).order_by(AnneeBudget.annee))
    return list(result.scalars().all())


async def obtenir_annee(db: AsyncSession, annee: int) -> AnneeBudget:
    """Retourne l'agregat budgetaire d'une annee donnee, ou 404 si absent."""
    result = await db.execute(select(AnneeBudget).where(AnneeBudget.annee == annee))
    annee_budget = result.scalar_one_or_none()
    if annee_budget is None:
        raise ProblemDetailException(
            title="Annee budgetaire introuvable",
            status=404,
            detail=f"Aucune donnee budgetaire disponible pour l'annee {annee}.",
        )
    return annee_budget


async def obtenir_derniere_annee_disponible(db: AsyncSession) -> AnneeBudget:
    """Retourne l'agregat budgetaire de la derniere annee disponible en base.

    Utilise par le module Budget personnalise (Module 5) pour ventiler la
    contribution individuelle estimee au prorata des depenses reelles de la
    derniere annee ingeree.
    """
    result = await db.execute(select(func.max(AnneeBudget.annee)))
    derniere_annee = result.scalar_one_or_none()
    if derniere_annee is None:
        raise ProblemDetailException(
            title="Aucune donnee budgetaire disponible",
            status=404,
            detail="Aucune annee budgetaire n'est disponible en base.",
        )
    return await obtenir_annee(db, derniere_annee)


async def lister_historique(db: AsyncSession, de: int | None, a: int | None) -> list[AnneeBudget]:
    """Retourne l'historique des agregats budgetaires sur une plage d'annees."""
    stmt = select(AnneeBudget).order_by(AnneeBudget.annee)
    if de is not None:
        stmt = stmt.where(AnneeBudget.annee >= de)
    if a is not None:
        stmt = stmt.where(AnneeBudget.annee <= a)
    result = await db.execute(stmt)
    return list(result.scalars().all())
