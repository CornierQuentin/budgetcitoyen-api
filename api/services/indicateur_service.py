"""Logique metier liee aux indicateurs macro-economiques (PIB, population)."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.core.errors import ProblemDetailException
from api.models.indicateur_macro import IndicateurMacro


async def obtenir_indicateur(db: AsyncSession, annee: int) -> IndicateurMacro:
    """Retourne l'indicateur macro d'une annee donnee, ou 404 si absent."""
    result = await db.execute(select(IndicateurMacro).where(IndicateurMacro.annee == annee))
    indicateur = result.scalar_one_or_none()
    if indicateur is None:
        raise ProblemDetailException(
            title="Indicateur macro-economique introuvable",
            status=404,
            detail=f"Aucun indicateur macro-economique disponible pour l'annee {annee}.",
        )
    return indicateur


async def lister_historique(
    db: AsyncSession, de: int | None, a: int | None
) -> list[IndicateurMacro]:
    """Retourne l'historique des indicateurs macro sur une plage d'annees."""
    stmt = select(IndicateurMacro).order_by(IndicateurMacro.annee)
    if de is not None:
        stmt = stmt.where(IndicateurMacro.annee >= de)
    if a is not None:
        stmt = stmt.where(IndicateurMacro.annee <= a)
    result = await db.execute(stmt)
    return list(result.scalars().all())
