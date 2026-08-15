"""Logique metier liee aux recettes fiscales de l'Etat."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.core.errors import ProblemDetailException
from api.models.recette import Recette, TypeRecette


async def lister_recettes(db: AsyncSession, annee: int) -> list[Recette]:
    """Retourne les recettes fiscales d'une annee donnee, ou 404 si aucune n'existe."""
    result = await db.execute(select(Recette).where(Recette.annee == annee))
    recettes = list(result.scalars().all())
    if not recettes:
        raise ProblemDetailException(
            title="Recettes introuvables",
            status=404,
            detail=f"Aucune recette fiscale disponible pour l'annee {annee}.",
        )
    return recettes


async def lister_historique_recettes(
    db: AsyncSession,
    de: int | None,
    a: int | None,
    type_recette: TypeRecette | None,
) -> list[Recette]:
    """Retourne l'historique des recettes fiscales, filtre par plage d'annees et type."""
    stmt = select(Recette).order_by(Recette.annee)
    if de is not None:
        stmt = stmt.where(Recette.annee >= de)
    if a is not None:
        stmt = stmt.where(Recette.annee <= a)
    if type_recette is not None:
        stmt = stmt.where(Recette.type == type_recette)
    result = await db.execute(stmt)
    return list(result.scalars().all())
