"""Logique metier liee aux depenses fiscales (niches fiscales)."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.core.errors import ProblemDetailException
from api.models.depense_fiscale import DepenseFiscale


async def lister_depenses_fiscales(db: AsyncSession, annee: int) -> list[DepenseFiscale]:
    """Retourne les mesures de depenses fiscales d'une annee, triees par montant
    chiffre decroissant (les mesures non chiffrables, `montant_millions` a
    `None`, sont placees en fin de liste par cette meme requete plutot que
    d'obliger le frontend a trier lui-meme), ou 404 si aucune n'existe.
    """
    stmt = (
        select(DepenseFiscale)
        .where(DepenseFiscale.annee == annee)
        .order_by(DepenseFiscale.montant_millions.is_(None), DepenseFiscale.montant_millions.desc())
    )
    result = await db.execute(stmt)
    depenses_fiscales = list(result.scalars().all())
    if not depenses_fiscales:
        raise ProblemDetailException(
            title="Depenses fiscales introuvables",
            status=404,
            detail=f"Aucune depense fiscale disponible pour l'annee {annee}.",
        )
    return depenses_fiscales


async def lister_annees(db: AsyncSession) -> list[int]:
    """Retourne la liste des annees disponibles, triees."""
    result = await db.execute(
        select(DepenseFiscale.annee).distinct().order_by(DepenseFiscale.annee)
    )
    return list(result.scalars().all())
