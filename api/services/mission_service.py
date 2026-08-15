"""Logique metier liee aux missions, programmes et actions budgetaires."""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.core.errors import ProblemDetailException
from api.models.mission import Mission
from api.models.programme import Programme


async def lister_missions(db: AsyncSession, annee: int | None) -> list[Mission]:
    """Retourne la liste des missions, filtrees eventuellement par annee."""
    stmt = select(Mission).order_by(Mission.nom_officiel)
    if annee is not None:
        stmt = stmt.where(Mission.annee == annee)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def obtenir_mission(db: AsyncSession, slug: str, annee: int | None) -> Mission:
    """Retourne une mission par slug (et annee optionnelle), ou 404 si absente."""
    stmt = select(Mission).where(Mission.slug == slug)
    if annee is not None:
        stmt = stmt.where(Mission.annee == annee)
    stmt = stmt.order_by(Mission.annee.desc())
    result = await db.execute(stmt)
    mission = result.scalars().first()
    if mission is None:
        raise ProblemDetailException(
            title="Mission introuvable",
            status=404,
            detail=f"Aucune mission trouvee pour le slug '{slug}'.",
        )
    return mission


async def historique_mission(
    db: AsyncSession, slug: str, de: int | None, a: int | None
) -> list[Mission]:
    """Retourne l'historique d'une mission (par slug) sur une plage d'annees."""
    stmt = select(Mission).where(Mission.slug == slug).order_by(Mission.annee)
    if de is not None:
        stmt = stmt.where(Mission.annee >= de)
    if a is not None:
        stmt = stmt.where(Mission.annee <= a)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def obtenir_programme(db: AsyncSession, programme_id: int, annee: int | None) -> Programme:
    """Retourne un programme par id (et annee optionnelle), ou 404 si absent."""
    stmt = select(Programme).where(Programme.id == programme_id)
    if annee is not None:
        stmt = stmt.where(Programme.annee == annee)
    result = await db.execute(stmt)
    programme = result.scalar_one_or_none()
    if programme is None:
        raise ProblemDetailException(
            title="Programme introuvable",
            status=404,
            detail=f"Aucun programme trouve pour l'id {programme_id}.",
        )
    return programme
