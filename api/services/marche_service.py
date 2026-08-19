"""Logique metier liee aux marches publics (DECP)."""

from datetime import date
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from api.core.text import normaliser_pour_recherche
from api.models.marche_public import MarchePublic


def _appliquer_filtres(
    stmt: Select[Any],
    *,
    q: str | None,
    date_debut: date | None,
    date_fin: date | None,
    montant_min: float | None,
    montant_max: float | None,
    cpv_division: str | None,
) -> Select[Any]:
    """Construit les clauses WHERE communes a lister_marches et
    repartition_cpv, pour que les deux ne divergent jamais silencieusement."""
    if q:
        stmt = stmt.where(MarchePublic.objet_recherche.ilike(f"%{normaliser_pour_recherche(q)}%"))
    if date_debut is not None:
        stmt = stmt.where(MarchePublic.datenotification >= date_debut)
    if date_fin is not None:
        stmt = stmt.where(MarchePublic.datenotification <= date_fin)
    if montant_min is not None:
        stmt = stmt.where(MarchePublic.montant >= montant_min)
    if montant_max is not None:
        stmt = stmt.where(MarchePublic.montant <= montant_max)
    if cpv_division:
        stmt = stmt.where(MarchePublic.codecpv_division == cpv_division)
    return stmt


async def lister_marches(
    db: AsyncSession,
    *,
    q: str | None = None,
    date_debut: date | None = None,
    date_fin: date | None = None,
    montant_min: float | None = None,
    montant_max: float | None = None,
    cpv_division: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[MarchePublic], int]:
    """Retourne (lignes de la page, total correspondant aux filtres).

    A la difference de lister_depenses_fiscales, ne leve JAMAIS 404: une
    liste vide (recherche sans correspondance, filtre trop restrictif, page
    au-dela du total) est un etat normal pour une liste paginee/filtrable,
    pas une absence de donnees - retourne toujours des resultats (eventuellement
    vides), a l'appelant (le router) de repondre 200.
    """
    base = _appliquer_filtres(
        select(MarchePublic),
        q=q,
        date_debut=date_debut,
        date_fin=date_fin,
        montant_min=montant_min,
        montant_max=montant_max,
        cpv_division=cpv_division,
    )

    total = (
        await db.execute(
            _appliquer_filtres(
                select(func.count()).select_from(MarchePublic),
                q=q,
                date_debut=date_debut,
                date_fin=date_fin,
                montant_min=montant_min,
                montant_max=montant_max,
                cpv_division=cpv_division,
            )
        )
    ).scalar_one()

    stmt = (
        base.order_by(MarchePublic.datenotification.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(stmt)
    return list(result.scalars().all()), total


async def repartition_cpv(
    db: AsyncSession,
    *,
    q: str | None = None,
    date_debut: date | None = None,
    date_fin: date | None = None,
    montant_min: float | None = None,
    montant_max: float | None = None,
) -> list[tuple[str, float, int]]:
    """GROUP BY codecpv_division cote serveur, sous les memes filtres que
    lister_marches (hors pagination) - jamais calcule cote frontend sur des
    lignes deja chargees (a la difference de DepensesFiscales.tsx, qui peut
    se le permettre avec seulement ~465 lignes tenant entierement en memoire)."""
    stmt = _appliquer_filtres(
        select(
            MarchePublic.codecpv_division,
            func.sum(MarchePublic.montant),
            func.count(),
        ).group_by(MarchePublic.codecpv_division),
        q=q,
        date_debut=date_debut,
        date_fin=date_fin,
        montant_min=montant_min,
        montant_max=montant_max,
        cpv_division=None,
    )
    result = await db.execute(stmt)
    return [
        (division, float(montant_total), nombre) for division, montant_total, nombre in result.all()
    ]


async def bornes(db: AsyncSession) -> tuple[date | None, date | None, float | None, float | None]:
    """MIN/MAX(datenotification), MIN/MAX(montant) sur l'ensemble de la table
    (sans filtre) - pour que le frontend calibre ses selecteurs de date/
    montant sans deviner."""
    result = await db.execute(
        select(
            func.min(MarchePublic.datenotification),
            func.max(MarchePublic.datenotification),
            func.min(MarchePublic.montant),
            func.max(MarchePublic.montant),
        )
    )
    date_min, date_max, montant_min, montant_max = result.one()
    return (
        date_min,
        date_max,
        float(montant_min) if montant_min is not None else None,
        float(montant_max) if montant_max is not None else None,
    )
