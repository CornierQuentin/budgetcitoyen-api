"""Endpoints relatifs aux marches publics (DECP)."""

from datetime import date
from math import ceil

from fastapi import APIRouter, Query

from api.core.cpv import CPV_DIVISION_LABELS
from api.db.deps import DbSession
from api.schemas.marche import (
    MarchePublicResponse,
    MarchesBornesResponse,
    MarchesCpvRepartitionItem,
    MarchesPageResponse,
)
from api.services import marche_service

router = APIRouter(prefix="/marches", tags=["marches"])


@router.get("", response_model=MarchesPageResponse)
async def lister_marches(
    db: DbSession,
    q: str | None = None,
    date_debut: date | None = None,
    date_fin: date | None = None,
    montant_min: float | None = None,
    montant_max: float | None = None,
    cpv_division: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> MarchesPageResponse:
    items, total = await marche_service.lister_marches(
        db,
        q=q,
        date_debut=date_debut,
        date_fin=date_fin,
        montant_min=montant_min,
        montant_max=montant_max,
        cpv_division=cpv_division,
        page=page,
        page_size=page_size,
    )
    return MarchesPageResponse(
        items=[MarchePublicResponse.model_validate(m) for m in items],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=ceil(total / page_size) if total else 0,
    )


@router.get("/repartition-cpv", response_model=list[MarchesCpvRepartitionItem])
async def obtenir_repartition_cpv(
    db: DbSession,
    q: str | None = None,
    date_debut: date | None = None,
    date_fin: date | None = None,
    montant_min: float | None = None,
    montant_max: float | None = None,
) -> list[MarchesCpvRepartitionItem]:
    lignes = await marche_service.repartition_cpv(
        db,
        q=q,
        date_debut=date_debut,
        date_fin=date_fin,
        montant_min=montant_min,
        montant_max=montant_max,
    )
    return [
        MarchesCpvRepartitionItem(
            cpv_division=division,
            label=CPV_DIVISION_LABELS.get(division, division),
            montant_total=montant_total,
            nombre=nombre,
        )
        for division, montant_total, nombre in lignes
    ]


@router.get("/bornes", response_model=MarchesBornesResponse)
async def obtenir_bornes(db: DbSession) -> MarchesBornesResponse:
    date_min, date_max, montant_min, montant_max = await marche_service.bornes(db)
    return MarchesBornesResponse(
        date_min=date_min, date_max=date_max, montant_min=montant_min, montant_max=montant_max
    )
