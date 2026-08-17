"""Endpoints relatifs aux indicateurs macro-economiques (PIB, population)."""

from fastapi import APIRouter

from api.db.deps import DbSession
from api.schemas.indicateur import IndicateurMacroResponse
from api.services import indicateur_service

router = APIRouter(prefix="/indicateurs", tags=["indicateurs"])


@router.get("/historique", response_model=list[IndicateurMacroResponse])
async def historique(
    db: DbSession,
    de: int | None = None,
    a: int | None = None,
) -> list[IndicateurMacroResponse]:
    indicateurs = await indicateur_service.lister_historique(db, de, a)
    return [IndicateurMacroResponse.model_validate(item) for item in indicateurs]


@router.get("/{annee}", response_model=IndicateurMacroResponse)
async def obtenir_indicateur(annee: int, db: DbSession) -> IndicateurMacroResponse:
    indicateur = await indicateur_service.obtenir_indicateur(db, annee)
    return IndicateurMacroResponse.model_validate(indicateur)
