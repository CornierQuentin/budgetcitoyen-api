"""Endpoints relatifs aux recettes fiscales de l'Etat."""

from fastapi import APIRouter

from api.db.deps import DbSession
from api.models.recette import TypeRecette
from api.schemas.recette import RecetteResponse
from api.services import recette_service

router = APIRouter(prefix="/recettes", tags=["recettes"])


@router.get("/historique", response_model=list[RecetteResponse])
async def historique(
    db: DbSession,
    de: int | None = None,
    a: int | None = None,
    type: TypeRecette | None = None,
) -> list[RecetteResponse]:
    recettes = await recette_service.lister_historique_recettes(db, de, a, type)
    return [RecetteResponse.model_validate(r) for r in recettes]


@router.get("/{annee}", response_model=list[RecetteResponse])
async def obtenir_recettes(annee: int, db: DbSession) -> list[RecetteResponse]:
    recettes = await recette_service.lister_recettes(db, annee)
    return [RecetteResponse.model_validate(r) for r in recettes]
