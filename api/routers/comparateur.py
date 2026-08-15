"""Endpoint du comparateur d'annees budgetaires."""

from fastapi import APIRouter

from api.db.deps import DbSession
from api.schemas.comparateur import ComparateurResponse
from api.services import comparateur_service

router = APIRouter(prefix="/comparateur", tags=["comparateur"])


@router.get("", response_model=ComparateurResponse)
async def comparer(annee_a: int, annee_b: int, db: DbSession) -> ComparateurResponse:
    return await comparateur_service.comparer_annees(db, annee_a, annee_b)
