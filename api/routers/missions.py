"""Endpoints relatifs aux missions, programmes et actions budgetaires."""

from fastapi import APIRouter

from api.db.deps import DbSession
from api.schemas.mission import MissionHistoriqueItem, MissionResponse, ProgrammeResponse
from api.services import mission_service

router = APIRouter(tags=["missions"])


@router.get("/missions", response_model=list[MissionResponse])
async def lister_missions(db: DbSession, annee: int | None = None) -> list[MissionResponse]:
    missions = await mission_service.lister_missions(db, annee)
    return [MissionResponse.model_validate(m) for m in missions]


@router.get("/missions/{slug}/historique", response_model=list[MissionHistoriqueItem])
async def historique_mission(
    slug: str,
    db: DbSession,
    de: int | None = None,
    a: int | None = None,
) -> list[MissionHistoriqueItem]:
    missions = await mission_service.historique_mission(db, slug, de, a)
    return [MissionHistoriqueItem.model_validate(m) for m in missions]


@router.get("/missions/{slug}", response_model=MissionResponse)
async def obtenir_mission(slug: str, db: DbSession, annee: int | None = None) -> MissionResponse:
    mission = await mission_service.obtenir_mission(db, slug, annee)
    return MissionResponse.model_validate(mission)


@router.get("/programmes/{id}", response_model=ProgrammeResponse)
async def obtenir_programme(id: int, db: DbSession, annee: int | None = None) -> ProgrammeResponse:
    programme = await mission_service.obtenir_programme(db, id, annee)
    return ProgrammeResponse.model_validate(programme)
