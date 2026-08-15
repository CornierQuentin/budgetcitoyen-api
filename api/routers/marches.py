"""Endpoints relatifs aux marches publics (module V2, non implemente)."""

from fastapi import APIRouter

from api.core.errors import ProblemDetailException
from api.schemas.marche import MarchePublicResponse

router = APIRouter(prefix="/marches", tags=["marches"])


@router.get("", response_model=list[MarchePublicResponse])
async def lister_marches() -> list[MarchePublicResponse]:
    raise ProblemDetailException(
        title="Non implemente",
        status=501,
        detail="Module V2 non implemente",
    )
