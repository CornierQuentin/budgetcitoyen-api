"""Endpoint de sante de l'API."""

from importlib.metadata import PackageNotFoundError, version

from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import select

from api.db.deps import DbSession
from api.models.ingestion_log import IngestionLog

router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: str
    version: str
    derniere_ingestion: str | None = None


def _get_version() -> str:
    try:
        return version("budgetcitoyen-api")
    except PackageNotFoundError:
        return "0.1.0"


@router.get("/health", response_model=HealthResponse)
async def health(db: DbSession) -> HealthResponse:
    derniere = await db.scalar(select(IngestionLog.termine_a).order_by(IngestionLog.id.desc()))
    return HealthResponse(
        status="ok",
        version=_get_version(),
        derniere_ingestion=derniere.isoformat() if derniere else None,
    )
