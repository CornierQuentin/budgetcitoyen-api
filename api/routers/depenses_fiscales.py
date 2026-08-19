"""Endpoints relatifs aux depenses fiscales (niches fiscales)."""

from fastapi import APIRouter

from api.db.deps import DbSession
from api.schemas.depense_fiscale import DepenseFiscaleResponse
from api.services import depense_fiscale_service

router = APIRouter(prefix="/depenses-fiscales", tags=["depenses-fiscales"])


@router.get("/annees", response_model=list[int])
async def lister_annees(db: DbSession) -> list[int]:
    return await depense_fiscale_service.lister_annees(db)


@router.get("/{annee}", response_model=list[DepenseFiscaleResponse])
async def obtenir_depenses_fiscales(annee: int, db: DbSession) -> list[DepenseFiscaleResponse]:
    depenses_fiscales = await depense_fiscale_service.lister_depenses_fiscales(db, annee)
    return [DepenseFiscaleResponse.model_validate(d) for d in depenses_fiscales]
