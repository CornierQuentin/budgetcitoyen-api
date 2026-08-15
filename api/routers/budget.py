"""Endpoints relatifs aux agregats budgetaires annuels."""

from fastapi import APIRouter

from api.db.deps import DbSession
from api.schemas.budget import AnneeBudgetListItem, AnneeBudgetResponse
from api.services import budget_service

router = APIRouter(prefix="/budget", tags=["budget"])


@router.get("/annees", response_model=list[AnneeBudgetListItem])
async def lister_annees(db: DbSession) -> list[AnneeBudgetListItem]:
    annees = await budget_service.lister_annees(db)
    return [AnneeBudgetListItem.model_validate(a) for a in annees]


@router.get("/historique", response_model=list[AnneeBudgetListItem])
async def historique(
    db: DbSession,
    de: int | None = None,
    a: int | None = None,
) -> list[AnneeBudgetListItem]:
    annees = await budget_service.lister_historique(db, de, a)
    return [AnneeBudgetListItem.model_validate(item) for item in annees]


@router.get("/{annee}", response_model=AnneeBudgetResponse)
async def obtenir_annee(annee: int, db: DbSession) -> AnneeBudgetResponse:
    budget = await budget_service.obtenir_annee(db, annee)
    return AnneeBudgetResponse.model_validate(budget)
