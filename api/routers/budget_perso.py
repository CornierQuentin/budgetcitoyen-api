"""Endpoint de simulation de budget personnel."""

from fastapi import APIRouter

from api.db.deps import DbSession
from api.schemas.budget_perso import BudgetPersoResponse
from api.services import budget_perso_service

router = APIRouter(prefix="/budget-perso", tags=["budget-perso"])


@router.get("", response_model=BudgetPersoResponse)
async def obtenir_budget_perso(revenu_net: float, db: DbSession) -> BudgetPersoResponse:
    return await budget_perso_service.calculer_budget_perso(revenu_net, db)
