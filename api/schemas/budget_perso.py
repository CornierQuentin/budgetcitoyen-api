"""Schemas Pydantic pour la simulation de budget personnel."""

from pydantic import BaseModel, ConfigDict


class RepartitionItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    mission: str
    montant: float
    part_pct: float


class BudgetPersoResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    revenu_net: float
    contribution_totale_estimee: float
    repartition: list[RepartitionItem]
