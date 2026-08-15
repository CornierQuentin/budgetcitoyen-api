"""Schemas Pydantic pour les agregats budgetaires annuels."""

from pydantic import BaseModel, ConfigDict


class AnneeBudgetResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    annee: int
    depenses_nettes: float
    recettes_nettes: float
    deficit: float
    dette_pib: float
    source_url: str


class AnneeBudgetListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    annee: int
    depenses_nettes: float
    recettes_nettes: float
    deficit: float
