"""Schemas Pydantic pour le comparateur d'annees budgetaires."""

from pydantic import BaseModel, ConfigDict

from api.schemas.budget import AnneeBudgetResponse


class ComparateurResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    annee_a: AnneeBudgetResponse
    annee_b: AnneeBudgetResponse
    ecart_depenses: float
    ecart_recettes: float
    ecart_deficit: float
