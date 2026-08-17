"""Schemas Pydantic pour le comparateur d'annees budgetaires."""

from pydantic import BaseModel, ConfigDict

from api.models.recette import TypeRecette
from api.schemas.budget import AnneeBudgetResponse


class MissionDeltaItem(BaseModel):
    """Ecart d'une mission (identifiee par son slug stable) entre deux annees."""

    model_config = ConfigDict(from_attributes=True)

    slug: str
    nom: str
    montant_a: float
    montant_b: float
    delta_absolu: float
    # None si montant_a == 0 (division impossible / variation non significative).
    delta_relatif_pct: float | None


class RecetteDeltaItem(BaseModel):
    """Ecart d'un type de recette fiscale entre deux annees.

    Les champs sont a None quand l'une des deux annees n'a pas de donnees de
    recettes ingerees (perimetre actuel: 2024-2025 uniquement).
    """

    model_config = ConfigDict(from_attributes=True)

    type: TypeRecette
    montant_a: float | None
    montant_b: float | None
    delta_absolu: float | None
    delta_relatif_pct: float | None


class ComparateurResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    annee_a: AnneeBudgetResponse
    annee_b: AnneeBudgetResponse
    ecart_depenses: float
    ecart_recettes: float
    ecart_deficit: float
    missions: list[MissionDeltaItem]
    recettes: list[RecetteDeltaItem]
