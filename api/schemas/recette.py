"""Schemas Pydantic pour les recettes fiscales."""

from pydantic import BaseModel, ConfigDict

from api.models.recette import TypeRecette


class RecetteResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    annee: int
    type: TypeRecette
    montant_brut: float
    montant_net: float
