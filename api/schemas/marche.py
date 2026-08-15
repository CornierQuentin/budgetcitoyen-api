"""Schemas Pydantic pour les marches publics (module V2)."""

from datetime import date

from pydantic import BaseModel, ConfigDict


class MarchePublicResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    acheteur_siret: str
    titulaire: str
    objet: str
    montant: float
    date_signature: date
