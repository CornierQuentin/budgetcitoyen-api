"""Schemas Pydantic pour les indicateurs macro-economiques (PIB, population)."""

from pydantic import BaseModel, ConfigDict


class IndicateurMacroResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    annee: int
    pib_courant: float | None
    population: int | None
    source_pib_url: str | None
    source_population_url: str | None
