"""Schemas Pydantic pour les agregats budgetaires annuels."""

from pydantic import BaseModel, ConfigDict


class AnneeBudgetResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    annee: int
    depenses_nettes: float
    recettes_nettes: float
    deficit: float
    # Hors perimetre de cette passe d'ingestion (donnees INSEE dette/PIB non
    # ingerees): `AnneeBudget.dette_pib` vaut toujours None pour l'instant,
    # cf. `api.etl.loader.recalculer_annee_budget`. Doit rester optionnel
    # ici, sans quoi la validation Pydantic rejette toute annee chargee par
    # ce pipeline (bug constate a la verification post-ETL).
    dette_pib: float | None
    source_url: str


class AnneeBudgetListItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    annee: int
    depenses_nettes: float
    recettes_nettes: float
    deficit: float
