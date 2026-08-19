"""Schemas Pydantic pour les depenses fiscales (niches fiscales)."""

from pydantic import BaseModel, ConfigDict

from api.models.depense_fiscale import StatutMontant


class DepenseFiscaleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    annee: int
    numero: str
    categorie: str
    sous_categorie: str
    sous_sous_categorie: str | None
    libelle: str
    beneficiaire: str
    montant_millions: float | None
    statut_montant: StatutMontant
    methode_chiffrage: str | None
