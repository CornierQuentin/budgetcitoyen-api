"""Schemas Pydantic pour la simulation de budget personnel."""

from pydantic import BaseModel, ConfigDict


class SourceCitee(BaseModel):
    """Une source institutionnelle citee dans la methodologie (nom + URL)."""

    model_config = ConfigDict(from_attributes=True)

    nom: str
    url: str


class MethodologieInfo(BaseModel):
    """Methodologie affichee explicitement (CDC 1.3: transparence et verifiabilite)."""

    model_config = ConfigDict(from_attributes=True)

    hypotheses: list[str]
    limites: list[str]
    sources: list[SourceCitee]


class RepartitionItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    mission_slug: str
    mission_nom: str
    montant: float


class BudgetPersoResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    revenu_net_mensuel: float
    annee_reference: int
    ir_estime: float
    tva_estimee: float
    contribution_totale_estimee: float
    repartition: list[RepartitionItem]
    methodologie: MethodologieInfo
