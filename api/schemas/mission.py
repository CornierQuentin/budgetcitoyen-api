"""Schemas Pydantic pour les missions, programmes et actions budgetaires."""

from pydantic import BaseModel, ConfigDict


class ActionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    programme_id: int
    code: str
    nom: str
    annee: int


class ProgrammeResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    mission_id: int
    code: str
    nom: str
    annee: int


class MissionResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    slug: str
    nom_normalise: str
    nom_officiel: str
    annee: int
    # Somme des credits de paiement (Depense.cp) de la mission pour son annee.
    # Necessaire pour le treemap des depenses par mission (CDC Module 1).
    montant_total: float


class MissionHistoriqueItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    annee: int
    nom_officiel: str
    montant_total: float


class ActionDetailItem(BaseModel):
    id: int
    code: str
    nom: str
    ae: float
    cp: float


class ProgrammeDetailItem(BaseModel):
    id: int
    code: str
    nom: str
    montant_total: float  # somme des cp de ses actions
    actions: list[ActionDetailItem]
    # Deux drapeaux qui disent ce que la SOURCE de l'annee fournit reellement,
    # pour qu'un client n'ait pas a redeviner les conventions de l'ETL (voir
    # `api.services.mission_service.obtenir_mission_detail`, qui les calcule,
    # et `api.etl.normalize.normalize_depenses_legifrance`, qui cree les cas
    # ou ils valent False).
    #
    # `code` reste renvoye tel quel dans les deux cas: c'est bien la cle qui
    # identifie le programme en base, et un client qui la veut doit pouvoir
    # l'obtenir. Ces drapeaux disent seulement s'il est publiable comme une
    # information budgetaire.
    code_officiel: bool
    actions_detaillees: bool


class MissionDetailResponse(BaseModel):
    id: int
    slug: str
    nom_officiel: str
    annee: int
    montant_total: float
    programmes: list[ProgrammeDetailItem]
