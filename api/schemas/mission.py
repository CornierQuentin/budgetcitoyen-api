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


class MissionHistoriqueItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    annee: int
    nom_officiel: str
