"""Regroupe tous les modeles SQLAlchemy pour l'autogenerate Alembic."""

from api.models.action import Action
from api.models.annee_budget import AnneeBudget
from api.models.base import Base
from api.models.depense import Depense
from api.models.marche_public import MarchePublic
from api.models.mission import Mission
from api.models.mission_alias import MissionAlias
from api.models.programme import Programme
from api.models.recette import Recette, TypeRecette

__all__ = [
    "Base",
    "AnneeBudget",
    "Mission",
    "Programme",
    "Action",
    "Depense",
    "Recette",
    "TypeRecette",
    "MissionAlias",
    "MarchePublic",
]
