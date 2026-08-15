"""Modele Recette (recettes fiscales de l'Etat par type et par annee)."""

import enum

from sqlalchemy import Enum, Numeric, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class TypeRecette(enum.StrEnum):
    IR = "IR"
    TVA = "TVA"
    IS = "IS"
    TICPE = "TICPE"
    AUTRES = "AUTRES"


class Recette(Base):
    __tablename__ = "recette"
    __table_args__ = (UniqueConstraint("annee", "type"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    annee: Mapped[int]
    type: Mapped[TypeRecette] = mapped_column(Enum(TypeRecette, name="type_recette"))
    montant_brut: Mapped[float] = mapped_column(Numeric(15, 2))
    montant_net: Mapped[float] = mapped_column(Numeric(15, 2))
