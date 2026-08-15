"""Modele Depense (autorisations d'engagement / credits de paiement d'une Action)."""

from sqlalchemy import ForeignKey, Numeric
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class Depense(Base):
    __tablename__ = "depense"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    action_id: Mapped[int] = mapped_column(ForeignKey("action.id"))
    ae: Mapped[float] = mapped_column(Numeric(15, 2))
    cp: Mapped[float] = mapped_column(Numeric(15, 2))
    annee: Mapped[int]
