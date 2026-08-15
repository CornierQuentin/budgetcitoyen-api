"""Modele agrege budgetaire annuel (vue macro: depenses/recettes/deficit/dette)."""

from sqlalchemy import Numeric, Text
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class AnneeBudget(Base):
    __tablename__ = "annee_budget"

    annee: Mapped[int] = mapped_column(primary_key=True)
    depenses_nettes: Mapped[float] = mapped_column(Numeric(15, 2))
    recettes_nettes: Mapped[float] = mapped_column(Numeric(15, 2))
    deficit: Mapped[float] = mapped_column(Numeric(15, 2))
    dette_pib: Mapped[float] = mapped_column(Numeric(5, 2))
    source_url: Mapped[str] = mapped_column(Text)
