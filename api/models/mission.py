"""Modele Mission (mission budgetaire au sens LOLF)."""

from sqlalchemy import String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class Mission(Base):
    __tablename__ = "mission"
    __table_args__ = (UniqueConstraint("slug", "annee"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    slug: Mapped[str] = mapped_column(String(255), index=True)
    nom_normalise: Mapped[str] = mapped_column(String(255))
    nom_officiel: Mapped[str] = mapped_column(String(255))
    annee: Mapped[int]
