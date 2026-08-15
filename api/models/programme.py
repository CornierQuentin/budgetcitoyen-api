"""Modele Programme (subdivision d'une Mission)."""

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class Programme(Base):
    __tablename__ = "programme"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    mission_id: Mapped[int] = mapped_column(ForeignKey("mission.id"))
    code: Mapped[str] = mapped_column(String(50))
    nom: Mapped[str] = mapped_column(String(255))
    annee: Mapped[int]
