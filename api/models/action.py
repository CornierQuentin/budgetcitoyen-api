"""Modele Action (subdivision d'un Programme)."""

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class Action(Base):
    __tablename__ = "action"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    programme_id: Mapped[int] = mapped_column(ForeignKey("programme.id"))
    code: Mapped[str] = mapped_column(String(50))
    nom: Mapped[str] = mapped_column(String(255))
    annee: Mapped[int]
