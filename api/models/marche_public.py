"""Modele MarchePublic (donnees de la commande publique - module V2)."""

from datetime import date

from sqlalchemy import Date, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class MarchePublic(Base):
    __tablename__ = "marche_public"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    acheteur_siret: Mapped[str] = mapped_column(String(14))
    titulaire: Mapped[str] = mapped_column(String(255))
    objet: Mapped[str] = mapped_column(Text)
    montant: Mapped[float] = mapped_column(Numeric(15, 2))
    date_signature: Mapped[date] = mapped_column(Date)
