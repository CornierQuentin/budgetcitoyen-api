"""Modele IngestionLog: trace la date de fin de chaque execution ETL reussie.

Alimente `derniere_ingestion` sur GET /health (cahier des charges section
4.2). Sert aussi de brique minimale pour les "logs d'ingestion horodates"
du cahier des charges section 3.5 - la retention a 90 jours qui y est
mentionnee suppose un job planifie de purge, hors perimetre V1 (le CDC
lui-meme precise "V1: execution manuelle du script"), donc non implementee
ici : une seule ligne par execution reussie, jamais purgee automatiquement.
"""

from datetime import datetime

from sqlalchemy import DateTime
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class IngestionLog(Base):
    __tablename__ = "ingestion_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    termine_a: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
