"""Table de correspondance entre les intitules bruts (CSV source) et les Missions."""

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class MissionAlias(Base):
    __tablename__ = "mission_alias"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    nom_csv: Mapped[str] = mapped_column(String(255))
    mission_id: Mapped[int] = mapped_column(ForeignKey("mission.id"))
    annee_debut: Mapped[int]
    annee_fin: Mapped[int]
