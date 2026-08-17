"""Modele des indicateurs macro-economiques (PIB nominal, population).

Table dediee, distincte de `annee_budget`: sert uniquement a alimenter les
futurs indicateurs de contextualisation "par habitant" / "par seconde" du
frontend (ex. "Depenses = X EUR par Francais"). `annee_budget.dette_pib`
reste hors perimetre de ce chantier - le PIB seul ne le remplit pas (il
faudrait aussi la dette publique, non ingeree ici).
"""

from sqlalchemy import BigInteger, Numeric, Text
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class IndicateurMacro(Base):
    __tablename__ = "indicateur_macro"

    annee: Mapped[int] = mapped_column(primary_key=True)
    # En EUROS (pas en millions), coherent avec `depense.ae`/`depense.cp` deja
    # en euros. Nullable: trou reel 2023-2025 possible selon la disponibilite
    # de la source complementaire (cf. `api.etl.sources.PIB_COMPLEMENT_XLSX_URLS`).
    pib_courant: Mapped[float | None] = mapped_column(Numeric(15, 2))
    population: Mapped[int | None] = mapped_column(BigInteger)
    source_pib_url: Mapped[str | None] = mapped_column(Text)
    source_population_url: Mapped[str | None] = mapped_column(Text)
