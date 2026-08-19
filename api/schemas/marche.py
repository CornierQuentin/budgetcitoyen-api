"""Schemas Pydantic pour les marches publics (Donnees Essentielles de la
Commande Publique - DECP)."""

from datetime import date

from pydantic import BaseModel, ConfigDict


class MarchePublicResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    marche_id_source: str
    nature: str | None
    objet: str
    codecpv: str
    codecpv_division: str
    procedure: str | None
    acheteur_siret: str
    titulaire_siret: str
    titulaire_id_type: str | None
    dureemois: int | None
    datenotification: date
    datepublicationdonnees: date | None
    montant: float
    formeprix: str | None
    offresrecues: int | None
    marcheinnovant: bool | None


class MarchesPageResponse(BaseModel):
    """Enveloppe de pagination dediee (pas de generique Page[T]): premier et
    seul point d'usage de la pagination dans cette API pour l'instant."""

    items: list[MarchePublicResponse]
    total: int
    page: int
    page_size: int
    total_pages: int


class MarchesCpvRepartitionItem(BaseModel):
    cpv_division: str
    label: str
    montant_total: float
    nombre: int


class MarchesBornesResponse(BaseModel):
    """Bornes reelles des donnees (date/montant min-max), pour que le
    frontend calibre ses selecteurs de filtre sans deviner."""

    date_min: date | None
    date_max: date | None
    montant_min: float | None
    montant_max: float | None
