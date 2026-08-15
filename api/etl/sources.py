"""Constantes de sources de donnees ETL.

URLs exactes a confirmer apres inspection manuelle - non resolu.
Ce module ne doit contenir AUCUN appel reseau: uniquement des constantes
et des points d'ancrage pour les futurs jobs d'ingestion (Phase 1).
"""

from api.core.config import get_settings

settings = get_settings()

# Base URLs (voir .env.example / Settings)
DATA_ECONOMIE_BASE_URL = settings.data_economie_base_url
DATA_GOUV_BASE_URL = settings.data_gouv_base_url

# URLs exactes a confirmer apres inspection manuelle - non resolu.
URL_DEPENSES_ETAT_CSV = f"{DATA_ECONOMIE_BASE_URL}/explore/dataset/depenses-etat"
URL_RECETTES_ETAT_CSV = f"{DATA_ECONOMIE_BASE_URL}/explore/dataset/recettes-etat"
URL_MISSIONS_REFERENTIEL = f"{DATA_GOUV_BASE_URL}/fr/datasets/referentiel-missions"
