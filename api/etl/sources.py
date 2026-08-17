"""Constantes de sources de donnees ETL.

Portail source: data.economie.gouv.fr (OpenDataSoft v2.1). Le format des
donnees "depenses" change de generation selon l'annee - voir DEPENSES_DATASETS
et le docstring de chaque normaliseur dans `api.etl.normalize`:

- 2019: API records JSON (dataset dedie, colonnes prefixees "code_"/annee_lfi).
- 2020: deux fichiers CSV en piece jointe (nomenclature + credits) a joindre.
- 2021-2022: un seul fichier CSV "detaillee" en piece jointe (libelles inclus).
- 2023-2025: API records JSON "propre" (colonnes stables entre les 3 annees).

Les recettes ("recettes du budget general") ne sont disponibles sous forme
structuree que pour 2024 et 2025 sur ce portail (verifie par recherche
exhaustive du catalogue complet - aucun dataset equivalent pour 2015-2023).
"""

from api.core.config import get_settings

settings = get_settings()

DATA_ECONOMIE_BASE_URL = settings.data_economie_base_url.rstrip("/")
DATA_GOUV_BASE_URL = settings.data_gouv_base_url.rstrip("/")

API_EXPLORE_V21 = f"{DATA_ECONOMIE_BASE_URL}/api/explore/v2.1/catalog/datasets"

# Nombre maximal de lignes par page accepte par l'API OpenDataSoft v2.1.
RECORDS_PAGE_SIZE = 100

# --------------------------------------------------------------------------
# Depenses: un dataset "records" (API JSON) par annee pour 2019 et 2023-2025.
# --------------------------------------------------------------------------
DEPENSES_DATASETS_RECORDS: dict[int, str] = {
    2019: "loi-de-finances-initiale-pour-2019-lfi-2019-1",
    2023: "credits-ae-et-cp-votes-nomenclature-par-destination-et-nature-lfi-2023",
    2024: "plf-2024-depenses-2024-selon-nomenclatures-destination-et-nature",
    2025: "plf25-depenses-2025-selon-destination",
}

# Depenses: datasets a pieces jointes (attachments) pour 2020-2022, dont il
# faut resoudre dynamiquement l'URL exacte via l'endpoint catalog dataset.
DEPENSES_DATASETS_ATTACHMENTS: dict[int, str] = {
    2020: "projet-de-loi-de-finances-initiale-pour-2020-lfi-2020",
    2021: "projet-de-loi-de-finances-initiale-pour-2021-lfi-2021",
    2022: "projet-de-loi-de-finances-initiale-pour-2022-lfi-2022",
}

# Identifiants des pieces jointes CSV a utiliser au sein de chaque dataset
# 2020-2022 (2020: deux fichiers a joindre : nomenclature + credits).
DEPENSES_ATTACHMENT_IDS: dict[int, dict[str, str]] = {
    2020: {
        "nomenclature": "lfi2020_nomenclature_csv",
        "credits": "lfi2020_credits_csv",
    },
    2021: {
        "detaillee": "lfi2021_credits_ae_cp_csv",
    },
    2022: {
        "detaillee": "lfi2022_detaillee_csv",
    },
}

# Toutes les annees de depenses couvertes par cette passe d'ingestion.
DEPENSES_ANNEES: tuple[int, ...] = (2019, 2020, 2021, 2022, 2023, 2024, 2025)

# --------------------------------------------------------------------------
# Recettes: seules 2024 et 2025 disposent d'un dataset "recettes du budget
# general" structure sur ce portail (trou de donnees reel pour 2015-2023).
# --------------------------------------------------------------------------
RECETTES_DATASETS_RECORDS: dict[int, str] = {
    2024: "plf-2024-recettes-du-budget-general",
    2025: "plf25-recettes-du-budget-general",
}

RECETTES_ANNEES: tuple[int, ...] = (2024, 2025)

# Codes de ligne de recette (code_ligne_recettes) mappes sur les buckets
# TypeRecette stables. Tout code absent de cette table tombe dans AUTRES.
CODE_LIGNE_RECETTE_VERS_TYPE: dict[float, str] = {
    1101.0: "IR",
    1301.0: "IS",
    1601.0: "TVA",
    1501.0: "TICPE",
}


def records_url(dataset_id: str) -> str:
    """URL de base de l'endpoint records (v2.1) pour un dataset donne."""
    return f"{API_EXPLORE_V21}/{dataset_id}/records"


def dataset_metadata_url(dataset_id: str) -> str:
    """URL de l'endpoint catalog dataset (metadonnees + liste des attachments)."""
    return f"{API_EXPLORE_V21}/{dataset_id}"


def attachment_url(dataset_id: str, attachment_id: str) -> str:
    """URL de telechargement d'une piece jointe d'un dataset."""
    return f"{API_EXPLORE_V21}/{dataset_id}/attachments/{attachment_id}"


def default_depenses_source_url(annee: int) -> str | None:
    """URL du dataset "depenses" principal d'une annee, quel que soit son format.

    Utilise notamment pour renseigner `annee_budget.source_url` meme quand
    l'annee courante n'a pas ete re-traitee lors du run courant (ex: relance
    en mode --recettes-only alors que les depenses avaient deja ete chargees
    lors d'un run precedent).
    """
    if annee in DEPENSES_DATASETS_RECORDS:
        return records_url(DEPENSES_DATASETS_RECORDS[annee])
    if annee == 2020:
        dataset_id = DEPENSES_DATASETS_ATTACHMENTS[2020]
        return attachment_url(dataset_id, DEPENSES_ATTACHMENT_IDS[2020]["credits"])
    if annee in (2021, 2022):
        dataset_id = DEPENSES_DATASETS_ATTACHMENTS[annee]
        return attachment_url(dataset_id, DEPENSES_ATTACHMENT_IDS[annee]["detaillee"])
    return None
