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


# --------------------------------------------------------------------------
# Indicateurs macro: PIB nominal et population, pour les futurs indicateurs
# "par habitant"/"par seconde" du frontend (ex. "Depenses = X EUR par
# Francais"). Table dediee `indicateur_macro`, distincte de `annee_budget`.
# --------------------------------------------------------------------------

# PIB nominal ("PIB en valeur", prix courants - PAS le volume/prix chaines),
# 1949-2022, colonnes CSV "annee,pib" en MILLIONS d'euros courants. Source:
# data.gouv.fr, qui reprend telle quelle la serie INSEE "Comptes nationaux
# annuels base 2014 - Produit interieur brut approche produit - Prix
# courant" (idbank 010548500,
# https://www.insee.fr/fr/statistiques/serie/010548500) - EXPLICITEMENT
# marquee "Serie arretee" (discontinuee) par l'INSEE suite au changement de
# base (passage a la base 2020 le 31/05/2024, cf. "Comptes nationaux
# annuels en 2024"): aucune donnee 2023-2025 dans ce fichier. Ce n'est pas
# un trou de telechargement mais un trou reel de continuite de cette serie
# precise - complete par PIB_COMPLEMENT_XLSX_URLS ci-dessous.
PIB_CSV_URL = f"{DATA_GOUV_BASE_URL}/api/1/datasets/r/cd2ac200-0130-459e-809f-843f46e20d28"

# Complement 2023, 2024, 2025 (PIB nominal, prix courants, base 2020).
#
# La serie successeur base 2020 (idbank 011779992,
# https://www.insee.fr/fr/statistiques/serie/011779992) n'est PAS
# telechargeable publiquement en CSV/xlsx sans cle d'API: verifie a
# l'execution reelle (aout 2026) - les pages "serie/<idbank>" d'insee.fr
# sont rendues cote client (React/SPA, aucune donnee dans le HTML statique)
# et les endpoints bdm.insee.fr/series/sdmx/data/... retournent une erreur
# 500 sans authentification (portail "Melodi", successeur de l'ancien BDM,
# necessite une cle sur api.insee.fr).
#
# A la place, chaque edition annuelle de la publication "Insee Premiere -
# Les comptes de la Nation en <annee>" fournit un fichier de donnees joint
# (feuille "Figure 1 - Le PIB et les operations sur les biens et les
# services") avec le NIVEAU du PIB en milliards d'euros COURANTS (colonne
# "En milliards d'euros", explicitement distincte des colonnes "Evolution
# en volume" en %) pour la derniere annee couverte par l'edition. Valeurs
# verifiees a l'execution reelle: 2023 = 2822,5 Md EUR (IP1997), 2024 =
# 2919,9 Md EUR (IP2053), 2025 = 2991,1 Md EUR (IP2105) - source "Insee,
# comptes nationaux, base 2020" citee dans chaque fichier. Ces chiffres
# peuvent faire l'objet de legeres revisions d'une edition a l'autre (toute
# comptabilite nationale est susceptible de revisions): on retient ici,
# pour chaque annee, le chiffre publie dans l'edition qui lui est dediee.
PIB_COMPLEMENT_XLSX_URLS: dict[int, str] = {
    2023: "https://www.insee.fr/fr/statistiques/fichier/8193933/IP1997.xlsx",
    2024: "https://www.insee.fr/fr/statistiques/fichier/8574058/IP2053.xlsx",
    2025: "https://www.insee.fr/fr/statistiques/fichier/8996855/IP2105.xlsx",
}

# Population au 1er janvier, France entiere (avec DOM a partir de 2014, hors
# Mayotte avant), onglet "FR" du fichier INSEE "Population annuelle et
# composantes de l'evolution demographique" - colonne "Population au 1er
# janvier". Les annees anciennes (avant 1982 dans l'edition courante) valent
# "nd" (non disponible) dans la source: trou reel documente, pas un bug de
# parsing (cf. `api.etl.normalize.normalize_population_xlsx`).
POPULATION_XLSX_URL = (
    "https://www.insee.fr/fr/statistiques/fichier/8560651/1_Pop_annu_compo_evol.xlsx"
)


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
