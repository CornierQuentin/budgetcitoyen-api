"""Constantes de sources de donnees ETL.

Portail source: data.economie.gouv.fr (OpenDataSoft v2.1). Le format des
donnees "depenses" change de generation selon l'annee - voir DEPENSES_DATASETS
et le docstring de chaque normaliseur dans `api.etl.normalize`:

- 2016: piece jointe dediee "BG-Action_Titre" (un fichier par perimetre
  budgetaire BG/CAS/CCF au sein du dataset, filtrage BG donc fait par le
  CHOIX de la piece jointe, pas par une colonne interne); 3 lignes d'en-tete
  parasites avant la ligne de colonnes; pas de code mission ni de libelle
  programme (repli sur le code).
- 2017: piece jointe dediee "BG-Action_Categorie" (meme principe de
  filtrage BG que 2016 par le choix de la piece jointe); pas de lignes
  d'en-tete parasites mais colonne 'Libelle' repetee 3x (programme/
  action/categorie), parsee par indice plutot que par nom; pas de code
  mission mais un vrai libelle programme, contrairement a 2016.
- 2018: un seul fichier CSV en piece jointe, BG+CAS+CCF deja fusionnes dans
  un meme fichier (filtre sur 'Type de Budget (Hors Budgets annexes)' ==
  'Budget général', comme 2019), montants finaux directs (pas de
  decomposition T2/HT2 comme 2021-2022).
- 2019: API records JSON (dataset dedie, colonnes prefixees "code_"/annee_lfi).
- 2020: deux fichiers CSV en piece jointe (nomenclature + credits) a joindre.
- 2021-2022: un seul fichier CSV "detaillee" en piece jointe (libelles inclus).
- 2023-2025: API records JSON "propre" (colonnes stables entre les 3 annees).

Toutes les annees (2016-2025) ne retiennent que le budget general (BG),
jamais les comptes d'affectation speciale (CAS) ni les comptes de concours
financiers (CCF), pour rester comparables entre elles - meme quand les
fichiers CAS/CCF existent et seraient techniquement sommables (2016-2017
notamment: sommer BG+CAS+CCF romprait la coherence de perimetre avec
2018-2025, deja tous en BG seul).

Les recettes ("recettes du budget general") ne sont disponibles sous forme
structuree que pour 2024 et 2025 sur ce portail (verifie par recherche
exhaustive du catalogue complet - aucun dataset equivalent pour 2015-2023).
Pour 2016-2020, 2022 et 2023, les recettes sont reconstituees a partir d'une
source differente: les rapports annuels "Le budget de l'Etat en <annee>" de
la Cour des comptes (voir RECETTES_COUR_DES_COMPTES_* ci-dessous et le
docstring de `api.etl.normalize.normalize_recettes_cour_des_comptes`). 2015
et 2021 restent des trous reels (aucune des deux sources ne fournit un
tableau exploitable pour ces annees - voir ces memes constantes).
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

# Depenses: datasets a pieces jointes (attachments) pour 2016-2018 et
# 2020-2022, dont il faut resoudre dynamiquement l'URL exacte via l'endpoint
# catalog dataset.
DEPENSES_DATASETS_ATTACHMENTS: dict[int, str] = {
    2016: "loi-de-finances-initiale-pour-2016-lfi-2016",
    2017: "loi-de-finances-initiale-pour-2017-lfi-2017",
    2018: "loi-de-finances-initiale-pour-2018-lfi-2018",
    2020: "projet-de-loi-de-finances-initiale-pour-2020-lfi-2020",
    2021: "projet-de-loi-de-finances-initiale-pour-2021-lfi-2021",
    2022: "projet-de-loi-de-finances-initiale-pour-2022-lfi-2022",
}

# Identifiants des pieces jointes CSV a utiliser au sein de chaque dataset
# 2016-2018/2020-2022 (2020: deux fichiers a joindre : nomenclature +
# credits). 2016/2017: un seul des deux decoupages BG alternatifs
# (Action_Categorie vs Action_Titre - memes totaux, juste une decomposition
# fine differente) est retenu par annee, celui qui expose le plus de
# libelles exploitables tel quel - cf. `api.etl.normalize.
# normalize_depenses_2016`/`normalize_depenses_2017`.
DEPENSES_ATTACHMENT_IDS: dict[int, dict[str, str]] = {
    2016: {
        "detaillee": "lfi2016_bg_action_titre_csv",
    },
    2017: {
        "detaillee": "lfi2017_bg_action_categorie_csv",
    },
    2018: {
        "detaillee": "lfi_2018_act_cat_tit_bg_cas_ccf_csv",
    },
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
DEPENSES_ANNEES: tuple[int, ...] = (
    2016,
    2017,
    2018,
    2019,
    2020,
    2021,
    2022,
    2023,
    2024,
    2025,
)

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

# --------------------------------------------------------------------------
# Recettes 2016-2020/2022/2023: rapports annuels "Le budget de l'Etat en
# <annee> (resultats et gestion)" de la Cour des comptes
# (https://www.ccomptes.fr/fr/publications/le-budget-de-letat-en-<annee>
# -resultats-et-gestion). Chaque rapport porte sur l'annee <annee> elle-meme
# (pas de decalage: le rapport "2017" documente l'exercice 2017), et propose
# un ZIP telechargeable contenant tous les tableaux/graphiques du rapport en
# CSV.
#
# Deux tableaux distincts sont exploites par millesime, quand disponibles:
# - un tableau "recettes fiscales nettes par impot" (IR/IS/TICPE/TVA/Autres),
#   avec une colonne LFI (Loi de Finances Initiale votee) -> RECETTES_
#   COUR_DES_COMPTES_FICHIER_IMPOT. C'est la seule donnee disponible pour
#   TOUTES les annees couvertes.
# - un tableau "tableau d'equilibre" (recettes fiscales nettes / recettes
#   non fiscales / PSR UE / PSR collectivites / ...), egalement en colonne
#   LFI -> RECETTES_COUR_DES_COMPTES_FICHIER_EQUILIBRE. Fournit les
#   "recettes non fiscales" (ajoutees au bucket AUTRES, cf. normalize.py) et
#   les PSR (exclus, meme methodologie que pour 2024-2025 - cf.
#   `normalize.extract_prelevements_sur_recettes`). PAS disponible pour 2023
#   (le ZIP 2023 ne contient que des fichiers de graphiques "G*.csv", aucun
#   tableau "T*"/"D_Tableau*" avec une decomposition non-fiscale/PSR en LFI
#   - verifie par recherche exhaustive du contenu du ZIP): pour 2023,
#   `recette` ne couvre donc QUE les recettes fiscales nettes par impot,
#   sans ajout des recettes non fiscales ni deduction des PSR - limitation
#   connue et documentee, qui sous-estime legerement le deficit LFI 2023
#   calcule par rapport aux autres annees de ce lot (cf. JOURNAL/PR).
#
# Noms de fichiers dans le ZIP VOLONTAIREMENT NON devinables par motif:
# constate a l'inspection reelle des 7 ZIP (aout 2026), ils sont
# incoherents d'un millesime a l'autre ("T14 recettes.csv" en 2015,
# "D_T9 recettes fiscales nettes.csv" en 2016, "G16.csv" en 2022, "G 14.csv"
# en 2023...). Les noms ci-dessous ont ete localises par recherche du
# CONTENU (libelles "Impot sur le revenu"/"TVA"/"Impot sur les societes"/
# "TICPE" + une colonne "LFI") au sein de chaque ZIP, jamais par le nom du
# fichier lui-meme - voir le detail annee par annee dans le journal de la
# PR qui a introduit ce bloc.
#
# 2015: le ZIP ("RBDE-2015.zip") ne contient AUCUN tableau consolide par
# impot avec une colonne LFI absolue - seuls des tableaux d'"ecart"
# (variations relatives annee sur annee, ex "Ecart LFI: +1,9 Md ... -0,8
# Md") et un tableau agrege "Recettes fiscales nettes" (sans decomposition
# par impot) sont presents. Reconstruire un montant absolu par impot a
# partir de ces seuls ecarts serait fragile (risque d'erreur de composition
# non detectable) - annee volontairement exclue plutot que forcee.
#
# 2021: le rapport ne propose PAS de ZIP de donnees consolidees (verifie a
# l'execution reelle: la page /fr/publications/le-budget-de-letat-en-2021
# -resultats-et-gestion ne contient aucun lien vers un fichier .zip). Les
# seules ressources telechargeables sont des PDF individuels par
# mission/theme (notes d'execution budgetaire, format "NEB"), dont un PDF
# "Recettes fiscales 2021" (NEB-2021-Recettes-fiscales.pdf) - PAS un CSV
# structure exploitable automatiquement. Annee exclue.
RECETTES_COUR_DES_COMPTES_ZIP_URLS: dict[int, str] = {
    2016: "https://www.ccomptes.fr/sites/default/files/EzPublish/Donnees-RBDE-2016.zip",
    2017: "https://www.ccomptes.fr/sites/default/files/2018-05/20180523-donnees-rapport-budget-Etat-2017.zip",
    2018: "https://www.ccomptes.fr/sites/default/files/2023-10/20190515-donnees-Budget-Etat-2018_0.zip",
    2019: "https://www.ccomptes.fr/sites/default/files/2023-10/20200428-donnees-RBDE_2019.zip",
    2020: "https://www.ccomptes.fr/sites/default/files/2021-10/20210413-donnees-Budget-Etat-2020.zip",
    2022: "https://www.ccomptes.fr/sites/default/files/2023-10/20230413-donnees-RBDE-2022.zip",
    2023: "https://www.ccomptes.fr/sites/default/files/2024-08/20240417-donnees-RBDE-2023.zip",
}

# Membre du ZIP portant le tableau "recettes fiscales nettes par impot".
RECETTES_COUR_DES_COMPTES_FICHIER_IMPOT: dict[int, str] = {
    2016: "Données/D_T9 recettes fiscales nettes.csv",
    2017: "Data/DATAFIJ/D_T12- Recettes fiscales nettes.csv",
    2018: (
        "Sources OPEN DATA-Tableaux et graphiques-version 13 mai matin/"
        "D_T5 - recettes fiscales nettes.csv"
    ),
    2019: "RBDE 2019 - Tableaux et graphiques - 28 avril 2020/D_T4 - recettes fiscales.csv",
    2020: "D_Graphique 3.csv",
    2022: "G16.csv",
    2023: "G 14.csv",
}

# Membre du ZIP portant le "tableau d'equilibre" (recettes non fiscales +
# PSR, colonne LFI). Absent de ce dict pour 2023 (cf. commentaire ci-dessus).
RECETTES_COUR_DES_COMPTES_FICHIER_EQUILIBRE: dict[int, str] = {
    2016: "Données/D_T1- formation du solde.csv",
    2017: "Data/DATAFIJ/D_T3 - Recettes nettes Etat.csv",
    2018: (
        "Sources OPEN DATA-Tableaux et graphiques-version 13 mai matin/"
        "D_T4 - Recettes nettes de l'Etat.csv"
    ),
    2019: "RBDE 2019 - Tableaux et graphiques - 28 avril 2020/D_T8 - recettes du BG.csv",
    2020: "D_Tableau n°1.csv",
    2022: "T1.csv",
}

# Delimiteur CSV par millesime: ";" pour 2016-2020 (export d'origine
# Cour des comptes), "," a partir de 2022 (constate a l'inspection reelle -
# pas une regle generale du site, juste l'export tel quel observe par
# annee). Le point (pas la virgule) est le separateur decimal dans TOUS les
# fichiers retenus ici, y compris ceux delimites par ";" - `clean_montant`
# gere les deux de toute facon.
RECETTES_COUR_DES_COMPTES_DELIMITER: dict[int, str] = {
    2016: ";",
    2017: ";",
    2018: ";",
    2019: ";",
    2020: ";",
    2022: ",",
    2023: ",",
}

RECETTES_COUR_DES_COMPTES_ANNEES: tuple[int, ...] = (2016, 2017, 2018, 2019, 2020, 2022, 2023)


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
    if annee in (2016, 2017, 2018, 2021, 2022):
        dataset_id = DEPENSES_DATASETS_ATTACHMENTS[annee]
        return attachment_url(dataset_id, DEPENSES_ATTACHMENT_IDS[annee]["detaillee"])
    return None
