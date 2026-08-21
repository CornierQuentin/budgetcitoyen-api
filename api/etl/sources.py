"""Constantes de sources de donnees ETL.

Portail source: data.economie.gouv.fr (OpenDataSoft v2.1). Le format des
donnees "depenses" change de generation selon l'annee - voir DEPENSES_DATASETS
et le docstring de chaque normaliseur dans `api.etl.normalize`:

- 2012: API records JSON, dataset "dotation BG" deja filtre BG par
  construction, mais SANS libelle programme/action ni libelle mission
  autre que du texte brut: deux nomenclatures (mission-programme, filtree
  BG - fournit un code mission LOLF unique parmi 2012-2014 - et
  par-destination) sont jointes par code. Voir
  `api.etl.normalize.normalize_depenses_2012`.
- 2013: meme famille que 2012 (API records JSON, montants BG par
  construction), mais AUCUN dataset de nomenclature mission-programme
  n'existe pour ce millesime: pas de code mission disponible (repli slug).
  Nomenclature programme + destination jointes separement. Voir
  `api.etl.normalize.normalize_depenses_2013`.
- 2014: API records JSON, dataset "dotation BG" avec mission/programme/
  action DEJA en clair (seuls les libelles programme/action manquent), une
  seule nomenclature (filtree BG via `type_de_mission`) fournit les deux
  libelles en une jointure. Colonnes CP typees texte avec separateur de
  milliers espace (deja gere par `clean_montant` sans modification). Pas de
  code mission disponible (repli slug). Voir
  `api.etl.normalize.normalize_depenses_2014`.
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
et 2021 sont couverts par une troisieme source (voir ci-dessous, LFI via
l'API Legifrance) plutot que par le portail data.economie.gouv.fr, qui n'en
fournit aucun dataset exploitable pour ces 2 millesimes.

- 2015, 2021, 2026: source radicalement differente des annees precedentes -
  le texte meme de la LFI, recupere via l'API officielle Legifrance
  (plateforme PISTE, OAuth2 client_credentials - voir PISTE_* dans
  `api.core.config.Settings`), PAS via une piece jointe ou un dataset
  "records": Legifrance bloque les requetes non-navigateur (Cloudflare) sur
  son site public, mais l'API PISTE elle est accessible en direct par httpx
  une fois authentifiee. Introduite pour 2026 (aucun dataset
  data.economie.gouv.fr n'existe pour ce millesime, portail non alimente
  au-dela de 2025), puis reutilisee TELLE QUELLE (meme normalizer) pour
  combler les trous 2015/2021 identifies au chantier 5 - format verifie
  identique sur les 3 annees (memes codes de ligne stables, meme structure
  de tableau). 2021 n'y figure QUE pour les recettes: ses depenses sont
  deja couvertes par DEPENSES_DATASETS_ATTACHMENTS[2021] (source plus
  ancienne, deja en place).

  La reponse de `POST /consult/jorf` (voir `LFI_TEXT_CID_PAR_ANNEE`) est un
  JSON de l'ARTICULATION LEGALE du texte (sections/articles recursifs), pas
  un jeu de donnees tabulaire: les annexes "Etat A" (recettes, "Voies et
  moyens") et "Etat B" (depenses par mission/programme, "Budget general")
  sont toutes deux imbriquees, avec les etats C-G, dans le HTML du `content`
  d'un seul article sans numero repere par la marque textuelle "ETATS
  LEGISLATIFS ANNEXES" (PAS par un id d'article fige, qui pourrait changer
  en cas de texte rectificatif) - voir
  `api.etl.run._extract_etats_html`/`api.etl.normalize.normalize_depenses_legifrance`/
  `normalize_recettes_legifrance` pour le detail du parsing.

  Etat B ne fournit NI code mission NI decomposition par action (seulement
  Mission -> Programme, avec une ligne memo "Dont titre 2" par programme, a
  exclure de toute somme - deja incluse dans le total du programme): repli
  sur le slug pour l'identite mission (meme mecanisme que 2013/2014/2016/
  2017) et action synthetique unique par programme (limitation reelle de la
  source, documentee, pas un bug). La ligne "Total" de controle est ABSENTE
  pour la LFI 2015 (presente pour 2021/2026) - verification par trajectoire
  plutot que par egalite exacte pour cette annee-la.

  Etat A inclut directement les prelevements sur recettes (PSR, categorie
  "3." du tableau, sous-categories "31." collectivites et "32." Union
  europeenne) contrairement a la source records JSON 2024-2025 ou ils sont
  un `type_de_recettes` distinct - voir
  `api.etl.normalize.extract_prelevements_sur_recettes_legifrance`. La
  reforme fiscale 2026 eclate l'ancienne ligne TICPE (code 1501) en 4 codes
  (1501 ex-TICPE, 1502 ex-TICGN, 1503 ex-TICFE, 1504 "Autres taxes
  interieures") - les 4 sont regroupes dans le bucket TICPE existant
  (`CODE_LIGNE_RECETTE_VERS_TYPE` etendu ci-dessous) pour preserver la
  continuite de la serie dans le comparateur/historique, decision validee
  explicitement avec l'utilisateur plutot que de les laisser tomber dans
  AUTRES par defaut. L'Etat A de la LFI 2015 est exprime en MILLIERS
  d'euros (Etat B de la MEME loi deja en euros - asymetrie au sein d'un
  seul document) - voir `LFI_ETAT_A_MILLIERS_EUROS` et le parametre
  `unite_milliers` des 2 fonctions ci-dessus.
"""

from pathlib import Path

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

# Depenses 2012-2014: chacune de ces 3 annees necessite un dataset "montants"
# (deja filtre au budget general par construction, cf. docstring de module)
# et 1 ou 2 datasets de "nomenclature" (libelles/codes manquants du dataset
# de montants), tous exposes via l'API records JSON (v2.1) - PAS de piece
# jointe CSV pour cette generation de sources, contrairement a 2016-2018/
# 2020-2022. Roles par annee (voir le docstring du normaliseur associe dans
# `api.etl.normalize` pour le detail de la jointure):
#
# - 2012: "montants" (BG, sans libelle programme/action/code mission),
#   "nomenclature_mission_programme" (code programme -> code mission +
#   libelle programme, filtre BG parmi les 3 perimetres qu'il expose) et
#   "nomenclature_destination" (code programme+action -> libelle action,
#   perimetres melanges mais sans collision de code constatee).
# - 2013: memes roles "montants"/"nomenclature_destination" que 2012, mais
#   "nomenclature_programme" (PAS "nomenclature_mission_programme": aucun
#   dataset equivalent n'existe pour ce millesime - code programme ->
#   libelle programme SEUL, pas de code mission).
# - 2014: "montants" (BG, avec mission/programme/action DEJA en clair - donc
#   pas de nomenclature "programme" separee necessaire) et
#   "nomenclature_destination" (code programme+action -> libelle programme
#   ET libelle action en une seule jointure, filtrable BG via
#   `type_de_mission` - id source avec une vraie typo, "li-2014" et non
#   "lfi-2014", volontairement conservee telle quelle).
DEPENSES_DATASETS_2012_2014: dict[int, dict[str, str]] = {
    2012: {
        "montants": "lfi-2012-dotation-bg-en-ae-cp-par-mission-programme-action-et-categorie",
        "nomenclature_mission_programme": "lfi-2012-nomenclature-mission-programme",
        "nomenclature_destination": "plf-2012-nomenclature-par-destination",
    },
    2013: {
        "montants": "lfi-2013-dotation-bg-en-ae-cp-par-mission-programme-action-et-categorie",
        "nomenclature_programme": "plf-2013-budget-general-par-mission",
        "nomenclature_destination": "plf-2013-budget-general-nomenclature-par-destination",
    },
    2014: {
        "montants": "lfi-2014-dotation-bg-en-ae-cp-par-action-categorie",
        "nomenclature_destination": "li-2014-nomenclature-par-destination",
    },
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
    2012,
    2013,
    2014,
    2015,
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
    2026,
)

# Identifiant Legifrance (textCid) du texte de la LFI par annee, pour
# `POST /consult/jorf` de l'API PISTE (voir docstring de module ci-dessus).
# Sert a la fois aux depenses (Etat B) et aux recettes (Etat A) - PAS
# forcement les deux pour chaque annee: 2021 n'y figure QUE pour les
# recettes (ses depenses sont deja couvertes par
# DEPENSES_DATASETS_ATTACHMENTS[2021], une source plus ancienne et deja en
# place - le dispatch de `api.etl.run._fetch_depenses_annee` route 2021 vers
# cette branche AVANT d'atteindre celle-ci, aucun conflit).
LFI_TEXT_CID_PAR_ANNEE: dict[int, str] = {
    2015: "JORFTEXT000029988857",  # LOI n° 2014-1654 du 29 decembre 2014
    2021: "JORFTEXT000042753580",  # LOI n° 2020-1721 du 29 decembre 2020 (recettes seulement)
    2026: "JORFTEXT000053508155",  # LOI n° 2026-103 du 19 fevrier 2026
}

# Annees de recettes couvertes par la source Legifrance/PISTE - distincte de
# RECETTES_ANNEES (data.economie.gouv.fr, 2024-2025) et RECETTES_COUR_DES_
# COMPTES_ANNEES: un meme millesime ne doit jamais apparaitre dans plusieurs
# de ces 3 tuples (double traitement non gere par `api.etl.run.run_etl`).
RECETTES_LEGIFRANCE_ANNEES: tuple[int, ...] = tuple(sorted(LFI_TEXT_CID_PAR_ANNEE))

# Annees dont l'Etat A (recettes + PSR) de la source Legifrance/PISTE est
# exprime en MILLIERS d'euros plutot qu'en euros - verifie dans l'en-tete de
# la table source ("(En milliers d'euros)"). Piege reel: Etat B de la MEME
# loi 2015 est deja en euros (asymetrie au sein d'un seul document, valeurs
# de mission ~milliards coherentes seulement si NON multipliees) - ce
# multiplicateur ne doit donc JAMAIS s'appliquer aux depenses, uniquement
# aux recettes/PSR d'Etat A. A revalider explicitement (pas a supposer) pour
# toute annee future ajoutee a cette source.
LFI_ETAT_A_MILLIERS_EUROS: tuple[int, ...] = (2015,)

# Annees Legifrance necessitant un rattrapage brut/net cote recettes
# (`api.etl.run._charger_recettes_legifrance`), limite au seul programme
# "Remboursements et degrevements d'impots d'Etat" (PAS "...d'impots
# locaux") - cf. `api.etl.loader.get_remboursements_degrevements_impots_
# etat_cp`. Piege reel: ce n'est PAS une regle generale de methodologie
# budgetaire, juste une convention constatee sur cette seule annee -
# verifie explicitement pour chacune des 3 annees Legifrance disponibles
# en comparant le deficit calcule au tableau d'equilibre officiel (l'
# article qui precede immediatement Etat A dans le texte de loi): la LFI
# 2026 (article 147) a besoin de ce rattrapage cible pour retomber sur son
# solde officiel (-133,5 Md EUR, sinon ~274,7 Md EUR calcules). LES LFI
# 2015 (article 49) ET 2021 (article 93) N'ONT BESOIN D'AUCUN RATTRAPAGE -
# ni celui-ci ni la variante "mission entiere" (`api.etl.loader.
# get_remboursements_degrevements_cp`, utilisee pour la Cour des comptes):
# la somme brute de leurs lignes Etat A (hors PSR) correspond DEJA
# exactement a la ligne "recettes brutes" de leur propre tableau
# d'equilibre officiel, comparee a des depenses elles-memes brutes - la
# mission "Remboursements et degrevements" s'annule mathematiquement des
# 2 cotes de l'equation sans qu'aucun ajustement soit necessaire. Bug reel
# trouve en verifiant chaque annee individuellement (voir la docstring de
# `api.etl.run._charger_recettes_legifrance` pour le detail complet des 2
# essais errones - "mission entiere" sur 2015 donnait un surplus
# implausible, "impots d'Etat seul" sur 2021 donnait un deficit trop
# faible) plutot que de supposer qu'une convention verifiee sur une annee
# se generalise. Toute annee future ajoutee a cette source doit etre
# revalidee contre son propre article d'equilibre.
LFI_REMBOURSEMENTS_IMPOTS_ETAT_SEUL: tuple[int, ...] = (2026,)

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
#
# 1502/1503/1504 (ex-TICGN, ex-TICFE, "Autres taxes interieures"): introduits
# par la reforme fiscale 2026 qui eclate l'ancienne ligne unique TICPE
# (code 1501) en 4 codes dans l'Etat A de la LFI 2026 - regroupes ici dans
# le bucket TICPE (plutot que laisses tomber dans AUTRES par defaut) pour
# preserver la continuite de la serie TICPE dans le comparateur/historique;
# decision validee explicitement avec l'utilisateur (chantier ingestion LFI
# 2026), l'alternative (exposer les 4 lignes separement) etant jugee trop
# detaillee par rapport au reste de l'UI.
CODE_LIGNE_RECETTE_VERS_TYPE: dict[float, str] = {
    1101.0: "IR",
    1301.0: "IS",
    1601.0: "TVA",
    1501.0: "TICPE",
    1502.0: "TICPE",
    1503.0: "TICPE",
    1504.0: "TICPE",
}

# --------------------------------------------------------------------------
# Depenses 2026: fichier "LFI 2026 - Credits AE et CP votes" du ministere.
#
# L'annexe "Etat B" de la LFI publiee au Journal officiel s'arrete au
# PROGRAMME et ne donne meme pas son numero (cf. `normalize_depenses_
# legifrance`, qui doit fabriquer une cle de hachage). Le ministere publie
# separement la meme loi sous forme exploitable, avec numeros de programme et
# ventilation par action - c'est ce fichier.
#
# Somme des CP du budget general verifiee EGALE A L'EURO PRES au total tire de
# l'Etat B (593 890 071 649 EUR), ce qui confirme qu'il s'agit bien du meme
# texte vote et non du projet: le fichier PLF equivalent, lui, totalise
# 588,26 Md EUR (ecart des amendements parlementaires).
#
# Fichier VERSIONNE dans le depot plutot que telecharge a chaque run: le site
# du ministere est protege par un pare-feu applicatif qui renvoie une page
# anti-robot (212 octets de HTML) a tout client automatique, et contourner
# cette protection n'est pas une option. Ce n'est pas un risque de peremption:
# une loi de finances INITIALE est un texte definitif, son rendu chiffre ne
# change plus. Un nouvel exercice demandera le meme geste manuel, documente
# ici.
#
# Provenance: https://www.budget.gouv.fr/documentation/documents-budgetaires
# -lois/exercice-2026/loi-finances-initiale-2026-lfi (publie le 03/03/2026).
DEPENSES_LFI_XLS_PAR_ANNEE: dict[int, str] = {
    2026: "lfi_2026_credits_ae_cp_votes.xls",
}


def depenses_lfi_xls_path(annee: int) -> Path:
    """Chemin du fichier LFI exploitable embarque pour `annee`."""
    return Path(__file__).parent / "data" / DEPENSES_LFI_XLS_PAR_ANNEE[annee]


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


def legifrance_url(text_cid: str) -> str:
    """URL publique (lisible par un humain) d'un texte Legifrance, par son textCid.

    A ne PAS confondre avec l'endpoint API PISTE (`Settings.piste_api_base_url`,
    protege par OAuth) - celle-ci est la page publique du site legifrance.gouv.fr,
    utilisee comme lien de citation en frontend (`annee_budget.source_url`).
    """
    return f"https://www.legifrance.gouv.fr/jorf/id/{text_cid}"


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
    if annee in DEPENSES_DATASETS_2012_2014:
        return records_url(DEPENSES_DATASETS_2012_2014[annee]["montants"])
    if annee == 2020:
        dataset_id = DEPENSES_DATASETS_ATTACHMENTS[2020]
        return attachment_url(dataset_id, DEPENSES_ATTACHMENT_IDS[2020]["credits"])
    if annee in (2016, 2017, 2018, 2021, 2022):
        dataset_id = DEPENSES_DATASETS_ATTACHMENTS[annee]
        return attachment_url(dataset_id, DEPENSES_ATTACHMENT_IDS[annee]["detaillee"])
    if annee in LFI_TEXT_CID_PAR_ANNEE:
        return legifrance_url(LFI_TEXT_CID_PAR_ANNEE[annee])
    return None


# --------------------------------------------------------------------------
# Depenses fiscales (niches fiscales), annexe "Voies et moyens" Tome II du
# PLF - domaine independant de missions/depenses/recettes/annee_budget (les
# niches fiscales sont des allegements deja integres dans les recettes
# fiscales nettes, ne jamais les re-deduire du deficit calcule).
# --------------------------------------------------------------------------

# Seule edition disponible en format structure (xlsx) sur
# data.economie.gouv.fr: verifie par sondage direct de l'API - PLF2024/2025/
# 2026 retournent tous 404 sur ce portail (aucun dataset equivalent, non
# alimente au-dela de cette edition). Pas d'API "records" pour ce dataset
# (`has_records: false`) - uniquement le fichier xlsx en piece jointe, meme
# pattern que DEPENSES_DATASETS_ATTACHMENTS. Vue ponctuelle sur ce seul
# millesime, pas de serie historique (cf. `annee` gardee sur le modele
# `DepenseFiscale` pour une future edition, si le portail en publie une).
DEPENSE_FISCALE_DATASET_ID = "plf2023_voies_et_moyens_t2_liste_des_depenses_fiscales"
DEPENSE_FISCALE_ATTACHMENT_ID = "plf2023_voies_et_moyens_t2_liste_des_depenses_fiscales_xlsx"

# Feuille "Chiffrages" du xlsx: PAS un seul montant par mesure mais 3
# colonnes (verifie a l'inspection reelle du fichier, structure non
# documentee dans la fiche dataset) - "Realisation" annee N-2, "Prevision"
# N-1, "Prevision" N (le millesime du PLF lui-meme, ici 2023). Piege reel:
# retenir la colonne N (2023) sous-estimerait fortement le total (75,9 Md EUR
# sur cette edition, chiffre encore tres incomplet - 300/465 mesures
# chiffrees, beaucoup de "nc" a cet horizon) car une depense fiscale n'est
# definitivement connue qu'apres depouillement des declarations fiscales de
# l'annee suivante. La colonne "Realisation" N-2 (ici 2021) est la seule
# definitive (pas une prevision) et la plus complete (333/465 mesures
# chiffrees, somme 89,586 Md EUR - la plus proche de l'ordre de grandeur CDC
# "~88 Md EUR/an"): c'est elle qui est retenue, PAS le millesime du PLF
# source. Le bandeau frontend doit donc citer 2021 (dernier realise connu),
# pas 2023, comme annee des donnees.
DEPENSE_FISCALE_ANNEE = 2021


# --------------------------------------------------------------------------
# Marches publics (DECP - Donnees Essentielles de la Commande Publique)
# --------------------------------------------------------------------------

# "2022" designe la VERSION DU FORMAT LEGAL (arrete du 22/12/2022 qui definit
# le schema des colonnes), PAS une restriction d'annee - verifie directement:
# la colonne `datenotification` s'etale de 2010-06-02 a aujourd'hui,
# actualisee quotidiennement (`frequency: daily`). Le dataset frere au format
# anterieur (`decp-v3-marches-valides`, arrete 2019-03-22) est ecarte:
# schema different a reconcilier, pour un gain faible (les noms d'entreprise
# y sont NULL aussi en pratique). ~689 000 lignes, 54 colonnes source (17
# retenues, cf. `api.etl.normalize.normalize_marches_parquet`).
MARCHES_DATASET_ID = "decp-2022-marches-valides"


def parquet_export_url(dataset_id: str) -> str:
    """URL d'export Parquet en masse d'un dataset (endpoint `/exports/parquet`,
    distinct de `records_url`/`attachment_url` ci-dessus). Seul format
    pertinent pour un dataset a ~689 000 lignes: paginer via `/records` a 100
    lignes/page (cf. `api.etl.run._fetch_all_records`) prendrait ~6 900
    requetes HTTP. Verifie a l'execution reelle: 82,6 Mo pour la totalite du
    dataset marches (colonnaire + compresse), telecharge en un seul GET.
    """
    return f"{API_EXPLORE_V21}/{dataset_id}/exports/parquet"
