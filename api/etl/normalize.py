"""Normalisation des donnees brutes issues des sources ETL vers une structure
intermediaire commune, independante du format source.

Le portail data.economie.gouv.fr expose les "depenses" sous plusieurs
generations de format selon l'annee (voir `api.etl.sources` pour le detail);
ce module fournit un normaliseur par sous-format, tous convergeant vers
`DepenseRecord` puis `DepenseAggregat` (une ligne par triplet
mission/programme/action, AE/CP sommes). Les "recettes" convergent vers
`RecetteRecord` / `RecetteAggregat`.

Resolution d'identite des missions
-----------------------------------
Le libelle d'une mission peut changer d'une annee sur l'autre (ex: fusion,
renommage de ministere) alors que la mission logique reste la meme. La cle de
rapprochement prioritaire est `code_mission` (code LOLF a 2 lettres, stable
dans le temps sur tout notre perimetre 2018-2025, et egalement disponible
pour 2012 - cf. `normalize_depenses_2012` - via une nomenclature dediee). A
defaut de code (2013 et 2014, cf. `normalize_depenses_2013`/
`normalize_depenses_2014`: aucune source disponible n'expose de code mission
pour ces deux millesimes; 2016 et 2017, cf. `normalize_depenses_2016`/
`normalize_depenses_2017`: ces millesimes n'exposent qu'un libelle mission
texte, pas de code), on retombe sur le slug du libelle courant comme cle de
secours - chaque variante sans code forme alors sa propre mission logique.
Le slug etant calcule a partir du TEXTE du libelle (independamment de la
presence d'un code), il reste neanmoins souvent identique a celui deja
retenu pour les autres annees (meme cle naturelle de rapprochement) tant que
l'intitule de la mission n'a pas change d'un millesime a l'autre.

Le "libelle canonique" d'une mission logique (utilise pour `nom_normalise`
et pour calculer son `slug`) est celui de l'annee la plus recente ou elle a
ete observee: le slug reste ainsi stable dans le temps, y compris pour les
annees plus anciennes ou l'intitule affiche differait. Chaque variante de
libelle brut distincte est tracee dans `MissionAlias` avec la plage
d'annees (annee_debut/annee_fin) ou elle a ete utilisee telle quelle.
"""

from __future__ import annotations

import csv
import hashlib
import io
import logging
import re
import unicodedata
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd

from api.etl.sources import CODE_LIGNE_RECETTE_VERS_TYPE
from api.models.recette import TypeRecette

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Structures intermediaires communes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DepenseRecord:
    """Une ligne de depense brute normalisee, avant agregation par action."""

    annee: int
    mission_code: str
    mission_libelle: str
    programme_code: str
    programme_libelle: str
    action_code: str
    action_libelle: str
    ae: float
    cp: float


@dataclass(frozen=True)
class DepenseAggregat:
    """Depense agregee (AE/CP sommes) au niveau du triplet mission/programme/action."""

    annee: int
    mission_code: str
    mission_libelle: str
    programme_code: str
    programme_libelle: str
    action_code: str
    action_libelle: str
    ae: float
    cp: float


@dataclass(frozen=True)
class RecetteRecord:
    """Une ligne de recette brute normalisee, avant agregation par type."""

    annee: int
    type: TypeRecette
    montant: float


@dataclass(frozen=True)
class RecetteAggregat:
    """Recette agregee (montant somme) par (annee, type)."""

    annee: int
    type: TypeRecette
    montant_brut: float
    montant_net: float


@dataclass(frozen=True)
class PrelevementsSurRecettes:
    """Total des "prelevements sur recettes" (PSR) d'une annee, par destinataire.

    Les PSR (`type_de_recettes` commencant par "Prelevement(s)") ne sont PAS
    des recettes de l'Etat: ce sont des sommes que l'Etat retrocede
    directement aux collectivites territoriales ou a l'Union europeenne,
    prelevees sur ses propres recettes avant meme qu'elles n'apparaissent au
    budget general. Le "tableau d'equilibre" officiel du budget de l'Etat les
    presente d'ailleurs a part, en deduction des recettes brutes, jamais
    additionnees a celles-ci. Voir `extract_prelevements_sur_recettes`.
    """

    annee: int
    collectivites: float
    union_europeenne: float

    @property
    def total(self) -> float:
        return self.collectivites + self.union_europeenne


@dataclass(frozen=True)
class MissionIdentity:
    """Identite logique resolue d'une mission, stable a travers les annees."""

    slug: str
    nom_normalise: str
    code_mission: str | None


@dataclass(frozen=True)
class MissionYearRow:
    """Ligne Mission prete a l'upsert (une par mission logique x annee observee)."""

    slug: str
    nom_normalise: str
    nom_officiel: str
    annee: int
    code_mission: str | None


@dataclass(frozen=True)
class MissionAliasRow:
    """Ligne MissionAlias prete a l'upsert (un libelle brut distinct observe)."""

    nom_csv: str
    slug: str
    annee_cible: int
    annee_debut: int
    annee_fin: int


# ---------------------------------------------------------------------------
# Nettoyage numerique
# ---------------------------------------------------------------------------

# Separateurs de milliers observes dans les sources: espace normal (0x20) et
# espaces insecables (0xA0 "no-break space", 0x202F "narrow no-break space").
_THOUSANDS_SEPARATORS = (" ", "\xa0", " ")


def clean_montant(raw: float | int | str | None) -> float:
    """Nettoie un montant source: espaces de milliers, virgule ou point decimal.

    Accepte directement les nombres deja types (JSON) ainsi que les chaines
    brutes issues des CSV (ex: "484 226 865", "1 234,56"). Une valeur vide ou
    absente est traitee comme 0.
    """
    if raw is None:
        return 0.0
    if isinstance(raw, int | float):
        return float(raw)
    text = raw.strip()
    if not text:
        return 0.0
    for sep in _THOUSANDS_SEPARATORS:
        text = text.replace(sep, "")
    text = text.replace(",", ".")
    return float(text)


# ---------------------------------------------------------------------------
# Parsing HTML minimal (source Legifrance/PISTE, Etats A/B de la LFI 2026)
# ---------------------------------------------------------------------------

_TR_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.DOTALL)
_TD_RE = re.compile(r'<td(?:\s+align="([^"]*)")?[^>]*>(.*?)</td>', re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")


def _cell_text(raw_html: str) -> str:
    """Extrait le texte d'une cellule `<td>`: retire les balises HTML, espaces normalises."""
    return _TAG_RE.sub(" ", raw_html).strip()


def parse_html_table_rows(html: str) -> list[list[tuple[str, str]]]:
    """Extrait les lignes d'un tableau HTML simple en (align, texte) par cellule.

    Parseur regex minimal, volontairement sans dependance HTML (BeautifulSoup
    etc.): les tableaux Etat A/Etat B de la reponse API Legifrance sont
    verifies structurellement simples (`<tr><td align="...">...</td></tr>`,
    aucun `rowspan`/`colspan`/tableau imbrique observe) - cf. `api.etl.
    sources` (docstring de module, section LFI 2026) pour le detail de la
    structure et des regles de classification des lignes par etat.

    `align` vaut "" quand l'attribut est absent de la cellule (observe pour
    les cellules "numero de ligne" vides des lignes de categorie d'Etat A).
    """
    rows: list[list[tuple[str, str]]] = []
    for tr_match in _TR_RE.finditer(html):
        cells = [
            (align or "", _cell_text(cell_html))
            for align, cell_html in _TD_RE.findall(tr_match.group(1))
        ]
        if cells:
            rows.append(cells)
    return rows


# ---------------------------------------------------------------------------
# Slug / identite des missions
# ---------------------------------------------------------------------------


def _slugify(text: str) -> str:
    """Transforme un libelle en slug kebab-case (minuscules, sans accents)."""
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.lower()
    ascii_text = re.sub(r"[^a-z0-9]+", "-", ascii_text)
    return ascii_text.strip("-")


def normalize_mission_name(nom_csv: str, annee: int) -> str:
    """Normalise un libelle de mission en slug kebab-case.

    Le libelle passe doit etre le plus recent connu pour le `code_mission` de
    cette mission logique (resolu par `resolve_mission_identities`), afin que
    le slug reste stable meme quand l'intitule change d'une annee sur
    l'autre. Le parametre `annee` n'intervient pas dans le calcul lui-meme;
    il est conserve pour la tracabilite des appels et des logs.
    """
    return _slugify(nom_csv)


def mission_key(mission_code: str, mission_libelle: str) -> str:
    """Cle de rapprochement d'une mission: code_mission, sinon slug du libelle."""
    return mission_code or _slugify(mission_libelle)


def resolve_mission_identities(
    depenses_par_annee: dict[int, list[DepenseAggregat]],
) -> dict[str, MissionIdentity]:
    """Resout l'identite logique de chaque mission a travers les annees fournies.

    Retourne un dict {cle: MissionIdentity} ou `cle` est celle produite par
    `mission_key` (code_mission, ou slug de secours). Le libelle canonique
    retenu est celui de l'annee la plus recente observee pour cette cle.
    """
    libelles_par_cle: dict[str, dict[int, str]] = defaultdict(dict)
    code_par_cle: dict[str, str | None] = {}

    for annee, aggregats in depenses_par_annee.items():
        for agg in aggregats:
            cle = mission_key(agg.mission_code, agg.mission_libelle)
            libelles_par_cle[cle][annee] = agg.mission_libelle
            if agg.mission_code:
                code_par_cle[cle] = agg.mission_code
            else:
                code_par_cle.setdefault(cle, None)

    identites: dict[str, MissionIdentity] = {}
    for cle, libelles in libelles_par_cle.items():
        derniere_annee = max(libelles)
        libelle_canonique = libelles[derniere_annee]
        identites[cle] = MissionIdentity(
            slug=_slugify(libelle_canonique),
            nom_normalise=libelle_canonique,
            code_mission=code_par_cle.get(cle),
        )
    return identites


def build_mission_rows(
    depenses_par_annee: dict[int, list[DepenseAggregat]],
    identites: dict[str, MissionIdentity],
) -> list[MissionYearRow]:
    """Construit une ligne Mission par (mission logique, annee ou elle apparait)."""
    rows: list[MissionYearRow] = []
    for annee, aggregats in depenses_par_annee.items():
        vues: set[str] = set()
        for agg in aggregats:
            cle = mission_key(agg.mission_code, agg.mission_libelle)
            if cle in vues:
                continue
            vues.add(cle)
            identite = identites[cle]
            rows.append(
                MissionYearRow(
                    slug=identite.slug,
                    nom_normalise=identite.nom_normalise,
                    nom_officiel=agg.mission_libelle,
                    annee=annee,
                    code_mission=identite.code_mission,
                )
            )
    return rows


def build_mission_alias_rows(
    depenses_par_annee: dict[int, list[DepenseAggregat]],
    identites: dict[str, MissionIdentity],
) -> list[MissionAliasRow]:
    """Construit les alias (libelle brut -> mission logique) avec leur plage d'annees.

    Un alias est cree par couple (mission logique, libelle brut distinct);
    `annee_debut`/`annee_fin` bornent les annees ou ce libelle exact a ete
    observe. L'alias est rattache (via `annee_cible`) au Mission de l'annee
    `annee_fin`, ou `nom_officiel` est litteralement egal a `nom_csv`.
    """
    occurences: dict[tuple[str, str], list[int]] = defaultdict(list)
    for annee, aggregats in depenses_par_annee.items():
        vues: set[tuple[str, str]] = set()
        for agg in aggregats:
            cle = mission_key(agg.mission_code, agg.mission_libelle)
            paire = (cle, agg.mission_libelle)
            if paire in vues:
                continue
            vues.add(paire)
            occurences[paire].append(annee)

    rows: list[MissionAliasRow] = []
    for (cle, libelle), annees in occurences.items():
        identite = identites[cle]
        rows.append(
            MissionAliasRow(
                nom_csv=libelle,
                slug=identite.slug,
                annee_cible=max(annees),
                annee_debut=min(annees),
                annee_fin=max(annees),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Depenses: normaliseurs par sous-format
# ---------------------------------------------------------------------------


def _format_code(value: Any) -> str:
    """Normalise un code source qui peut arriver en str ou en float (JSON).

    L'API renvoie certains codes numeriques (ex: programme) en JSON comme
    nombre flottant (ex: 166.0) plutot que comme chaine ("166"): on les
    reconvertit en entier avant stringification pour eviter le suffixe
    ".0".
    """
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def _build_action_libelle_par_destination(
    nomenclature_destination: list[dict[str, Any]],
) -> dict[tuple[str, str], str]:
    """Construit un mapping (code programme, code action 2 chiffres) -> libelle action.

    Utilise par `normalize_depenses_2012`/`normalize_depenses_2013`: dans ces
    deux millesimes, le dataset "nomenclature par destination" (`mission`,
    `programme`, `action`, `libelle_action`, ...) porte plusieurs lignes par
    action distincte (une par sous-action, cf. colonne `s_action`/
    `sous_action`) qui partagent toutes le meme `libelle_action` - verifie
    exhaustivement a l'execution reelle sur les deux millesimes (aucun
    contre-exemple): on ne garde donc que la PREMIERE ligne rencontree pour
    chaque cle. `action` arrive en JSON comme entier (ex: 8) alors que le
    dataset de montants l'exprime en chaine 2 chiffres ("08"): normalise ici
    via `_format_code` puis `zfill(2)` pour que la cle matche.

    Ce dataset n'est PAS filtre par perimetre budgetaire (il melange BG/CAS/
    CCF, contrairement au dataset de montants qui lui est BG pur): verifie a
    l'execution reelle qu'aucune paire (programme, action) n'y est partagee
    entre deux missions de perimetres differents - la jointure par
    (programme, action) seul, sans le libelle mission, est donc sans
    ambiguite malgre l'absence de filtre BG explicite ici.
    """
    out: dict[tuple[str, str], str] = {}
    for row in nomenclature_destination:
        programme_code = _format_code(row["programme"])
        action_raw = row.get("action")
        if action_raw is None:
            continue
        action_code = _format_code(action_raw).zfill(2)
        out.setdefault((programme_code, action_code), row["libelle_action"])
    return out


def normalize_depenses_2012(
    montants: list[dict[str, Any]],
    nomenclature_mission_programme: list[dict[str, Any]],
    nomenclature_destination: list[dict[str, Any]],
    annee: int = 2012,
) -> list[DepenseRecord]:
    """Normalise le format 2012: dataset "dotation BG" (montants) + 2 nomenclatures a joindre.

    Source des montants: `lfi-2012-dotation-bg-en-ae-cp-par-mission-
    programme-action-et-categorie` (API records JSON, PAS une piece jointe
    CSV comme 2016-2018/2020-2022). Deja filtre au budget general par
    CONSTRUCTION (pas de colonne perimetre a filtrer ici): verifie a
    l'execution reelle - ses 32 missions distinctes sont exactement les
    missions BG connues, aucune mission CAS/CCF (ex "Pensions", "Avances...")
    n'y apparait. Chaque ligne expose un libelle mission en texte brut (PAS
    de code), un CODE programme et un CODE action bruts (SANS libelle
    programme ni libelle action), et le detail par `categorie` (comme
    `normalize_depenses_2016`/`2017`: plusieurs lignes categorie par action,
    sommees par `aggregate_depenses`). Colonne montant retenue: `aelf`/
    `cplf` (LFI VOTEE finale, derniere colonne de chaque bloc - PAS `aelf_
    2011`/`cplf_2011` (LFI annee precedente), `aeplf` (avant-projet) ni
    `aeamt` (amendements seuls, deja inclus dans `aelf`/`cplf`)).

    Deux jointures completent les libelles/codes manquants:

    1. `nomenclature_mission_programme` (`lfi-2012-nomenclature-mission-
       programme`): mapping (code programme) -> (code mission LOLF 2
       lettres, libelle programme), apres filtrage sur `type_de_budget ==
       "Budget général"` (ce dataset melange BG/CAS/CCF, la colonne permet
       d'isoler le perimetre voulu - verifie a l'execution reelle qu'aucun
       code programme n'est partage entre deux perimetres differents sur ce
       millesime, la restriction BG est donc sans ambiguite malgre son
       absence de la cle de jointure elle-meme). Fournit `code_mission`,
       CONTRAIREMENT a 2013/2014 (`normalize_depenses_2013`/
       `normalize_depenses_2014`) dont aucune source disponible n'expose de
       code mission LOLF: 2012 est donc la seule des 3 annees ou la
       resolution d'identite de mission (`resolve_mission_identities`) peut
       s'appuyer sur un code plutot que sur le repli slug.
    2. `nomenclature_destination` (`plf-2012-nomenclature-par-destination`,
       via `_build_action_libelle_par_destination`): mapping (code
       programme, code action) -> libelle action.

    Taux de correspondance constate a l'execution reelle (aout 2026, sur les
    2076 lignes de montants): 2076/2076 (100%) trouvent leur (code_mission,
    libelle programme) via la nomenclature mission-programme; 2014/2076
    (97%, 62 lignes orphelines sur 19 paires (programme, action) distinctes
    - concentrees sur les programmes 137/149/166/169/180/216/308/309)
    trouvent un libelle d'action via la nomenclature destination. Les lignes
    orphelines d'un cote ou de l'autre replient sur le CODE brut comme
    libelle (comme le repli deja utilise par `normalize_depenses_2016` pour
    `programme_libelle` en l'absence totale de nomenclature programme): le
    MONTANT de ces lignes n'est pas affecte, seul l'affichage du libelle en
    est degrade - juge acceptable au vu du taux de correspondance tres
    majoritaire.
    """
    prog_info: dict[str, tuple[str, str]] = {}
    for row in nomenclature_mission_programme:
        if row.get("type_de_budget") != "Budget général":
            continue
        prog_info[row["code_de_programme"]] = (row["code_de_mission"], row["programme"])

    action_libelle = _build_action_libelle_par_destination(nomenclature_destination)

    out: list[DepenseRecord] = []
    for row in montants:
        programme_code = _format_code(row["programme"])
        action_code = _format_code(row["action"]).zfill(2)
        mission_code, programme_libelle = prog_info.get(programme_code, ("", programme_code))
        out.append(
            DepenseRecord(
                annee=annee,
                mission_code=mission_code,
                mission_libelle=row["mission"],
                programme_code=programme_code,
                programme_libelle=programme_libelle,
                action_code=f"{programme_code}-{action_code}",
                action_libelle=action_libelle.get((programme_code, action_code), action_code),
                ae=clean_montant(row.get("aelf")),
                cp=clean_montant(row.get("cplf")),
            )
        )
    return out


def normalize_depenses_2013(
    montants: list[dict[str, Any]],
    nomenclature_programme: list[dict[str, Any]],
    nomenclature_destination: list[dict[str, Any]],
    annee: int = 2013,
) -> list[DepenseRecord]:
    """Normalise le format 2013: meme famille que 2012, sans code mission disponible.

    Source des montants: `lfi-2013-dotation-bg-en-ae-cp-par-mission-
    programme-action-et-categorie` (API records JSON), structure identique a
    2012 (`normalize_depenses_2012`): libelle mission en texte brut, codes
    programme/action bruts, decomposition par `categorie` sommee par
    `aggregate_depenses`, colonnes montant `aelf`/`cplf` (LFI votee finale).

    A la difference de 2012, AUCUN dataset "nomenclature-mission-programme"
    equivalent n'existe pour 2013 sur ce portail (verifie par recherche
    exhaustive du catalogue complet, aout 2026: aucun dataset dont l'id ou le
    titre associe "2013" et "nomenclature"+"mission"+"programme"): pas de
    code mission LOLF disponible pour ce millesime, `mission_code` reste
    donc systematiquement vide (comme `normalize_depenses_2016`/`2017`) -
    resolution d'identite de mission repliee sur le slug du libelle (cf.
    docstring du module).

    Deux nomenclatures BG-dediees comblent les libelles manquants:

    1. `nomenclature_programme` (`plf-2013-budget-general-par-mission`,
       malgre son nom trompeur - inspection reelle du contenu confirme qu'il
       s'agit d'un mapping PAR PROGRAMME: 125 lignes pour 125 programmes,
       champs `programme` (code) + `libelle` (libelle du programme, PAS de
       la mission)): mapping (code programme) -> libelle programme. 100% des
       125 codes programme du dataset de montants s'y retrouvent (verifie a
       l'execution reelle).
    2. `nomenclature_destination` (`plf-2013-budget-general-nomenclature-
       par-destination`, via `_build_action_libelle_par_destination`):
       mapping (code programme, code action) -> libelle action.

    Taux de correspondance constate a l'execution reelle (aout 2026, sur les
    2147 lignes de montants): 2147/2147 (100%) trouvent leur libelle
    programme; 2027/2147 (94%, 120 lignes orphelines sur 30 paires
    (programme, action) distinctes - concentrees sur les programmes
    124/144/164/212/224, dont le dataset de montants LFI expose davantage
    d'actions que la nomenclature PLF de destination) trouvent un libelle
    d'action. Meme repli sur le code brut que `normalize_depenses_2012` pour
    les lignes orphelines (montant non affecte, seul le libelle l'est).
    """
    prog_libelle: dict[str, str] = {
        _format_code(row["programme"]): row["libelle"] for row in nomenclature_programme
    }
    action_libelle = _build_action_libelle_par_destination(nomenclature_destination)

    out: list[DepenseRecord] = []
    for row in montants:
        programme_code = _format_code(row["programme"])
        action_code = _format_code(row["action"]).zfill(2)
        out.append(
            DepenseRecord(
                annee=annee,
                mission_code="",
                mission_libelle=row["mission"],
                programme_code=programme_code,
                programme_libelle=prog_libelle.get(programme_code, programme_code),
                action_code=f"{programme_code}-{action_code}",
                action_libelle=action_libelle.get((programme_code, action_code), action_code),
                ae=clean_montant(row.get("aelf")),
                cp=clean_montant(row.get("cplf")),
            )
        )
    return out


def normalize_depenses_2014(
    montants: list[dict[str, Any]],
    nomenclature_destination: list[dict[str, Any]],
    annee: int = 2014,
) -> list[DepenseRecord]:
    """Normalise le format 2014: dataset "dotation BG" + 1 seule nomenclature BG-filtree.

    Source des montants: `lfi-2014-dotation-bg-en-ae-cp-par-action-
    categorie` (API records JSON, ~2046 lignes). Deja BG par construction
    (pas de colonne perimetre a filtrer), avec `mission`/`programme`/
    `action` DEJA presents en clair sur chaque ligne (contrairement a 2012/
    2013: seuls les LIBELLES programme/action manquent encore, pas les
    codes). Decomposition par `categorie` (int), sommee par
    `aggregate_depenses` comme les autres millesimes.

    Piege de qualite de donnees VERIFIE a l'execution reelle (aout 2026): les
    colonnes CP (`cplf_2013`, `cpplf`, `cpamt`, `cplf`) sont typees TEXTE
    avec un separateur de milliers en ESPACE ASCII normal (0x20) integre a
    la chaine (ex: `"1 112 702"`), alors que les colonnes AE equivalentes
    (`aelf_2013`, `aeplf`, `aeamt`, `aelf`) sont des entiers JSON propres.
    `clean_montant` gere DEJA ce cas SANS modification necessaire: son
    tuple `_THOUSANDS_SEPARATORS` inclut l'espace ASCII normal (`" "`, le
    tout premier element) en plus des espaces insecables - confirme par test
    direct sur ce millesime (`clean_montant("1 112 702") == 1112702.0`).
    Aucune extension du nettoyage numerique n'a donc ete necessaire, malgre
    l'hypothese initiale. Le dataset porte aussi des colonnes fantomes
    `column_14` a `column_22`, TOUJOURS nulles (verifie exhaustivement sur
    les 2046 lignes): ignorees ici, jamais lues.

    Libelles programme ET action: `nomenclature_destination` (`li-2014-
    nomenclature-par-destination` - id source avec une VRAIE typo ("li-2014"
    et non "lfi-2014"), volontairement PAS "corrigee" ici, l'id exact
    existant reellement dans le catalogue ayant ete verifie par requete
    directe), apres filtrage sur `type_de_mission == "Budget général"` (ce
    dataset melange BG/CAS/CCF; contrairement a 2012/2013, la colonne
    perimetre EST presente sur ce dataset de nomenclature - filtrage direct,
    sans avoir besoin de verifier l'absence de collision de code). Fournit
    en une seule jointure, par (code_programme, code_action), a la fois le
    libelle programme (`programme`) et le libelle action (`action` - le nom
    de colonne de ce dataset, malgre son nom, porte le LIBELLE, pas le code:
    le code est dans `code_action`).

    AUCUN code mission LOLF disponible pour ce millesime (ce dataset expose
    `type_de_mission`, un texte de PERIMETRE budgetaire BG/CAS/CCF, PAS un
    code mission a 2 lettres): `mission_code` reste vide comme 2013, meme
    repli sur le slug pour la resolution d'identite (cf. docstring du
    module).

    Taux de correspondance constate a l'execution reelle (aout 2026, sur les
    2046 lignes de montants): 2036/2046 (99.5%, 10 lignes orphelines sur 4
    paires (programme, action) distinctes) trouvent leur libelle programme
    et action simultanement via cette unique jointure. Meme repli sur le
    code brut que 2012/2013 pour les lignes orphelines (montant non
    affecte).
    """
    prog_action_libelle: dict[tuple[str, str], tuple[str, str]] = {}
    for row in nomenclature_destination:
        if row.get("type_de_mission") != "Budget général":
            continue
        key = (_format_code(row["code_programme"]), _format_code(row["code_action"]).zfill(2))
        prog_action_libelle.setdefault(key, (row["programme"], row["action"]))

    out: list[DepenseRecord] = []
    for row in montants:
        programme_code = _format_code(row["programme"])
        action_code = _format_code(row["action"]).zfill(2)
        programme_libelle, action_libelle = prog_action_libelle.get(
            (programme_code, action_code), (programme_code, action_code)
        )
        out.append(
            DepenseRecord(
                annee=annee,
                mission_code="",
                mission_libelle=row["mission"],
                programme_code=programme_code,
                programme_libelle=programme_libelle,
                action_code=f"{programme_code}-{action_code}",
                action_libelle=action_libelle,
                ae=clean_montant(row.get("aelf")),
                cp=clean_montant(row.get("cplf")),
            )
        )
    return out


def normalize_depenses_records_json(
    records: list[dict[str, Any]], annee: int
) -> list[DepenseRecord]:
    """Normalise les enregistrements de l'API records (2019, 2023-2025).

    Ce portail n'expose PAS un format JSON uniforme pour 2023-2025: seul le
    dataset "*-selon-destination" (2025, et son equivalent "plf24" s'il
    existait) suit le format "propre" documente dans le CDC. Les datasets
    2023 ("credits-ae-et-cp-votes...") et 2024
    ("plf-2024-depenses-2024-selon-nomenclatures-destination-et-nature")
    suivent en realite un schema plus proche des fichiers CSV "detaillee"
    (constate a l'execution reelle, corrige ici par rapport a l'hypothese
    initiale du CDC qui supposait a tort un schema identique a 2025):

    - 2025 ("selon-destination"): cle `typebudget`, codes `mission`/
      `programme`/`action` deja au format canonique ("103-01" pour action),
      montants `autorisation_engagement`/`credit_de_paiement`.
    - 2019: cle `type_de_budget_hors_budgets_annexes` (valeur texte "Budget
      général"), codes separes `code_mission`/`code_programme`/
      `code_action` (action non prefixee par le programme), montants
      `ae_lfi_2019`/`cp_lfi_2019`.
    - 2024: cle `type_mission`, `code_mission` separe, `programme` en
      nombre (ex 224.0), `action` non prefixee, montants `ae_plf`/`cp_plf`
      (pas de decomposition T2/HT2 exposee pour cette annee).
    - 2023: cle `type_mission`, `code_mission` separe, `programme` en
      nombre, `action` non prefixee. Les colonnes calculees
      `ae_t2_hors_t2_lfi_2023`/`cp_t2_hors_t2_lfi_2023` cense fournir le
      total T2+HT2 se sont averees cassees a l'execution reelle (valeur
      constante 4.0 sur toutes les lignes, quelle que soit la mission): le
      montant est donc recalcule ici comme PLF + amendements
      (`*_plf_2023` + `*_amendements_2023`, T2 et hors T2), qui correspond
      a la definition de la LFI votee.

    Dans tous les cas, seul le perimetre "budget general" (BG) est retenu,
    coherent avec les recettes qui elles aussi ne couvrent que le budget
    general.
    """
    out: list[DepenseRecord] = []
    for row in records:
        if "typebudget" in row:  # 2025 "selon-destination"
            if row.get("typebudget") != "BG":
                continue
            mission_code = row["mission"]
            mission_libelle = row["libelle_mission"]
            programme_code = _format_code(row["programme"])
            programme_libelle = row["libelle_programme"]
            action_code = row["action"]
            action_libelle = row["libelle_action"]
            ae = clean_montant(row.get("autorisation_engagement"))
            cp = clean_montant(row.get("credit_de_paiement"))
        elif "type_de_budget_hors_budgets_annexes" in row:  # 2019
            if row.get("type_de_budget_hors_budgets_annexes") != "Budget général":
                continue
            mission_code = row["code_mission"]
            mission_libelle = row["mission"]
            programme_code = _format_code(row["code_programme"])
            programme_libelle = row["programme"]
            # Uniformise le format du code action ("PPP-AA") avec les autres
            # generations de source, qui prefixent deja par le code programme.
            action_code = f"{programme_code}-{row['code_action']}"
            action_libelle = row["action"]
            ae = clean_montant(row.get("ae_lfi_2019"))
            cp = clean_montant(row.get("cp_lfi_2019"))
        elif "type_mission" in row:  # 2023-2024, schema "detaillee"-like
            if row.get("type_mission") != "BG":
                continue
            mission_code = row["code_mission"]
            mission_libelle = row["mission"]
            programme_code = _format_code(row["programme"])
            programme_libelle = row["libelle_programme"]
            action_code = f"{programme_code}-{row['action']}"
            action_libelle = row["libelle_action"]
            if "ae_plf" in row:  # 2024: pas de decomposition T2/HT2 exposee
                ae = clean_montant(row.get("ae_plf"))
                cp = clean_montant(row.get("cp_plf"))
            else:
                # 2023: les colonnes calculees "*_lfi_2023" (et "check_*")
                # sont un champ casse cote source: elles valent
                # systematiquement 4.0 (ou 0.0) quelle que soit la ligne,
                # constate a l'execution reelle. Le montant LFI est donc
                # recalcule ici comme PLF + amendements (T2 + hors T2), ce
                # qui correspond a la definition de la LFI votee (texte du
                # PLF tel qu'amende en cours de discussion parlementaire).
                ae = (
                    clean_montant(row.get("ae_t2_plf_2023"))
                    + clean_montant(row.get("ae_hors_t2_plf_2023"))
                    + clean_montant(row.get("ae_t2_amendements_2023"))
                    + clean_montant(row.get("ae_hors_t2_amendements_2023"))
                )
                cp = (
                    clean_montant(row.get("cp_t2_plf_2023"))
                    + clean_montant(row.get("cp_hors_t2_plf_2023"))
                    + clean_montant(row.get("cp_t2_amendements_2023"))
                    + clean_montant(row.get("cp_hors_t2_amendements_2023"))
                )
        else:
            raise ValueError(f"Format JSON depenses non reconnu pour l'annee {annee}: {row!r}")

        out.append(
            DepenseRecord(
                annee=annee,
                mission_code=mission_code,
                mission_libelle=mission_libelle,
                programme_code=programme_code,
                programme_libelle=programme_libelle,
                action_code=action_code,
                action_libelle=action_libelle,
                ae=ae,
                cp=cp,
            )
        )
    return out


def normalize_depenses_2016(csv_text: str, annee: int = 2016) -> list[DepenseRecord]:
    """Normalise le format 2016: piece jointe "BG-Action_Titre" dediee (BG only).

    A la difference de 2018+ (fichier unique BG+CAS+CCF fusionne, filtre par
    une colonne 'Type de Budget'/'Type Mission'), le portail expose ici un
    fichier PAR perimetre budgetaire (BG/CAS/CCF sont 3 pieces jointes
    distinctes du meme dataset, cf. `api.etl.sources.
    DEPENSES_ATTACHMENT_IDS[2016]`): le filtrage BG est donc fait en amont,
    par le CHOIX de la piece jointe (celle-ci), pas par une colonne a
    l'interieur du fichier - il n'y en a pas.

    3 lignes d'en-tete parasites precedent la vraie ligne de colonnes
    ("Gestion;2016;...", "Type Budget;Budget general;...", une ligne
    quasi-vide portant juste un marqueur "Donnees") - `lignes[4:]` les saute
    pour atteindre les donnees (la ligne de colonnes elle-meme, `lignes[3]`,
    n'est pas utilisee: les colonnes sont adressees par position, pas par
    nom, cf. ci-dessous).

    Colonnes reelles (piece "Action_Titre", verifiees a l'inspection reelle
    aout 2026): Mission;Programme;Action;Libelle action;Titre;AELF 2015;
    AEPLF;AEAMT;AELF;CPLF 2015;CPPLF;CPAMT;CPLF. Pas de code mission (le
    'Mission' est un libelle texte brut) ni de libelle programme: seul
    `mission_libelle` est exploitable comme texte, `programme_libelle` est
    donc replie sur le code programme lui-meme (aucune autre piece jointe du
    dataset ne fournit de nomenclature programme separee - verifie a
    l'inspection reelle des 18 pieces jointes du dataset 2016) - cf. le
    docstring du module sur la resolution d'identite par slug quand
    `code_mission` est absent. `Titre` (T2/T3/T5/T6/T7...) est une
    decomposition plus fine que l'action, comme la 'Categorie' des autres
    formats: plusieurs lignes Titre pour une meme action sont sommees par
    `aggregate_depenses`; seule la PREMIERE ligne Titre d'une action porte
    'Libelle action' (les suivantes l'ont vide, artefact d'export en
    cellules fusionnees) - verifie exhaustivement a l'inspection reelle (575
    actions distinctes, 0 libelle vide sur la premiere occurrence): sans
    risque donc de laisser `action_libelle` vide, la logique de merge de
    `aggregate_depenses` (garde les libelles de la premiere ligne rencontree
    pour une cle donnee) suffit, un repli sur le code n'est prevu que par
    securite si jamais elle etait vide.

    Colonne montant retenue: 'AELF'/'CPLF' (derniere colonne de chaque bloc,
    LFI VOTEE finale) - PAS 'AELF 2015' (LFI de l'annee precedente, un
    simple repere historique), 'AEPLF' (avant-projet) ni 'AEAMT'
    (amendements seuls, deja inclus dans le montant final).
    """
    lignes = list(csv.reader(io.StringIO(csv_text), delimiter=";"))
    out: list[DepenseRecord] = []
    for row in lignes[4:]:
        if len(row) < 13 or not row[0].strip():
            continue
        mission_libelle = row[0]
        programme_code = row[1]
        action_code_brut = row[2]
        action_libelle = row[3] or action_code_brut
        # row[4] = code Titre (T2/T3/...), decomposition plus fine que
        # l'action, volontairement ignoree ici (sommee via aggregate_depenses).
        ae = clean_montant(row[8])
        cp = clean_montant(row[12])
        out.append(
            DepenseRecord(
                annee=annee,
                mission_code="",
                mission_libelle=mission_libelle,
                programme_code=programme_code,
                programme_libelle=programme_code,
                action_code=f"{programme_code}-{action_code_brut}",
                action_libelle=action_libelle,
                ae=ae,
                cp=cp,
            )
        )
    return out


def normalize_depenses_2017(csv_text: str, annee: int = 2017) -> list[DepenseRecord]:
    """Normalise le format 2017: piece jointe "BG-Action_Categorie" dediee (BG only).

    Meme principe que 2016 (`normalize_depenses_2016`) pour le filtrage BG:
    une piece jointe dediee par perimetre budgetaire, pas de colonne a
    filtrer dans le fichier lui-meme. Contrairement a 2016, PAS de lignes
    d'en-tete parasites: la premiere ligne est directement la ligne de
    colonnes.

    Colonnes reelles (verifiees a l'inspection reelle aout 2026): ANNEE;
    Mission;Programme;Libelle;Action;Libelle;Categorie;Libelle;AE-LF_N1;
    AE-PLF;AE-AMT;AE-LF;CP-LF_N1;CP-PLF;CP-AMT;CP-LF. Le libelle 'Libelle'
    est REPETE 3 fois (programme/action/categorie): `csv.DictReader`
    collapserait ces 3 colonnes en une seule cle (dict, cle dupliquee =
    derniere valeur gagne), perdant les libelles programme et action - le
    parsing se fait donc par INDICE de colonne (`csv.reader`), pas par nom.
    A la difference de 2016, ce format fournit un vrai libelle programme
    (colonne 3): `programme_libelle` n'a donc pas besoin de repli sur le
    code ici. 'Categorie' (colonnes 6/7) est une decomposition plus fine que
    l'action (comme 'Titre' en 2016): plusieurs lignes Categorie pour une
    meme action sont sommees par `aggregate_depenses`.

    Colonne montant retenue: 'AE-LF'/'CP-LF' (derniere colonne de chaque
    bloc, LFI VOTEE finale) - PAS 'AE-LF_N1' (LFI annee precedente),
    'AE-PLF' (avant-projet) ni 'AE-AMT' (amendements seuls).
    """
    lignes = list(csv.reader(io.StringIO(csv_text), delimiter=";"))
    out: list[DepenseRecord] = []
    for row in lignes[1:]:
        if len(row) < 16 or not row[1].strip():
            continue
        mission_libelle = row[1]
        programme_code = row[2]
        programme_libelle = row[3]
        action_code_brut = row[4]
        action_libelle = row[5]
        # row[6]/row[7] = code/libelle Categorie, decomposition plus fine
        # que l'action, volontairement ignoree ici (sommee via aggregate_depenses).
        ae = clean_montant(row[11])
        cp = clean_montant(row[15])
        out.append(
            DepenseRecord(
                annee=annee,
                mission_code="",
                mission_libelle=mission_libelle,
                programme_code=programme_code,
                programme_libelle=programme_libelle,
                action_code=f"{programme_code}-{action_code_brut}",
                action_libelle=action_libelle,
                ae=ae,
                cp=cp,
            )
        )
    return out


def normalize_depenses_2020(
    nomenclature_csv: str, credits_csv: str, annee: int = 2020
) -> list[DepenseRecord]:
    """Normalise le format 2020: deux fichiers CSV a joindre par code.

    `nomenclature_csv` fournit les libelles (Mission/PGM/ACT) par code sans
    montants; `credits_csv` fournit les codes et montants AE/CP sans
    libelles. Seules les lignes du budget general (typeBudget == 'BG') sont
    retenues, dans les deux fichiers (un meme code peut exister sous
    plusieurs perimetres budgetaires).
    """
    libelles: dict[str, dict[str, str]] = {"Mission": {}, "PGM": {}, "ACT": {}}
    for row in csv.DictReader(io.StringIO(nomenclature_csv), delimiter=";"):
        type_ligne = row.get("Type ligne")
        if type_ligne not in libelles:
            continue
        if row.get("Type Budget") != "BG":
            continue
        libelles[type_ligne][row["code"]] = row["Libelle"]

    out: list[DepenseRecord] = []
    for row in csv.DictReader(io.StringIO(credits_csv), delimiter=";"):
        if row.get("typeBudget") != "BG":
            continue
        mission_code = row["mission"]
        programme_code = row["programme"]
        action_code = row["action"]
        out.append(
            DepenseRecord(
                annee=annee,
                mission_code=mission_code,
                mission_libelle=libelles["Mission"].get(mission_code, mission_code),
                programme_code=programme_code,
                programme_libelle=libelles["PGM"].get(programme_code, programme_code),
                action_code=action_code,
                action_libelle=libelles["ACT"].get(action_code, action_code),
                ae=clean_montant(row.get("AE LFI 2020")),
                cp=clean_montant(row.get("CP LFI 2020")),
            )
        )
    return out


def normalize_depenses_2018(csv_text: str, annee: int = 2018) -> list[DepenseRecord]:
    """Normalise le format 2018: un seul fichier CSV, BG+CAS+CCF deja fusionnes.

    A la difference du format "detaillee" 2021-2022 (colonnes calculees "AE
    ( T2 + HT2) LFI" / "CP ( T2 + HT2) LFI", qui totalisent deja titre 2 +
    hors titre 2), ce fichier expose directement les montants finaux par
    ligne categorie/titre dans les colonnes `AE LFI 2018`/`CP LFI 2018`, sans
    decomposition a recombiner: `aggregate_depenses` se charge de sommer ces
    lignes (une par categorie de depense, ex "21 - Remunerations d'activite")
    au niveau de l'action.

    Le perimetre budgetaire n'est pas porte par une colonne 'Type Mission'
    (BG/CAS/CCF/BA en 2 lettres, comme 2021-2022) mais par 'Type de Budget
    (Hors Budgets annexes)' (valeur texte, comme le dataset JSON 2019):
    seule la valeur 'Budget général' est retenue, les 'Comptes d'affectation
    speciale' et 'Comptes de concours financiers' presents dans ce meme
    fichier sont exclus - coherent avec toutes les autres annees deja en
    base, qui ne couvrent elles aussi que le budget general.
    """
    out: list[DepenseRecord] = []
    for row in csv.DictReader(io.StringIO(csv_text), delimiter=";"):
        if row.get("Type de Budget (Hors Budgets annexes)") != "Budget général":
            continue
        programme_code = row["Code Programme"]
        # Uniformise le format du code action ("PPP-AA") avec les autres
        # generations de source, qui prefixent deja par le code programme.
        action_code = f"{programme_code}-{row['Code Action']}"
        out.append(
            DepenseRecord(
                annee=annee,
                mission_code=row["Code Mission"],
                mission_libelle=row["Mission"],
                programme_code=programme_code,
                programme_libelle=row["Programme"],
                action_code=action_code,
                action_libelle=row["Action"],
                ae=clean_montant(row.get("AE LFI 2018")),
                cp=clean_montant(row.get("CP LFI 2018")),
            )
        )
    return out


def normalize_depenses_attachment_detaillee(csv_text: str, annee: int) -> list[DepenseRecord]:
    """Normalise le format "detaillee" 2021-2022: un seul fichier, libelles inclus.

    Utilise les colonnes totales `AE ( T2 + HT2) LFI` / `CP ( T2 + HT2) LFI`
    (derniere colonne de chaque bloc AE/CP) comme montants AE/CP, qui
    incluent deja la decomposition titre 2 (personnel) / hors titre 2. Seul
    le budget general (colonne 'Type Mission' == 'BG') est retenu.
    """
    out: list[DepenseRecord] = []
    for row in csv.DictReader(io.StringIO(csv_text), delimiter=";"):
        if row.get("Type Mission") != "BG":
            continue
        programme_code = row["Programme"]
        # Uniformise le format du code action ("PPP-AA") avec les autres
        # generations de source.
        action_code = f"{programme_code}-{row['Action']}"
        out.append(
            DepenseRecord(
                annee=annee,
                mission_code=row["Code Mission"],
                mission_libelle=row["Mission"],
                programme_code=programme_code,
                programme_libelle=row["Libellé Programme"],
                action_code=action_code,
                action_libelle=row["Libellé Action"],
                ae=clean_montant(row.get("AE ( T2 + HT2) LFI")),
                cp=clean_montant(row.get("CP ( T2 + HT2) LFI")),
            )
        )
    return out


def normalize_depenses_2026(etat_b_html: str, annee: int = 2026) -> list[DepenseRecord]:
    """Normalise l'Etat B (repartition par mission et programme des credits du
    budget general) de la LFI 2026 (et, potentiellement, des annees
    suivantes au meme format) en `DepenseRecord`.

    `etat_b_html` est le fragment HTML de la table "I." (deja isole par
    `api.etl.run._extract_etats_html` - Etat B ne contient qu'une seule
    table dans cette generation de source, contrairement a Etat A qui en
    contient plusieurs concatenees, cf. `api.etl.sources`).

    Mission et Programme sont distingues par l'attribut `align` de la
    premiere cellule de chaque ligne (`center` = Mission, `left` =
    Programme/"Dont titre 2"/"Total") - verifie exhaustivement sur les 213
    lignes de la LFI 2026 (32 missions, 131 programmes, 49 lignes "Dont
    titre 2", 1 ligne "Total"). "Dont titre 2" est un memo deja inclus dans
    le total du programme (exclu de toute somme, sous peine de doubler ce
    montant). La ligne finale "Total" sert uniquement de cross-check
    externe (voir tests), jamais integree au parsing.

    Contrairement a TOUTES les generations de source precedentes (y compris
    2016/2017, qui n'ont pas de code mission mais ont un vrai code
    programme et un vrai libelle/code action), Etat B ne fournit NI code
    mission NI code programme NI decomposition par action: seulement des
    libelles Mission/Programme. `mission_code=""` (repli sur le slug du
    libelle, cf. `resolve_mission_identities`, meme mecanisme que 2013/2014/
    2016/2017). `programme_code` est un hash court (16 caracteres hex) du
    couple `(mission_libelle, programme_libelle)`, plutot que le seul
    libelle programme: bien qu'aucune collision de libelle ne soit observee
    sur la LFI 2026 (131 libelles programme, tous uniques), rien ne
    garantit l'unicite d'un libelle de programme A TRAVERS plusieurs
    missions pour une annee future - sans en tenir compte,
    `aggregate_depenses` (cle `(mission_code, programme_code, action_code)`,
    ici `mission_code` vide pour toutes les lignes) fusionnerait
    silencieusement deux programmes homonymes de missions differentes. Un
    hash (plutot qu'une simple concatenation des libelles) est necessaire
    car `programme.code`/`action.code` sont limites a 50 caracteres en base
    - la concatenation de libelles longs (mission + programme, plus de 100
    caracteres pour plusieurs lignes reelles de la LFI 2026) depasserait
    cette limite: bug reel trouve et corrige a l'execution du run complet
    contre la base (`StringDataRightTruncationError`), pas en test unitaire
    seul (les fixtures de test n'exercaient que des libelles courts). Une
    action SYNTHETIQUE unique est creee par programme (`action_code`/`
    action_libelle` reprennent ceux du programme): limitation reelle de
    cette source (aucune granularite plus fine disponible, a la difference
    de toutes les autres annees), documentee plutot que masquee.
    """
    rows = parse_html_table_rows(etat_b_html)
    out: list[DepenseRecord] = []
    mission_libelle = ""
    for row in rows:
        align, label = row[0]
        ae_str, cp_str = row[1][1], row[2][1]
        if align == "center":
            mission_libelle = label
            continue
        if label in ("Dont titre 2", "Total"):
            continue
        cle = f"{mission_libelle}::{label}".encode()
        programme_code = hashlib.sha256(cle).hexdigest()[:16]
        out.append(
            DepenseRecord(
                annee=annee,
                mission_code="",
                mission_libelle=mission_libelle,
                programme_code=programme_code,
                programme_libelle=label,
                action_code=programme_code,
                action_libelle=label,
                ae=clean_montant(ae_str),
                cp=clean_montant(cp_str),
            )
        )
    return out


def aggregate_depenses(records: list[DepenseRecord]) -> list[DepenseAggregat]:
    """Agrege les depenses par triplet (mission, programme, action): somme AE/CP.

    Les sources fournissent une granularite plus fine (sous-action,
    categorie, titre) sans modele dedie cote base: toutes les lignes
    partageant le meme triplet mission/programme/action sont donc sommees.
    """
    par_cle: dict[tuple[str, str, str], DepenseAggregat] = {}
    for rec in records:
        cle = (rec.mission_code, rec.programme_code, rec.action_code)
        existant = par_cle.get(cle)
        if existant is None:
            par_cle[cle] = DepenseAggregat(
                annee=rec.annee,
                mission_code=rec.mission_code,
                mission_libelle=rec.mission_libelle,
                programme_code=rec.programme_code,
                programme_libelle=rec.programme_libelle,
                action_code=rec.action_code,
                action_libelle=rec.action_libelle,
                ae=rec.ae,
                cp=rec.cp,
            )
        else:
            par_cle[cle] = DepenseAggregat(
                annee=existant.annee,
                mission_code=existant.mission_code,
                mission_libelle=existant.mission_libelle,
                programme_code=existant.programme_code,
                programme_libelle=existant.programme_libelle,
                action_code=existant.action_code,
                action_libelle=existant.action_libelle,
                ae=existant.ae + rec.ae,
                cp=existant.cp + rec.cp,
            )
    return list(par_cle.values())


# ---------------------------------------------------------------------------
# Recettes
# ---------------------------------------------------------------------------


# Valeurs reelles observees du champ `type_de_recettes` qui constituent des
# "recettes" au sens du tableau d'equilibre du budget de l'Etat. Les deux
# autres valeurs reelles ("Prelevement(s) sur les recettes de l'Etat au
# profit ...") sont des PSR, traitees a part par
# `extract_prelevements_sur_recettes` - volontairement PAS incluses ici.
_TYPES_RECETTES_BUDGETAIRES = {"Recettes fiscales", "Recettes non fiscales"}


def normalize_recettes_records_json(
    records: list[dict[str, Any]], annee: int
) -> list[RecetteRecord]:
    """Normalise les enregistrements 'recettes du budget general' (2024-2025).

    Le champ `type_de_recettes` prend 4 valeurs reelles dans ce dataset:
    "Recettes fiscales", "Recettes non fiscales", "Prelevements sur les
    recettes de l'Etat au profit des collectivites territoriales" et
    "Prelevement sur les recettes de l'Etat au profit de l'Union
    europeenne". Seules les deux premieres sont de veritables recettes de
    l'Etat: les lignes PSR ("Prelevement(s)...") sont exclues ici (elles ne
    sont pas inserees dans `recette`, dont le `type` IR/TVA/IS/TICPE/AUTRES
    n'a pas de sens pour un prelevement reverse) et traitees separement par
    `extract_prelevements_sur_recettes`, a soustraire du total en aval - cf.
    `api.etl.loader.recalculer_annee_budget`.

    `code_ligne_recettes` est mappe vers un `TypeRecette` stable via
    `CODE_LIGNE_RECETTE_VERS_TYPE` (IR/IS/TVA/TICPE); toute ligne dont le
    code n'est pas dans cette table tombe dans le bucket AUTRES.
    """
    out: list[RecetteRecord] = []
    for row in records:
        type_recettes = row.get("type_de_recettes")
        if type_recettes not in _TYPES_RECETTES_BUDGETAIRES:
            continue
        code = row.get("code_ligne_recettes")
        code_f = float(code) if code is not None else None
        type_str = (
            CODE_LIGNE_RECETTE_VERS_TYPE.get(code_f, "AUTRES") if code_f is not None else "AUTRES"
        )
        out.append(
            RecetteRecord(
                annee=annee,
                type=TypeRecette(type_str),
                montant=clean_montant(row.get("montant_recettes_plf")),
            )
        )
    return out


def extract_prelevements_sur_recettes(
    records: list[dict[str, Any]], annee: int
) -> PrelevementsSurRecettes:
    """Isole et somme les lignes PSR (prelevements sur recettes) d'une annee.

    Opere sur les memes enregistrements bruts que
    `normalize_recettes_records_json` (avant filtrage), pour en extraire ce
    que cette derniere exclut volontairement: les lignes dont
    `type_de_recettes` commence par "Prelevement" (les deux valeurs reelles
    observees, PSR collectivites territoriales et PSR Union europeenne,
    partagent ce prefixe). Le total retourne est celui a soustraire de la
    somme des `recette.montant_net` pour obtenir `recettes_nettes`, selon la
    methodologie du tableau d'equilibre officiel du budget de l'Etat (voir
    `PrelevementsSurRecettes` et `api.etl.loader.recalculer_annee_budget`).
    """
    collectivites = 0.0
    union_europeenne = 0.0
    for row in records:
        type_recettes = row.get("type_de_recettes") or ""
        if not type_recettes.startswith("Prélèvement"):
            continue
        montant = clean_montant(row.get("montant_recettes_plf"))
        type_lower = type_recettes.lower()
        if "collectivit" in type_lower:
            collectivites += montant
        elif "union europ" in type_lower:
            union_europeenne += montant
        else:
            # Categorie PSR non reconnue (nouvelle ligne introduite par la
            # source apres cette passe d'implementation?): comptee quand
            # meme dans le total (bucket collectivites par defaut) pour ne
            # pas fausser recettes_nettes, mais signalee explicitement pour
            # investigation.
            logger.warning(
                "annee %d: categorie PSR non reconnue %r (comptee avec les "
                "collectivites par defaut)",
                annee,
                type_recettes,
            )
            collectivites += montant
    return PrelevementsSurRecettes(
        annee=annee, collectivites=collectivites, union_europeenne=union_europeenne
    )


# ---------------------------------------------------------------------------
# Recettes: source Legifrance/PISTE (LFI 2026+), Etat A - I. Budget general
# ---------------------------------------------------------------------------

# Categories de "2e niveau" d'Etat A (ex "11.", "13 bis.", "31."), qui
# regroupent normalement une ou plusieurs lignes numerotees. Distinctes des
# categories de "1er niveau" ("1."-"4.", meme motif mais prefixe a 1 chiffre
# - cf. `_etat_a_lignes_utiles`).
_CATEGORIE_NIVEAU2_RE = re.compile(r"^(\d{1,2})(?:\s+(?:bis|ter|quater))?\.\s")


def _etat_a_lignes_utiles(rows: list[list[tuple[str, str]]]) -> list[tuple[float, str]]:
    """Retourne (code_ligne, montant_brut_texte) pour chaque ligne de detail
    reelle d'Etat A - I. Budget general (LFI 2026+).

    Deux origines possibles pour une ligne "utile":
    - une ligne numerotee classique (colonne "Numero de ligne" non vide, ex
      "1101", "1501") -> `code_ligne` = ce numero tel quel;
    - une categorie de 2e niveau (ex "18. Autres remboursements et
      degrevements d'impots d'Etat") qui n'a AUCUNE ligne numerotee en
      dessous (observe sur la LFI 2026 pour la ligne "18.": categorie
      normalement detaillee par ligne(s) numerotee(s), comme "11."-"17." ou
      "21."-"26.", mais qui cette annee-la porte son montant directement,
      sans enfant) -> `code_ligne` SYNTHETIQUE = numero de categorie * 100
      (ex 1800), pour rester dans la meme convention de plage que les vrais
      codes de detail. Ceci permet a `normalize_recettes_legifrance_2026`
      et `extract_prelevements_sur_recettes_legifrance` de filtrer/mapper
      les deux origines de facon identique (y compris si une categorie PSR,
      "31."/"32.", devait un jour se retrouver sans ligne numerotee).

    Une categorie de 2e niveau qui a bien une ligne numerotee juste en
    dessous (le cas normal, ex "11. Impot net sur le revenu" suivie de
    "1101") est un PARENT: son propre montant duplique celui de son (ses)
    enfant(s) et est donc exclu (sous peine de compter deux fois).

    Les categories de 1er niveau ("1."-"4.") sont toujours exclues: "1."-
    "3." n'ont jamais de montant propre (uniquement leurs sous-categories
    "11."-"18."/"21."-"26."/"31."-"32."), et "4. Fonds de concours et
    attributions de produits" est volontairement hors du perimetre
    "recettes nettes des prelevements" (n'entre pas dans le total de
    controle "Total des recettes, nettes des prelevements" affiche
    nativement par la source - verifie par recoupement).
    """
    out: list[tuple[float, str]] = []
    for i, row in enumerate(rows):
        numero, label, montant = row[0][1].strip(), row[1][1].strip(), row[2][1].strip()
        if numero:
            out.append((float(numero), montant))
            continue
        categorie_match = _CATEGORIE_NIVEAU2_RE.match(label)
        if categorie_match is None or len(categorie_match.group(1)) != 2:
            continue
        prochaine_ligne_est_detail = i + 1 < len(rows) and rows[i + 1][0][1].strip().isdigit()
        if not prochaine_ligne_est_detail and montant:
            out.append((float(categorie_match.group(1)) * 100, montant))
    return out


def normalize_recettes_legifrance_2026(etat_a_html: str, annee: int = 2026) -> list[RecetteRecord]:
    """Normalise l'Etat A (Voies et moyens, I. - Budget general) de la LFI
    2026 (et, potentiellement, des annees suivantes au meme format) en
    `RecetteRecord`.

    `etat_a_html` est le fragment HTML de la table "I." (deja isole par
    `api.etl.run._extract_etats_html` - Etat A contient PLUSIEURS tables
    concatenees dans la reponse brute: Budget general, puis Budgets
    annexes, Comptes d'affectation speciale, Comptes de concours financiers
    - seule la premiere (Budget general) est retenue ici, cf. `api.etl.
    sources`).

    Exclut les lignes "3. Prelevements sur les recettes de l'Etat" (codes
    31xx/32xx), traitees a part par
    `extract_prelevements_sur_recettes_legifrance` sur les MEMES lignes
    (avant filtrage) - meme principe que `normalize_recettes_records_json`/
    `extract_prelevements_sur_recettes` pour la source 2024-2025.

    Mapping vers `TypeRecette` via `CODE_LIGNE_RECETTE_VERS_TYPE` (par
    `code_ligne`, pas par libelle - les libelles changent, ex TICPE ->
    "Accises sur les energies (ex-TICPE)" en 2026, mais les codes restent
    stables). Tout code absent de cette table (dont les codes synthetiques
    de categorie sans enfant, cf. `_etat_a_lignes_utiles`) tombe dans
    `AUTRES`.
    """
    rows = parse_html_table_rows(etat_a_html)
    out: list[RecetteRecord] = []
    for code, montant in _etat_a_lignes_utiles(rows):
        if 3100 <= code < 3300:
            continue
        type_str = CODE_LIGNE_RECETTE_VERS_TYPE.get(code, "AUTRES")
        out.append(
            RecetteRecord(annee=annee, type=TypeRecette(type_str), montant=clean_montant(montant))
        )
    return out


def extract_prelevements_sur_recettes_legifrance(
    etat_a_html: str, annee: int
) -> PrelevementsSurRecettes:
    """Isole et somme les PSR (prelevements sur recettes) d'Etat A - I. Budget
    general (LFI 2026+).

    Contrairement a la source records JSON 2024-2025
    (`extract_prelevements_sur_recettes`, ou les PSR sont un
    `type_de_recettes` distinct au sein d'un flux de lignes plat), Etat A
    les presente directement dans son arborescence, sous la categorie "3.
    Prelevements sur les recettes de l'Etat" (sous-categories "31."
    collectivites territoriales et "32." Union europeenne, codes de detail
    31xx/32xx) - memes lignes que celles exclues par
    `normalize_recettes_legifrance_2026`, via le meme helper
    `_etat_a_lignes_utiles`.
    """
    rows = parse_html_table_rows(etat_a_html)
    collectivites = 0.0
    union_europeenne = 0.0
    for code, montant in _etat_a_lignes_utiles(rows):
        if 3100 <= code < 3200:
            collectivites += clean_montant(montant)
        elif 3200 <= code < 3300:
            union_europeenne += clean_montant(montant)
    return PrelevementsSurRecettes(
        annee=annee, collectivites=collectivites, union_europeenne=union_europeenne
    )


def _strip_accents(text: str) -> str:
    """Retire les diacritiques d'une chaine (e -> e, a -> a...), sans toucher a la casse."""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(c for c in normalized if not unicodedata.combining(c))


def _normalize_label(cell: str) -> str:
    """Normalise une cellule CSV pour un rapprochement insensible aux accents/casse/espaces.

    A la difference de `_slugify` (utilise pour les slugs de mission), les
    espaces internes sont CONSERVES (pas remplaces par des tirets): les
    predicats de `_LABEL_MATCHERS` ci-dessous testent des sous-chaines
    multi-mots ("valeur ajoutee") qui doivent rester detectables telles
    quelles.
    """
    return _strip_accents(cell).strip().lower()


def _md_ou_m_vers_euros(valeur: float) -> float:
    """Convertit un montant Cour des comptes (Md EUR ou M EUR) en EUROS.

    Les tableaux Cour des comptes exploites ici expriment les montants en
    milliards d'euros (Md EUR, ex: "82.361" pour l'IR) ou en millions
    d'euros (M EUR, ex: "287860.8" pour le total des recettes fiscales
    nettes) SELON LE MILLESIME - et l'intitule de colonne du fichier n'est
    PAS fiable pour distinguer les deux: le fichier 2022 (G16.csv) et le
    fichier 2023 (G 14.csv) affichent tous deux un en-tete "Designation des
    recettes (M.€)" alors que leurs valeurs sont en realite en Md EUR
    (confirme par recoupement avec le total agrege du "tableau d'equilibre"
    de la meme annee, exprime celui-la sans ambiguite en M EUR) - erreur de
    libelle cote source, pas une variation reelle d'unite.
    On se base donc sur l'ORDRE DE GRANDEUR plutot que sur le texte de
    l'en-tete: un poste fiscal unique (IR, IS, TICPE, TVA, Autres, PSR,
    recettes non fiscales) vaut, pour la France, TOUJOURS moins de 1000 en
    Md EUR (dizaines a ~150 Md EUR) et TOUJOURS plus de 1000 en M EUR
    (dizaines a centaines de milliers de M EUR) - heuristique verifiee sur
    les 13 tableaux (7 "par impot" + 6 "equilibre") retenus par cette passe
    d'ingestion en comparant leurs totaux aux montants officiels connus.
    """
    if abs(valeur) < 1000:
        return valeur * 1_000_000_000
    return valeur * 1_000_000


def _lire_lignes_csv(csv_text: str, delimiter: str) -> list[list[str]]:
    """Parse un CSV Cour des comptes en liste de lignes (listes de cellules).

    Retire un eventuel BOM UTF-8 en tete de fichier (`normalize_recettes_
    cour_des_comptes`/`extract_non_fiscal_et_psr_cour_des_comptes` peuvent
    recevoir un texte deja decode sans que l'appelant ait necessairement
    utilise un codec "-sig"). `csv.reader` sur un `io.StringIO` gere
    nativement les champs quotes contenant des retours a la ligne (observes
    dans l'en-tete de certains fichiers, ex: une cellule litterale
    '"LFI\\n2020"').
    """
    return list(csv.reader(io.StringIO(csv_text.lstrip("﻿")), delimiter=delimiter))


def _colonne_lfi(lignes: list[list[str]], annee: int) -> int:
    """Localise l'index de la colonne "LFI" (Loi de Finances Initiale votee).

    Les tableaux Cour des comptes exploites ici n'ont pas une ligne d'en-tete
    a position fixe (certains fichiers portent un titre fusionne sur la
    premiere ligne, une colonne d'index pandas parasite en tete de chaque
    ligne pour d'autres - artefact d'un export `DataFrame.to_csv()` sans
    `index=False` cote Cour des comptes): on cherche donc, sur TOUTES les
    lignes et TOUTES les cellules, la premiere qui commence par "lfi" une
    fois normalisee (ex: "LFI 2016", "LFI 2018", "LFI.2022", ou simplement
    "LFI"). Ce prefixe distingue bien la colonne voulue de "LFR"/"LFG"/"PLF"
    (Loi de Finances Rectificative / de fin de Gestion / Projet de LF),
    presentes cote a cote dans plusieurs de ces tableaux.
    """
    for ligne in lignes:
        for idx, cellule in enumerate(ligne):
            if _normalize_label(cellule).startswith("lfi"):
                return idx
    raise ValueError(f"annee {annee}: colonne LFI introuvable dans le tableau Cour des comptes")


def _label_ligne(ligne: list[str]) -> str | None:
    """Retourne la premiere cellule "textuelle" (non vide, non numerique) d'une ligne.

    Ignore les cellules vides et celles parsables comme un nombre (la
    colonne d'index pandas parasite en tete de certaines lignes, cf.
    `_colonne_lfi`, ou une annee isolee en cellule). Retourne None si la
    ligne ne contient aucune cellule "label" exploitable (ligne vide,
    separateur, ou ligne uniquement numerique).
    """
    for cellule in ligne:
        normalisee = _normalize_label(cellule)
        if not normalisee:
            continue
        try:
            clean_montant(normalisee)
        except ValueError:
            return normalisee
    return None


# Predicats de rapprochement (libelle normalise -> TypeRecette) pour le
# tableau "recettes fiscales nettes par impot". L'intitule exact de chaque
# ligne varie d'un millesime a l'autre ("Impot sur le revenu" vs "Impot NET
# sur le revenu", "TICPE" vs "Taxe interieure de consommation sur les
# produits energetiques" en toutes lettres en 2017...): les predicats
# cherchent un fragment stable plutot qu'une correspondance exacte. Verifie
# sur les 7 fichiers "par impot" retenus (2016-2020, 2022, 2023): aucune
# ligne "hors perimetre" (total, sous-detail "...dont ...") ne matche par
# accident (elles ne contiennent aucun de ces fragments).
_RECETTES_PAR_IMPOT_MATCHERS: tuple[tuple[TypeRecette, Callable[[str], bool]], ...] = (
    (TypeRecette.IR, lambda n: "revenu" in n),
    (TypeRecette.IS, lambda n: "societ" in n),
    (TypeRecette.TICPE, lambda n: "ticpe" in n or "energetique" in n),
    (TypeRecette.TVA, lambda n: "tva" in n or "valeur ajoutee" in n),
    (TypeRecette.AUTRES, lambda n: "autres" in n and "fiscale" in n),
)


def normalize_recettes_cour_des_comptes(
    csv_text: str, annee: int, delimiter: str = ";"
) -> list[RecetteRecord]:
    """Normalise le tableau "recettes fiscales nettes par impot" (Cour des comptes).

    Source: rapports annuels "Le budget de l'Etat en <annee>" de la Cour des
    comptes, 2016-2020/2022/2023 (voir `api.etl.sources.
    RECETTES_COUR_DES_COMPTES_*` pour le detail par millesime, les fichiers
    ZIP/CSV exacts localises par recherche de CONTENU - PAS par motif de nom,
    les noms etant incoherents d'une annee sur l'autre - et les exclusions
    2015/2021).

    Colonne retenue: LFI (Loi de Finances Initiale VOTEE), pour rester
    coherent avec les depenses deja en base (elles aussi toutes en LFI) -
    PAS la colonne Execution/PLF/LFR/LFG presente a cote dans ces memes
    tableaux, meme quand elle serait disponible.

    Precision moindre que les donnees 2024-2025 (OpenDataSoft, a l'euro
    pres): ces tableaux Cour des comptes arrondissent a 1 decimale en
    milliards d'euros dans les millesimes 2016-2019 (~50-100 M EUR de bruit
    par ligne), et jusqu'a plusieurs decimales dans les fichiers CSV sources
    2020/2022/2023 (le PDF publie, lui, reste arrondi a 1 decimale - le CSV
    "brut" est simplement plus precis que sa restitution papier). Convertit
    Md EUR/M EUR en EUROS - voir `_md_ou_m_vers_euros` pour l'heuristique de
    detection de l'unite (PAS fiable via le texte de l'en-tete, verifie a
    l'execution reelle sur ces sources).

    ATTENTION consommateurs de cette fonction: le resultat est en recettes
    fiscales NETTES (nettes des remboursements et degrevements, R&D). Le
    pipeline "depenses" existant (`api.etl.loader.upsert_depenses`) charge
    lui les depenses sur une base BRUTE (il somme TOUTES les missions, y
    compris "Remboursements et degrevements" elle-meme, sans la
    retrancher). Combiner tel quel ces recettes nettes avec ces depenses
    brutes SURESTIME le deficit calcule d'environ le montant de cette
    mission (~130-150 Md EUR/an) - voir `api.etl.loader.
    get_remboursements_degrevements_cp` pour le rattrapage applique par
    `api.etl.run._charger_recettes_cour_des_comptes`.

    Ne contient PAS les PSR (prelevements sur recettes): ce tableau est
    conceptuellement une pure decomposition de la fiscalite par impot, la
    notion de PSR relevant du "budget general" (tableau d'equilibre) plutot
    que de la recette fiscale en tant que telle - verifie par recoupement
    (aucune ligne PSR dans les 7 fichiers retenus). Les PSR et les recettes
    non fiscales, quand disponibles pour le millesime, sont extraits a part
    par `extract_non_fiscal_et_psr_cour_des_comptes` depuis le "tableau
    d'equilibre" - un fichier DIFFERENT du meme rapport.
    """
    lignes = _lire_lignes_csv(csv_text, delimiter)
    colonne_lfi = _colonne_lfi(lignes, annee)

    montants: dict[TypeRecette, float] = defaultdict(float)
    for ligne in lignes:
        if colonne_lfi >= len(ligne):
            continue
        label = _label_ligne(ligne)
        if label is None:
            continue
        for type_recette, predicat in _RECETTES_PAR_IMPOT_MATCHERS:
            if predicat(label):
                try:
                    valeur = clean_montant(ligne[colonne_lfi])
                except ValueError:
                    logger.warning(
                        "annee %d: valeur LFI illisible pour la ligne %r (%r), ignoree",
                        annee,
                        label,
                        ligne[colonne_lfi],
                    )
                    break
                montants[type_recette] += _md_ou_m_vers_euros(valeur)
                break

    if not montants:
        raise ValueError(
            f"annee {annee}: aucune ligne recette fiscale reconnue dans le tableau Cour des "
            "comptes (structure de la source a peut-etre change)"
        )

    return [
        RecetteRecord(annee=annee, type=type_recette, montant=montant)
        for type_recette, montant in montants.items()
    ]


# Fragments identifiant les lignes PSR dans le "tableau d'equilibre": ces
# libelles abregent parfois "Union europeenne"/"collectivites territoriales"
# en simple suffixe "UE"/"CT" (ex: "Prelevements sur recettes UE") plutot
# que d'ecrire le nom en toutes lettres comme dans d'autres millesimes (ex:
# "PSR au profit de l'Union europeenne") - d'ou la recherche du mot "ue"/
# "ct" en plus du nom complet.
_RE_PSR_UE = re.compile(r"union europ|(?:^|[^a-z])ue(?:$|[^a-z])")
_RE_PSR_CT = re.compile(r"collectivit|(?:^|[^a-z])ct(?:$|[^a-z])")


def extract_non_fiscal_et_psr_cour_des_comptes(
    csv_text: str, annee: int, delimiter: str = ";"
) -> tuple[float, PrelevementsSurRecettes]:
    """Extrait les recettes non fiscales et les PSR du "tableau d'equilibre" (Cour des comptes).

    Source: le MEME rapport annuel que `normalize_recettes_cour_des_comptes`
    mais un fichier CSV DIFFERENT au sein du ZIP (voir `api.etl.sources.
    RECETTES_COUR_DES_COMPTES_FICHIER_EQUILIBRE` - absent pour 2023, cf. son
    commentaire). Ce tableau presente le passage des recettes fiscales
    nettes aux "recettes nettes du budget general": recettes fiscales
    nettes (a) + recettes non fiscales (b) - PSR UE (c) - PSR collectivites
    (d) [+ fonds de concours (e), volontairement IGNORE ici pour rester
    coherent avec la methodologie 2024-2025 deja en place, qui ne modelise
    pas non plus cette ligne - cf. `api.etl.loader.recalculer_annee_budget`].

    Retourne (recettes_non_fiscales_euros, PrelevementsSurRecettes): le
    premier est a ajouter au bucket TypeRecette.AUTRES (comme pour
    2024-2025, ou les recettes non fiscales n'ont pas de type dedie - cf.
    `normalize_recettes_records_json`), le second a fournir tel quel a
    `api.etl.loader.recalculer_annee_budget` (meme methodologie et meme
    dataclass que pour 2024-2025: les PSR sont retranches des recettes
    fiscales+non fiscales, pas stockes dans `recette`).

    Meme conversion Md EUR/M EUR que `normalize_recettes_cour_des_comptes`
    (voir `_md_ou_m_vers_euros`) et memes limites de precision.

    Leve `ValueError` si l'une des trois lignes attendues (recettes non
    fiscales, PSR UE, PSR collectivites) est introuvable, plutot que de
    retourner silencieusement une valeur par defaut de 0 qui fausserait
    `recettes_nettes` sans avertissement.
    """
    lignes = _lire_lignes_csv(csv_text, delimiter)
    colonne_lfi = _colonne_lfi(lignes, annee)

    non_fiscal: float | None = None
    psr_ue: float | None = None
    psr_ct: float | None = None

    for ligne in lignes:
        if colonne_lfi >= len(ligne):
            continue
        label = _label_ligne(ligne)
        if label is None:
            continue
        try:
            valeur = _md_ou_m_vers_euros(clean_montant(ligne[colonne_lfi]))
        except ValueError:
            continue

        if "recettes non fiscales" in label:
            non_fiscal = valeur
        elif _RE_PSR_UE.search(label) and ("prelevement" in label or "psr" in label):
            psr_ue = valeur
        elif _RE_PSR_CT.search(label) and ("prelevement" in label or "psr" in label):
            psr_ct = valeur

    if non_fiscal is None or psr_ue is None or psr_ct is None:
        raise ValueError(
            f"annee {annee}: tableau d'equilibre Cour des comptes incomplet "
            f"(non_fiscal={non_fiscal}, psr_ue={psr_ue}, psr_ct={psr_ct}) - structure de la "
            "source a peut-etre change"
        )

    # Les PSR sont des montants RETRANCHES du total (cellules source
    # negatives, ex: "-21480.0"): `PrelevementsSurRecettes` attend des
    # montants POSITIFS (le signe "-" est applique par l'appelant, cf.
    # `api.etl.loader.recalculer_annee_budget`: `recettes_total =
    # recettes_brutes - prelevements_sur_recettes`).
    psr = PrelevementsSurRecettes(
        annee=annee, collectivites=abs(psr_ct), union_europeenne=abs(psr_ue)
    )
    return non_fiscal, psr


def aggregate_recettes(records: list[RecetteRecord]) -> list[RecetteAggregat]:
    """Agrege les recettes par (annee, type): somme des montants.

    La source PLF ne fournit qu'un montant unique par ligne, sans
    decomposition brut/net: `montant_brut == montant_net` par construction
    (documente comme decision d'implementation, faute de donnee source plus
    fine sur ce point).
    """
    totaux: dict[tuple[int, TypeRecette], float] = defaultdict(float)
    for rec in records:
        totaux[(rec.annee, rec.type)] += rec.montant
    return [
        RecetteAggregat(annee=annee, type=type_, montant_brut=total, montant_net=total)
        for (annee, type_), total in totaux.items()
    ]


# ---------------------------------------------------------------------------
# Indicateurs macro: PIB nominal et population
# ---------------------------------------------------------------------------


def normalize_pib_csv(content: bytes | str) -> dict[int, float]:
    """Normalise le CSV PIB nominal principal (colonnes "annee,pib").

    La source (voir `api.etl.sources.PIB_CSV_URL`) exprime `pib` en MILLIONS
    d'euros courants ("PIB en valeur", prix courants - PAS un volume/prix
    chaines): convertit en EUROS (`* 1_000_000`), coherent avec
    `depense.ae`/`depense.cp` deja en euros. Ne couvre que 1949-2022 (serie
    source arretee, cf. docstring de `PIB_CSV_URL`).
    """
    text = content.decode("utf-8") if isinstance(content, bytes) else content
    out: dict[int, float] = {}
    for row in csv.DictReader(io.StringIO(text)):
        annee = int(row["annee"])
        pib_millions = clean_montant(row["pib"])
        out[annee] = pib_millions * 1_000_000
    return out


def normalize_pib_complement_insee_premiere(content: bytes, annee: int) -> float:
    """Extrait le PIB nominal (euros courants) d'une edition "Insee Premiere -
    Les comptes de la Nation en <annee>" (voir
    `api.etl.sources.PIB_COMPLEMENT_XLSX_URLS`), qui complete le trou
    2023-2025 de `normalize_pib_csv`.

    La feuille "Figure 1 - Le PIB et les operations sur les biens et les
    services" contient une ligne "Produit interieur brut (PIB)" dont la
    colonne "En milliards d'euros" donne le NIVEAU nominal (prix courants,
    base 2020) du PIB pour la derniere annee couverte par l'edition - a ne
    pas confondre avec les colonnes "Evolution en volume" (taux de
    croissance en volume, prix chaines) qui la precedent dans le tableau.
    Structure observee, identique sur les 3 editions 2023/2024/2025
    (IP1997/IP2053/IP2105): colonne A=libelle, B/C/D=evolutions en volume
    (3 dernieres annees), E="En milliards d'euros" (niveau, derniere
    annee), F/G/H=evolution prix/valeur/contribution.

    Leve `ValueError` si la structure attendue n'est pas retrouvee (ligne
    PIB introuvable) ou si l'annee d'en-tete du tableau ne correspond pas a
    `annee` (garde-fou contre une source qui aurait change de format sans
    prevenir), plutot que de charger silencieusement une valeur erronee.
    """
    df = pd.read_excel(io.BytesIO(content), sheet_name="Figure 1", header=None)

    annee_entete: int | None = None
    pib_milliards: float | None = None
    for _, row in df.iterrows():
        valeurs = row.tolist()
        col0 = valeurs[0]
        col3 = valeurs[3] if len(valeurs) > 3 else None
        if (
            (col0 is None or (isinstance(col0, float) and pd.isna(col0)))
            and isinstance(col3, int | float)
            and not pd.isna(col3)
            and col3 > 2000
        ):
            # Ligne d'en-tete des annees, ex: (None, 2021, 2022, 2023, "En
            # milliards d'euros", ...) - la derniere annee (col3) est celle
            # couverte par la colonne "niveau" (col4) plus loin sur la ligne.
            annee_entete = int(col3)
        if isinstance(col0, str) and col0.strip().startswith("Produit intérieur brut"):
            pib_milliards = float(valeurs[4])

    if annee_entete != annee:
        raise ValueError(
            f"annee {annee}: en-tete Figure 1 trouve {annee_entete!r}, attendu {annee} "
            "(structure de la source Insee Premiere a peut-etre change)"
        )
    if pib_milliards is None:
        raise ValueError(
            f"annee {annee}: ligne 'Produit interieur brut (PIB)' introuvable dans Figure 1 "
            "(structure de la source Insee Premiere a peut-etre change)"
        )
    return pib_milliards * 1_000_000_000


def normalize_population_xlsx(content: bytes) -> dict[int, int]:
    """Normalise le fichier population INSEE (onglet "FR", France entiere).

    Voir `api.etl.sources.POPULATION_XLSX_URL`. La feuille "FR" a 3 lignes
    d'en-tete (titre, sous-titre, puis la ligne de noms de colonnes propre)
    avant les donnees - d'ou `header=2`. La colonne "Annee" contient soit un
    entier (annees anciennes), soit une chaine suffixee " (p)" pour les
    dernieres annees provisoires (ex: "2025 (p)"): le prefixe a 4 chiffres
    est extrait dans les deux cas. La colonne "Population au 1er janvier"
    vaut "nd " (non disponible - chaine, pas un nombre) pour les annees les
    plus anciennes (avant 1982 dans l'edition courante): ces lignes sont
    ignorees, tout comme les lignes de notes de bas de page en fin de
    feuille (annee non parsable en 4 chiffres).
    """
    df = pd.read_excel(io.BytesIO(content), sheet_name="FR", header=2)
    out: dict[int, int] = {}
    for _, row in df.iterrows():
        annee_brute = row.iloc[0]
        population_brute = row.iloc[1]
        if pd.isna(annee_brute):
            continue
        match = re.match(r"^(\d{4})", str(annee_brute).strip())
        if not match:
            continue
        annee = int(match.group(1))
        if isinstance(population_brute, int | float) and not pd.isna(population_brute):
            out[annee] = int(population_brute)
    return out
