"""Normalisation des donnees brutes issues des sources ETL vers une structure
intermediaire commune, independante du format source.

Le portail data.economie.gouv.fr expose les "depenses" sous 4 generations de
format selon l'annee (voir `api.etl.sources` pour le detail); ce module
fournit un normaliseur par sous-format, tous convergeant vers `DepenseRecord`
puis `DepenseAggregat` (une ligne par triplet mission/programme/action, AE/CP
sommes). Les "recettes" convergent vers `RecetteRecord` / `RecetteAggregat`.

Resolution d'identite des missions
-----------------------------------
Le libelle d'une mission peut changer d'une annee sur l'autre (ex: fusion,
renommage de ministere) alors que la mission logique reste la meme. La cle de
rapprochement prioritaire est `code_mission` (code LOLF a 2 lettres, stable
dans le temps sur tout notre perimetre 2019-2025). A defaut de code (formats
source plus anciens, hors perimetre de cette passe), on retombe sur le slug
du libelle courant comme cle de secours - chaque variante sans code forme
alors sa propre mission logique.

Le "libelle canonique" d'une mission logique (utilise pour `nom_normalise`
et pour calculer son `slug`) est celui de l'annee la plus recente ou elle a
ete observee: le slug reste ainsi stable dans le temps, y compris pour les
annees plus anciennes ou l'intitule affiche differait. Chaque variante de
libelle brut distincte est tracee dans `MissionAlias` avec la plage
d'annees (annee_debut/annee_fin) ou elle a ete utilisee telle quelle.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

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
