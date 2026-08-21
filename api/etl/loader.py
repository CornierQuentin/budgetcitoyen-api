"""Chargement (upsert) des donnees normalisees en base.

`upsert_missions` et `upsert_recettes` s'appuient sur les contraintes
d'unicite existantes (`mission(slug, annee)`, `recette(annee, type)`) via
`INSERT ... ON CONFLICT DO UPDATE`, ce qui les rend idempotents nativement.

`programme`, `action` et `depense` n'ont pas de contrainte d'unicite dans le
schema actuel (granularite fine geree par la cle primaire uniquement): pour
rester idempotent sur des relances repetees, `upsert_depenses` recharge une
annee dans son integralite (delete puis insert des lignes `annee = :annee`)
plutot que de tenter un upsert ligne a ligne sans cle stable.

`upsert_marches` pousse cette meme logique plus loin: `marche_public` n'a NI
cle naturelle (l'`id` source n'est pas fiable, cf. son modele) NI notion
d'annee - la table entiere est rechargee a chaque run, par lots (streaming
depuis le normaliseur) plutot qu'en une seule liste Python de ~689 000
elements en memoire.
"""

import logging
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime

from sqlalchemy import delete, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from api.etl.normalize import (
    DepenseAggregat,
    DepenseFiscaleRecord,
    MarcheRecord,
    MissionAliasRow,
    MissionYearRow,
    RecetteAggregat,
)
from api.models.action import Action
from api.models.annee_budget import AnneeBudget
from api.models.depense import Depense
from api.models.depense_fiscale import DepenseFiscale
from api.models.indicateur_macro import IndicateurMacro
from api.models.ingestion_log import IngestionLog
from api.models.marche_public import MarchePublic
from api.models.mission import Mission
from api.models.mission_alias import MissionAlias
from api.models.programme import Programme
from api.models.recette import Recette

logger = logging.getLogger(__name__)

# Largeur des colonnes String(255) portant des libelles (mission/programme/
# action/alias). Une poignee de libelles legaux reels depassent 255
# caracteres (ex: intitules de programmes de compensation tres detailles):
# on tronque plutot que d'elargir le schema, hors perimetre de cette passe.
_LIBELLE_MAX_LEN = 255


def _tronque(texte: str, max_len: int = _LIBELLE_MAX_LEN) -> str:
    """Tronque un libelle a la largeur de la colonne, en le signalant si besoin."""
    if len(texte) <= max_len:
        return texte
    logger.warning("libelle tronque a %d caracteres: %r", max_len, texte)
    return texte[:max_len]


async def upsert_missions(
    db: AsyncSession, rows: Sequence[MissionYearRow]
) -> dict[tuple[str, int], int]:
    """Insere ou met a jour les missions, upsert idempotent sur (slug, annee).

    Retourne le mapping {(slug, annee): mission_id} pour les lignes fournies.
    """
    if not rows:
        return {}

    values = [
        {
            # slug/nom_normalise/nom_officiel ne sont volontairement pas
            # tronques ici: leur valeur exacte (non tronquee) est la cle de
            # correlation utilisee par l'appelant pour resoudre mission_id
            # (cf. `run.py`); une troncature cote loader casserait ce
            # rapprochement. En pratique les libelles de mission observes
            # restent bien en-deca de 255 caracteres (contrairement a
            # certains libelles de programme/action, tronques ci-dessous).
            "slug": r.slug,
            "nom_normalise": r.nom_normalise,
            "nom_officiel": r.nom_officiel,
            "annee": r.annee,
            "code_mission": r.code_mission,
        }
        for r in rows
    ]
    insert_stmt = pg_insert(Mission).values(values)
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[Mission.slug, Mission.annee],
        set_={
            "nom_normalise": insert_stmt.excluded.nom_normalise,
            "nom_officiel": insert_stmt.excluded.nom_officiel,
            "code_mission": insert_stmt.excluded.code_mission,
        },
    ).returning(Mission.id, Mission.slug, Mission.annee)
    result = await db.execute(upsert_stmt)
    mapping = {(row.slug, row.annee): row.id for row in result}
    logger.info("missions upsertees: %d", len(mapping))
    return mapping


async def upsert_mission_aliases(
    db: AsyncSession, rows: Sequence[tuple[MissionAliasRow, int]], annees: Sequence[int]
) -> None:
    """Reconstruit la portion de mission_alias couverte par `annees` a partir des alias fournis.

    `rows` contient des couples (alias, mission_id) deja resolus, calcules
    par `api.etl.normalize.build_mission_alias_rows` a partir des SEULES
    annees traitees par le run courant (`annees`). Un alias genere par cette
    passe cible toujours un `Mission` dont `annee` est dans `annees` (son
    `mission_id` provient de `annee_cible = max(annees observees)`, qui est
    necessairement une des annees fournies a `build_mission_alias_rows` -
    voir sa docstring): la suppression prealable est donc scopee aux
    `mission_alias` dont le `mission_id` pointe vers l'une de ces annees,
    PAS un `delete(MissionAlias)` sans condition.

    Sans ce filtre, un run partiel (ex: `--depenses-only --annees 2018`)
    effacerait la table entiere avant de ne reinserer QUE les alias de 2018,
    detruisant silencieusement les alias deja corrects des autres annees
    (bug constate a l'execution reelle lors de l'ajout de l'annee 2018 - cf.
    JOURNAL/PR correspondante). Reste idempotent sur re-execution des memes
    `annees` (purement derive des donnees de depenses de ces annees-la).
    """
    if annees:
        cible = select(Mission.id).where(Mission.annee.in_(annees))
        await db.execute(delete(MissionAlias).where(MissionAlias.mission_id.in_(cible)))
    if not rows:
        return
    values = [
        {
            "nom_csv": _tronque(alias.nom_csv),
            "mission_id": mission_id,
            "annee_debut": alias.annee_debut,
            "annee_fin": alias.annee_fin,
        }
        for alias, mission_id in rows
    ]
    await db.execute(insert(MissionAlias), values)
    logger.info("mission_alias reconstruits: %d", len(values))


async def upsert_depenses(
    db: AsyncSession,
    annee: int,
    aggregats: Sequence[tuple[DepenseAggregat, int]],
) -> int:
    """Recharge les programmes/actions/depenses d'une annee a partir des agregats.

    `aggregats` contient des couples (DepenseAggregat, mission_id) deja
    resolus. Supprime d'abord toutes les lignes `annee = :annee` existantes
    (depense puis action puis programme, dans cet ordre a cause des FK), puis
    reinsere les programmes, actions et depenses correspondants. Retourne le
    nombre de lignes `depense` inserees.
    """
    await db.execute(delete(Depense).where(Depense.annee == annee))
    await db.execute(delete(Action).where(Action.annee == annee))
    await db.execute(delete(Programme).where(Programme.annee == annee))

    if not aggregats:
        return 0

    programme_libelles: dict[tuple[int, str], str] = {}
    for agg, mission_id in aggregats:
        programme_libelles[(mission_id, agg.programme_code)] = agg.programme_libelle

    programme_values = [
        {"mission_id": mission_id, "code": code, "nom": _tronque(nom), "annee": annee}
        for (mission_id, code), nom in programme_libelles.items()
    ]
    result = await db.execute(
        insert(Programme).returning(Programme.id, Programme.mission_id, Programme.code),
        programme_values,
    )
    programme_ids = {(row.mission_id, row.code): row.id for row in result}

    action_libelles: dict[tuple[int, str], str] = {}
    for agg, mission_id in aggregats:
        programme_id = programme_ids[(mission_id, agg.programme_code)]
        action_libelles[(programme_id, agg.action_code)] = agg.action_libelle

    action_values = [
        {"programme_id": programme_id, "code": code, "nom": _tronque(nom), "annee": annee}
        for (programme_id, code), nom in action_libelles.items()
    ]
    result = await db.execute(
        insert(Action).returning(Action.id, Action.programme_id, Action.code),
        action_values,
    )
    action_ids = {(row.programme_id, row.code): row.id for row in result}

    depense_values = []
    for agg, mission_id in aggregats:
        programme_id = programme_ids[(mission_id, agg.programme_code)]
        action_id = action_ids[(programme_id, agg.action_code)]
        depense_values.append({"action_id": action_id, "ae": agg.ae, "cp": agg.cp, "annee": annee})

    await db.execute(insert(Depense), depense_values)
    logger.info(
        "annee %d: %d programmes, %d actions, %d depenses charges",
        annee,
        len(programme_values),
        len(action_values),
        len(depense_values),
    )
    return len(depense_values)


async def upsert_recettes(db: AsyncSession, aggregats: Sequence[RecetteAggregat]) -> None:
    """Insere ou met a jour les recettes, upsert idempotent sur (annee, type)."""
    if not aggregats:
        return
    values = [
        {
            "annee": a.annee,
            "type": a.type,
            "montant_brut": a.montant_brut,
            "montant_net": a.montant_net,
        }
        for a in aggregats
    ]
    stmt = pg_insert(Recette).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Recette.annee, Recette.type],
        set_={
            "montant_brut": stmt.excluded.montant_brut,
            "montant_net": stmt.excluded.montant_net,
        },
    )
    await db.execute(stmt)
    logger.info("recettes upsertees: %d", len(values))


async def get_remboursements_degrevements_cp(db: AsyncSession, annee: int) -> float:
    """Retourne le total des CP de la mission "Remboursements et degrevements" (code RD).

    Necessaire pour "regrossir" les recettes fiscales NETTES sourcees a la
    Cour des comptes (2016-2023, cf. `api.etl.normalize.
    normalize_recettes_cour_des_comptes`) et les rendre comparables, cote
    calcul du deficit, aux depenses deja chargees par le pipeline
    "depenses" existant (`upsert_depenses`).

    Note (source Legifrance/PISTE, 2015/2021/2026): ce rattrapage "mission
    entiere" a ete essaye puis ABANDONNE pour les annees Legifrance apres
    verification contre leur propre article d'equilibre officiel - voir
    `get_remboursements_degrevements_impots_etat_cp` (utilise pour 2026
    uniquement) et la docstring de `api.etl.run._charger_recettes_
    legifrance` pour le detail complet (2015/2021 n'ont besoin d'AUCUN
    rattrapage, ni celui-ci ni l'autre).

    Constat (verifie a l'execution reelle lors de l'ingestion des recettes
    2016-2023): `upsert_depenses` somme TOUTES les missions du budget
    general, y compris "Remboursements et degrevements" (code_mission
    "RD") elle-meme - c'est donc une base BRUTE de depenses (~130-150 Md
    EUR/an de plus que la base "nette" utilisee par le tableau d'equilibre
    officiel, qui retranche justement ce montant des DEUX cotes: recettes
    fiscales brutes -> nettes ET depenses brutes -> nettes, cf.
    `api.etl.normalize._md_ou_m_vers_euros` et le "tableau d'equilibre"
    Cour des comptes). Combiner des depenses BRUTES avec des recettes
    fiscales NETTES (comme le fait `normalize_recettes_cour_des_comptes`
    par construction: ses tableaux source disent explicitement "recettes
    fiscales NETTES") SURESTIME donc le deficit calcule d'environ ce
    montant - constate a l'execution reelle (ex: sans ce rattrapage, le
    deficit LFI 2022 calcule ressort a ~284 Md EUR au lieu des ~154 Md EUR
    officiels, verifies par ailleurs aupres du tableau d'equilibre Cour des
    comptes 2022 lui-meme).

    Le CP de la mission "RD" correspond, au M EUR pres, a la somme "R & D
    sur impots d'Etat" + "R & D sur impots locaux" du tableau d'equilibre
    Cour des comptes de la meme annee (ex 2020: 140830325376 EUR ici vs
    140830325378 EUR cote Cour des comptes, LFI 2020) - preferee ici a une
    nouvelle extraction Cour des comptes (qui ne serait de toute facon PAS
    disponible pour 2023, cf. `api.etl.sources.
    RECETTES_COUR_DES_COMPTES_FICHIER_EQUILIBRE`) car deja chargee et
    fiable.

    Retourne 0.0 si aucune depense n'est chargee pour cette mission/annee
    (ex: 2016-2017, hors perimetre du pipeline "depenses" - `annee_budget`
    ne sera de toute facon pas calcule pour ces annees, cf.
    `recalculer_annee_budget`, qui exige depenses ET recettes).
    """
    total = await db.scalar(
        select(func.coalesce(func.sum(Depense.cp), 0))
        .select_from(Depense)
        .join(Action, Depense.action_id == Action.id)
        .join(Programme, Action.programme_id == Programme.id)
        .join(Mission, Programme.mission_id == Mission.id)
        .where(Mission.code_mission == "RD", Mission.annee == annee)
    )
    return float(total or 0.0)


async def get_remboursements_degrevements_impots_etat_cp(db: AsyncSession, annee: int) -> float:
    """Retourne le CP du SEUL programme "Remboursements et degrevements d'impots
    d'Etat" (PAS "...d'impots locaux", cf. ci-dessous) de la mission
    "Remboursements et degrevements". Usage: SEULEMENT les annees listees
    dans `api.etl.sources.LFI_REMBOURSEMENTS_IMPOTS_ETAT_SEUL` (2026
    actuellement, source Legifrance/PISTE).

    Bug reel trouve et corrige a l'execution du run complet sur la LFI
    2026: en reutilisant `get_remboursements_degrevements_cp` (mission
    entiere) par erreur, le deficit calcule ressortait a ~129,1 Md EUR
    (sous-estime d'environ le CP du programme "impots locaux", ~4,4 Md
    EUR) au lieu des ~133,5 Md EUR officiels (tableau d'equilibre, article
    147 de la loi).

    ATTENTION - ceci n'est PAS une regle generale de methodologie
    budgetaire ni meme une regle Legifrance-specifique (ne pas supposer
    qu'elle s'applique a une nouvelle annee sans verifier): la LFI 2026 est
    la SEULE des 3 annees Legifrance verifiees a necessiter un rattrapage
    a ce niveau de granularite. Les LFI 2015 (article 49) et 2021 (article
    93) deduisent bien la mission ENTIERE dans leur PROPRE tableau
    d'equilibre officiel (article 49: "99 475" M EUR ; article 93: "129
    334" M EUR, chacune EXACTEMENT la somme des 2 programmes) - mais cela
    ne signifie PAS qu'il faille appliquer `get_remboursements_
    degrevements_cp` (mission entiere) a ces 2 annees non plus: leurs
    montants d'Etat A sont DEJA sur une base comparable aux depenses BRUTES
    sans AUCUN rattrapage (verifie: la somme brute des lignes d'Etat A hors
    PSR correspond EXACTEMENT a la ligne "recettes brutes" de leur propre
    tableau d'equilibre - le rattrapage de la mission "Remboursements et
    degrevements" s'annule mathematiquement des 2 cotes de l'equation sans
    intervention). Erreurs reelles trouvees en verifiant chaque annee
    individuellement plutot que d'assumer qu'une regle se generalise: un
    essai avec cette fonction (impots d'Etat seul) sur 2021 donnait un
    deficit de 43,0 Md EUR au lieu du solde officiel -172,4 Md EUR; un
    essai avec `get_remboursements_degrevements_cp` (mission entiere) sur
    2015 donnait -13,6 Md EUR (surplus implausible) au lieu de -74,2 Md EUR
    officiels - dans les 2 cas, seule l'absence de RATTRAPAGE DU TOUT etait
    correcte pour ces 2 annees. Voir la docstring de `api.etl.run.
    _charger_recettes_legifrance` pour le detail complet et le
    raisonnement algebrique qui explique cette annulation.

    Sur cette source (Legifrance/PISTE), aucun code n'est disponible ni pour
    la mission (`code_mission=""`, cf. `api.etl.normalize.
    normalize_depenses_legifrance`) ni pour les programmes (`programme.code`
    est un hash, non lisible) - recherche donc par SLUG de mission et par
    LIBELLE de programme (motif "impots d'Etat", ne doit PAS matcher
    "impots locaux"). Retourne 0.0 si aucune ligne ne correspond (ex:
    libelle du programme different d'une annee a l'autre - a revalider si
    ce cas se presente, meme logique de repli silencieux que
    `get_remboursements_degrevements_cp`).
    """
    total = await db.scalar(
        select(func.coalesce(func.sum(Depense.cp), 0))
        .select_from(Depense)
        .join(Action, Depense.action_id == Action.id)
        .join(Programme, Action.programme_id == Programme.id)
        .join(Mission, Programme.mission_id == Mission.id)
        .where(
            Mission.slug == "remboursements-et-degrevements",
            Mission.annee == annee,
            Programme.nom.ilike("%imp%ts d'Etat%"),
        )
    )
    return float(total or 0.0)


async def recalculer_annee_budget(
    db: AsyncSession,
    annee: int,
    source_url: str,
    prelevements_sur_recettes: float = 0.0,
    remboursements_impots_etat: float = 0.0,
) -> AnneeBudget | None:
    """Recalcule l'agregat AnneeBudget d'une annee, si depenses ET recettes existent.

    - `depenses_nettes` = somme des `Depense.cp` (credits de paiement) de l'annee.
    - `recettes_nettes` = somme de TOUS les `Recette.montant_net` de l'annee
      (types IR/TVA/IS/TICPE/AUTRES, deja limites aux "Recettes fiscales" et
      "Recettes non fiscales" par `normalize.normalize_recettes_records_json`
      - voir sa docstring), MOINS `prelevements_sur_recettes`.
    - `remboursements_impots_etat`: rattrapage brut/net, AJOUTE aux recettes
      (symetrique du PSR, qui en est retranche). Certaines annees expriment
      Etat A en montants NETS de remboursements ("Impot NET sur le revenu"),
      alors que les depenses comparees sont brutes: sans ce rattrapage, le
      deficit calcule ne retombe pas sur le solde de l'article d'equilibre
      officiel. Fourni par l'appelant (cf. `api.etl.run.
      _charger_recettes_legifrance`) plutot que stocke dans `recette`: ce
      n'est PAS une recette, et l'y ranger faussait le type AUTRES, qui est
      publie tel quel (camembert du tableau de bord). Vaut 0.0 par defaut,
      aucun rattrapage n'etant necessaire pour la plupart des annees.
    - `prelevements_sur_recettes` (PSR): total des lignes source
      "Prelevement(s) sur les recettes de l'Etat au profit des collectivites
      territoriales / de l'Union europeenne" de l'annee, calcule en amont par
      `normalize.extract_prelevements_sur_recettes` (ces lignes ne sont pas
      stockees dans `recette` - voir sa docstring - donc leur total doit
      etre fourni explicitement ici plutot que recalcule depuis la base).
      C'est la methodologie du "tableau d'equilibre" officiel du budget de
      l'Etat: les PSR sont des sommes retrocedees, presentees en deduction
      des recettes brutes plutot qu'additionnees a elles. Vaut 0.0 par
      defaut (aucun PSR a deduire) - notamment pour un run partiel
      (`--depenses-only`) qui ne recalcule pas les PSR de l'annee: dans ce
      cas `recettes_nettes` n'est PAS reactualise avec le PSR le plus
      recent, il reste celui du dernier run ayant traite les recettes de
      cette annee (limitation connue, cf. `api.etl.run`).
    - `deficit` = depenses_nettes - recettes_nettes: c'est le deficit
      budgetaire de l'Etat (recettes - depenses du budget general), PAS le
      deficit "Maastricht" au sens INSEE (perimetre plus large incluant les
      administrations publiques locales et de securite sociale).
    - `dette_pib` reste a None: hors perimetre de cette passe d'ingestion.

    Retourne None (et ne cree/modifie rien) si l'annee n'a pas encore a la
    fois des depenses et des recettes chargees.
    """
    depenses_count = await db.scalar(
        select(func.count()).select_from(Depense).where(Depense.annee == annee)
    )
    recettes_count = await db.scalar(
        select(func.count()).select_from(Recette).where(Recette.annee == annee)
    )
    if not depenses_count or not recettes_count:
        logger.info(
            "annee %d: agregat annee_budget non calcule (depenses=%d, recettes=%d)",
            annee,
            depenses_count or 0,
            recettes_count or 0,
        )
        return None

    depenses_total = float(
        await db.scalar(
            select(func.coalesce(func.sum(Depense.cp), 0)).where(Depense.annee == annee)
        )
    )
    recettes_brutes = float(
        await db.scalar(
            select(func.coalesce(func.sum(Recette.montant_net), 0)).where(Recette.annee == annee)
        )
    )
    # Methodologie du tableau d'equilibre officiel du budget de l'Etat:
    # recettes_nettes = (recettes fiscales + non fiscales)
    #                   + remboursements d'impots d'Etat - PSR
    # (voir docstring ci-dessus et `normalize.PrelevementsSurRecettes`).
    recettes_total = recettes_brutes + remboursements_impots_etat - prelevements_sur_recettes
    deficit = depenses_total - recettes_total

    stmt = pg_insert(AnneeBudget).values(
        annee=annee,
        depenses_nettes=depenses_total,
        recettes_nettes=recettes_total,
        deficit=deficit,
        dette_pib=None,
        source_url=source_url,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[AnneeBudget.annee],
        set_={
            "depenses_nettes": stmt.excluded.depenses_nettes,
            "recettes_nettes": stmt.excluded.recettes_nettes,
            "deficit": stmt.excluded.deficit,
            "source_url": stmt.excluded.source_url,
            # dette_pib n'est volontairement pas ecrase: hors perimetre ici,
            # une valeur renseignee par un futur backfill ne doit pas etre
            # effacee par une relance de cette passe.
        },
    )
    await db.execute(stmt)
    result = await db.execute(select(AnneeBudget).where(AnneeBudget.annee == annee))
    annee_budget = result.scalar_one()
    logger.info(
        "annee %d: annee_budget recalcule (depenses=%.0f, recettes_brutes=%.0f, "
        "psr_deduits=%.0f, recettes_nettes=%.0f, deficit=%.0f)",
        annee,
        depenses_total,
        recettes_brutes,
        prelevements_sur_recettes,
        recettes_total,
        deficit,
    )
    return annee_budget


async def upsert_indicateurs_macro(
    db: AsyncSession,
    pib: dict[int, float],
    population: dict[int, int],
    source_pib_url: dict[int, str],
    source_population_url: str,
) -> None:
    """Insere ou met a jour les indicateurs macro, upsert idempotent sur `annee`.

    Charge l'union des annees presentes dans `pib` OU `population`: une
    annee qui n'a que l'une des deux valeurs garde `NULL` pour l'autre (pas
    de valeur factice). `source_pib_url` est un mapping {annee: url} plutot
    qu'une URL unique, car le PIB nominal provient de deux sources
    distinctes selon l'annee: le CSV principal (1949-2022) et, pour
    2023-2025, une edition Insee Premiere differente par annee (voir
    `api.etl.sources.PIB_CSV_URL` et `PIB_COMPLEMENT_XLSX_URLS`). Pour une
    annee presente uniquement dans `population` (pas de valeur PIB),
    `source_pib_url` n'a logiquement pas d'entree: `source_pib_url` reste
    alors `NULL` pour cette ligne.
    """
    annees = sorted(set(pib) | set(population))
    if not annees:
        return

    values = [
        {
            "annee": annee,
            "pib_courant": pib.get(annee),
            "population": population.get(annee),
            "source_pib_url": source_pib_url.get(annee) if annee in pib else None,
            "source_population_url": source_population_url if annee in population else None,
        }
        for annee in annees
    ]
    stmt = pg_insert(IndicateurMacro).values(values)
    stmt = stmt.on_conflict_do_update(
        index_elements=[IndicateurMacro.annee],
        set_={
            "pib_courant": stmt.excluded.pib_courant,
            "population": stmt.excluded.population,
            "source_pib_url": stmt.excluded.source_pib_url,
            "source_population_url": stmt.excluded.source_population_url,
        },
    )
    await db.execute(stmt)
    logger.info("indicateurs_macro upsertes: %d", len(values))


async def upsert_depenses_fiscales(
    db: AsyncSession, annee: int, records: Sequence[DepenseFiscaleRecord]
) -> int:
    """Recharge les depenses fiscales d'une annee: delete puis reinsert.

    Domaine independant (pas de FK vers mission/programme/action), un
    `numero` de mesure suffit comme identifiant stable au sein d'une annee -
    pas de resolution d'identite inter-annees necessaire (contrairement aux
    missions). Retourne le nombre de lignes inserees.
    """
    await db.execute(delete(DepenseFiscale).where(DepenseFiscale.annee == annee))

    if not records:
        return 0

    values = [
        {
            "annee": annee,
            "numero": r.numero,
            "categorie": r.categorie,
            "sous_categorie": r.sous_categorie,
            "sous_sous_categorie": r.sous_sous_categorie,
            "libelle": r.libelle,
            "beneficiaire": r.beneficiaire,
            "montant_millions": r.montant_millions,
            "statut_montant": r.statut_montant,
            "methode_chiffrage": r.methode_chiffrage,
        }
        for r in records
    ]
    await db.execute(insert(DepenseFiscale), values)
    logger.info("depenses_fiscales upsertees pour %d: %d lignes", annee, len(values))
    return len(values)


async def upsert_marches(db: AsyncSession, batches: Iterable[Sequence[MarcheRecord]]) -> int:
    """Recharge integralement `marche_public`: delete-all puis reinsert par
    lots (streaming depuis le normaliseur, jamais une liste Python complete
    en memoire - cf. docstring de module). Pas d'upsert par cle naturelle
    possible (`id` source non fiable, cf. `MarchePublic.marche_id_source`).

    Le tout dans la meme transaction que le reste de `run_etl()` (deja
    commit/rollback en bloc): un echec en cours de run annule aussi le
    `delete()`, comportement all-or-nothing correct pour une strategie de
    remplacement integral.
    """
    await db.execute(delete(MarchePublic))

    total = 0
    for batch in batches:
        if not batch:
            continue
        values = [
            {
                "marche_id_source": r.marche_id_source,
                "nature": r.nature,
                "objet": r.objet,
                "objet_recherche": r.objet_recherche,
                "codecpv": r.codecpv,
                "codecpv_division": r.codecpv_division,
                "procedure": r.procedure,
                "acheteur_siret": r.acheteur_siret,
                "titulaire_siret": r.titulaire_siret,
                "titulaire_id_type": r.titulaire_id_type,
                "dureemois": r.dureemois,
                "datenotification": r.datenotification,
                "datepublicationdonnees": r.datepublicationdonnees,
                "montant": r.montant,
                "formeprix": r.formeprix,
                "offresrecues": r.offresrecues,
                "marcheinnovant": r.marcheinnovant,
            }
            for r in batch
        ]
        await db.execute(insert(MarchePublic), values)
        total += len(values)

    logger.info("marches upsertes: %d lignes", total)
    return total


async def enregistrer_ingestion_terminee(db: AsyncSession) -> None:
    """Trace la fin d'une execution ETL reussie, pour `derniere_ingestion` (GET /health)."""
    await db.execute(insert(IngestionLog).values(termine_a=datetime.now(UTC)))
