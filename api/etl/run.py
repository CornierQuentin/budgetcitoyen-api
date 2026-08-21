"""CLI d'orchestration du pipeline ETL: telechargement -> normalisation -> chargement.

Usage:
    python -m api.etl.run [--annees 2012-2026]
        [--depenses-only | --recettes-only | --indicateurs-only]

Ingere les depenses de l'Etat (budget general) pour 2012-2026 (2015 via
l'API Legifrance/PISTE, cf. ci-dessous), les recettes du budget general
pour 2015-2026 en integralite (trois sources cohabitent: le portail
data.economie.gouv.fr pour 2024-2025, les rapports annuels "Le budget de
l'Etat en <annee>" de la Cour des comptes pour 2016-2020 et 2022-2023, et
l'API Legifrance/PISTE pour 2015/2021/2026 - voir `_charger_recettes`,
`_charger_recettes_cour_des_comptes` et `_charger_recettes_legifrance`
respectivement), et les indicateurs macro (PIB nominal, population - voir
`_charger_indicateurs`). Depenses 2006-2010 restent un trou reel, hors
perimetre de cette passe d'ingestion (verifie: Etat A/B absent du JSON
structure de l'API Legifrance pour ces annees, contrairement a 2015+ -
necessiterait un parsing PDF distinct).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import zipfile
from collections.abc import Iterable
from io import BytesIO
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from api.core.config import get_settings
from api.db.session import async_session_maker
from api.etl import loader, normalize, sources
from api.models.recette import TypeRecette

logger = logging.getLogger("api.etl")


# ---------------------------------------------------------------------------
# Acces reseau (avec retries simples, l'API source etant un service public
# sans SLA garanti)
# ---------------------------------------------------------------------------

_HTTP_TIMEOUT = 30.0
_MAX_ATTEMPTS = 3


async def _get_json(
    client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None
) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            response = await client.get(url, params=params, timeout=_HTTP_TIMEOUT)
            response.raise_for_status()
            result: dict[str, Any] = response.json()
            return result
        except httpx.HTTPError as exc:
            last_exc = exc
            logger.warning(
                "echec requete %s (tentative %d/%d): %s", url, attempt, _MAX_ATTEMPTS, exc
            )
            if attempt < _MAX_ATTEMPTS:
                await asyncio.sleep(2 * attempt)
    assert last_exc is not None
    raise last_exc


async def _get_bytes(client: httpx.AsyncClient, url: str) -> bytes:
    last_exc: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            response = await client.get(url, timeout=_HTTP_TIMEOUT)
            response.raise_for_status()
            return response.content
        except httpx.HTTPError as exc:
            last_exc = exc
            logger.warning(
                "echec requete %s (tentative %d/%d): %s", url, attempt, _MAX_ATTEMPTS, exc
            )
            if attempt < _MAX_ATTEMPTS:
                await asyncio.sleep(2 * attempt)
    assert last_exc is not None
    raise last_exc


async def _fetch_all_records(client: httpx.AsyncClient, dataset_id: str) -> list[dict[str, Any]]:
    """Pagine sur l'endpoint records (v2.1) et retourne toutes les lignes."""
    url = sources.records_url(dataset_id)
    rows: list[dict[str, Any]] = []
    offset = 0
    total_count: int | None = None
    while True:
        data = await _get_json(
            client, url, params={"limit": sources.RECORDS_PAGE_SIZE, "offset": offset}
        )
        page = data.get("results", [])
        rows.extend(page)
        total_count = data.get("total_count", len(rows))
        offset += sources.RECORDS_PAGE_SIZE
        if not page or offset >= total_count:
            break
    logger.info("dataset %s: %d lignes telechargees", dataset_id, len(rows))
    return rows


async def _fetch_attachment_text(
    client: httpx.AsyncClient, dataset_id: str, attachment_id: str, encoding: str = "cp1252"
) -> str:
    content = await _get_bytes(client, sources.attachment_url(dataset_id, attachment_id))
    return content.decode(encoding)


# ---------------------------------------------------------------------------
# API Legifrance (PISTE) - LFI 2026+, OAuth2 client_credentials
# ---------------------------------------------------------------------------

# Cache memoire (le temps du process): un meme texte JORF (une annee) peut
# etre demande deux fois dans le meme run (une fois pour les depenses via
# `_fetch_depenses_annee`, une fois pour les recettes via `_charger_recettes_
# legifrance`) - evite un second aller-retour reseau identique.
_lfi_jorf_cache: dict[str, dict[str, Any]] = {}


async def _get_piste_token(client: httpx.AsyncClient) -> str:
    """Obtient un token OAuth2 (grant client_credentials) aupres de PISTE."""
    settings = get_settings()
    last_exc: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            response = await client.post(
                settings.piste_oauth_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": settings.piste_client_id,
                    "client_secret": settings.piste_client_secret,
                    "scope": "openid",
                },
                timeout=_HTTP_TIMEOUT,
            )
            response.raise_for_status()
            token: str = response.json()["access_token"]
            return token
        except httpx.HTTPError as exc:
            last_exc = exc
            logger.warning(
                "echec obtention token PISTE (tentative %d/%d): %s", attempt, _MAX_ATTEMPTS, exc
            )
            if attempt < _MAX_ATTEMPTS:
                await asyncio.sleep(2 * attempt)
    assert last_exc is not None
    raise last_exc


async def _fetch_lfi_jorf(client: httpx.AsyncClient, text_cid: str) -> dict[str, Any]:
    """Recupere (avec cache memoire) le contenu structure d'un texte JORF via l'API
    Legifrance (`POST /consult/jorf`), authentifie par un token OAuth PISTE
    frais a chaque appel (pas de cache de token: sa duree de vie - de l'ordre
    de l'heure - depasse largement celle d'un run ETL, mais un appel unique
    par texte suffit et evite toute gestion d'expiration)."""
    if text_cid in _lfi_jorf_cache:
        return _lfi_jorf_cache[text_cid]
    settings = get_settings()
    token = await _get_piste_token(client)
    last_exc: Exception | None = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            response = await client.post(
                f"{settings.piste_api_base_url}/consult/jorf",
                json={"textCid": text_cid},
                headers={"Authorization": f"Bearer {token}"},
                timeout=_HTTP_TIMEOUT,
            )
            response.raise_for_status()
            data: dict[str, Any] = response.json()
            _lfi_jorf_cache[text_cid] = data
            return data
        except httpx.HTTPError as exc:
            last_exc = exc
            logger.warning(
                "echec requete /consult/jorf %s (tentative %d/%d): %s",
                text_cid,
                attempt,
                _MAX_ATTEMPTS,
                exc,
            )
            if attempt < _MAX_ATTEMPTS:
                await asyncio.sleep(2 * attempt)
    assert last_exc is not None
    raise last_exc


_MARQUEUR_ETATS_ANNEXES = "ÉTATS LÉGISLATIFS ANNEXÉS"


def _walk_articles(node: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Parcourt recursivement `sections`/`articles` d'une reponse JORF (arborescence
    de l'articulation legale du texte, pas un flux de donnees tabulaire)."""
    yield from node.get("articles") or []
    for section in node.get("sections") or []:
        yield from _walk_articles(section)


def _extract_etats_html(jorf_json: dict[str, Any]) -> tuple[str, str]:
    """Isole les tables HTML "I. - Budget general" d'Etat A (recettes) et
    Etat B (depenses) au sein de la reponse `/consult/jorf`.

    Les etats legislatifs annexes (A a G) sont tous concatenes dans le
    `content` HTML d'UN SEUL article sans numero, repere par la marque
    textuelle "ETATS LEGISLATIFS ANNEXES" (pas par un id d'article fige, qui
    pourrait changer en cas de texte rectificatif - verifie a l'inspection
    reelle: cet article porte un id JORFARTI mais aucun "num"). "ETAT A"/
    "ETAT B"/"ETAT C" marquent le debut de chaque etat.

    Chacun de ces etats peut lui-meme contenir PLUSIEURS tables HTML
    concatenees (Etat A: Budget general, puis Budgets annexes, Comptes
    d'affectation speciale, Comptes de concours financiers - verifie: 5
    tables distinctes; Etat B: une seule table, Budget general uniquement)
    - seule la PREMIERE table de chaque etat est retenue (perimetre "Budget
    general", coherent avec la convention BG-only deja en place pour
    2016-2025, cf. `api.etl.sources`).
    """
    for article in _walk_articles(jorf_json):
        content = article.get("content") or ""
        if _MARQUEUR_ETATS_ANNEXES in content:
            debut_a = content.index("ÉTAT A")
            debut_b = content.index("ÉTAT B", debut_a)
            debut_c = content.index("ÉTAT C", debut_b)
            return (
                _premiere_table_html(content[debut_a:debut_b]),
                _premiere_table_html(content[debut_b:debut_c]),
            )
    raise ValueError("article des etats legislatifs annexes introuvable dans la reponse JORF")


def _premiere_table_html(fragment: str) -> str:
    """Extrait le premier `<table>...</table>` d'un fragment de contenu."""
    debut = fragment.index("<table")
    fin = fragment.index("</table>", debut) + len("</table>")
    return fragment[debut:fin]


# ---------------------------------------------------------------------------
# Etape depenses
# ---------------------------------------------------------------------------


async def _fetch_depenses_annee(
    client: httpx.AsyncClient, annee: int
) -> tuple[list[normalize.DepenseRecord], str]:
    """Telecharge et normalise les depenses brutes d'une annee. Retourne (records, source_url)."""
    if annee in sources.DEPENSES_DATASETS_RECORDS:
        dataset_id = sources.DEPENSES_DATASETS_RECORDS[annee]
        raw = await _fetch_all_records(client, dataset_id)
        records = normalize.normalize_depenses_records_json(raw, annee)
        source_url = sources.records_url(dataset_id)
    elif annee == 2012:
        ids = sources.DEPENSES_DATASETS_2012_2014[2012]
        montants = await _fetch_all_records(client, ids["montants"])
        nomenclature_mission_programme = await _fetch_all_records(
            client, ids["nomenclature_mission_programme"]
        )
        nomenclature_destination = await _fetch_all_records(client, ids["nomenclature_destination"])
        records = normalize.normalize_depenses_2012(
            montants, nomenclature_mission_programme, nomenclature_destination, annee
        )
        source_url = sources.records_url(ids["montants"])
    elif annee == 2013:
        ids = sources.DEPENSES_DATASETS_2012_2014[2013]
        montants = await _fetch_all_records(client, ids["montants"])
        nomenclature_programme = await _fetch_all_records(client, ids["nomenclature_programme"])
        nomenclature_destination = await _fetch_all_records(client, ids["nomenclature_destination"])
        records = normalize.normalize_depenses_2013(
            montants, nomenclature_programme, nomenclature_destination, annee
        )
        source_url = sources.records_url(ids["montants"])
    elif annee == 2014:
        ids = sources.DEPENSES_DATASETS_2012_2014[2014]
        montants = await _fetch_all_records(client, ids["montants"])
        nomenclature_destination = await _fetch_all_records(client, ids["nomenclature_destination"])
        records = normalize.normalize_depenses_2014(montants, nomenclature_destination, annee)
        source_url = sources.records_url(ids["montants"])
    elif annee == 2016:
        dataset_id = sources.DEPENSES_DATASETS_ATTACHMENTS[2016]
        attachment_id = sources.DEPENSES_ATTACHMENT_IDS[2016]["detaillee"]
        text = await _fetch_attachment_text(client, dataset_id, attachment_id)
        records = normalize.normalize_depenses_2016(text, annee)
        source_url = sources.attachment_url(dataset_id, attachment_id)
    elif annee == 2017:
        dataset_id = sources.DEPENSES_DATASETS_ATTACHMENTS[2017]
        attachment_id = sources.DEPENSES_ATTACHMENT_IDS[2017]["detaillee"]
        text = await _fetch_attachment_text(client, dataset_id, attachment_id)
        records = normalize.normalize_depenses_2017(text, annee)
        source_url = sources.attachment_url(dataset_id, attachment_id)
    elif annee == 2018:
        dataset_id = sources.DEPENSES_DATASETS_ATTACHMENTS[2018]
        attachment_id = sources.DEPENSES_ATTACHMENT_IDS[2018]["detaillee"]
        text = await _fetch_attachment_text(client, dataset_id, attachment_id)
        records = normalize.normalize_depenses_2018(text, annee)
        source_url = sources.attachment_url(dataset_id, attachment_id)
    elif annee == 2020:
        dataset_id = sources.DEPENSES_DATASETS_ATTACHMENTS[2020]
        ids = sources.DEPENSES_ATTACHMENT_IDS[2020]
        nomenclature_text = await _fetch_attachment_text(client, dataset_id, ids["nomenclature"])
        credits_text = await _fetch_attachment_text(client, dataset_id, ids["credits"])
        records = normalize.normalize_depenses_2020(nomenclature_text, credits_text, annee)
        source_url = sources.attachment_url(dataset_id, ids["credits"])
    elif annee in (2021, 2022):
        dataset_id = sources.DEPENSES_DATASETS_ATTACHMENTS[annee]
        attachment_id = sources.DEPENSES_ATTACHMENT_IDS[annee]["detaillee"]
        text = await _fetch_attachment_text(client, dataset_id, attachment_id)
        records = normalize.normalize_depenses_attachment_detaillee(text, annee)
        source_url = sources.attachment_url(dataset_id, attachment_id)
    elif annee in sources.DEPENSES_LFI_XLS_PAR_ANNEE:
        # Fichier exploitable du ministere plutot que l'Etat B du Journal
        # officiel: meme loi, memes montants a l'euro pres, mais avec les
        # NUMEROS de programme et la ventilation par ACTION que l'Etat B ne
        # publie pas (cf. `sources.DEPENSES_LFI_XLS_PAR_ANNEE`). `source_url`
        # continue de pointer sur Legifrance: c'est le texte qui ETABLIT ces
        # montants, le fichier n'en est que le rendu chiffre.
        chemin = sources.depenses_lfi_xls_path(annee)
        records = normalize.normalize_depenses_lfi_xls(chemin.read_bytes(), annee)
        source_url = sources.legifrance_url(sources.LFI_TEXT_CID_PAR_ANNEE[annee])
    elif annee in sources.LFI_TEXT_CID_PAR_ANNEE:
        text_cid = sources.LFI_TEXT_CID_PAR_ANNEE[annee]
        jorf = await _fetch_lfi_jorf(client, text_cid)
        _etat_a, etat_b = _extract_etats_html(jorf)
        records = normalize.normalize_depenses_legifrance(etat_b, annee)
        source_url = sources.legifrance_url(text_cid)
    else:
        raise ValueError(f"Annee non supportee pour les depenses dans cette passe: {annee}")

    logger.info("depenses %d: %d lignes normalisees (budget general)", annee, len(records))
    return records, source_url


async def _charger_depenses(
    db: AsyncSession, client: httpx.AsyncClient, annees: list[int]
) -> dict[int, str]:
    """Telecharge, agrege, resout les missions et charge les depenses de plusieurs annees.

    Retourne le mapping {annee: source_url} des annees effectivement traitees.
    """
    aggregats_par_annee: dict[int, list[normalize.DepenseAggregat]] = {}
    source_url_par_annee: dict[int, str] = {}

    for annee in annees:
        records, source_url = await _fetch_depenses_annee(client, annee)
        aggregats = normalize.aggregate_depenses(records)
        aggregats_par_annee[annee] = aggregats
        source_url_par_annee[annee] = source_url
        logger.info("depenses %d: %d actions agregees", annee, len(aggregats))

    identites = normalize.resolve_mission_identities(aggregats_par_annee)
    mission_rows = normalize.build_mission_rows(aggregats_par_annee, identites)
    alias_rows = normalize.build_mission_alias_rows(aggregats_par_annee, identites)

    mission_ids = await loader.upsert_missions(db, mission_rows)

    alias_with_ids = [(alias, mission_ids[(alias.slug, alias.annee_cible)]) for alias in alias_rows]
    await loader.upsert_mission_aliases(db, alias_with_ids, annees)

    for annee in annees:
        aggregats = aggregats_par_annee[annee]
        with_mission_ids = []
        for agg in aggregats:
            cle = normalize.mission_key(agg.mission_code, agg.mission_libelle)
            slug = identites[cle].slug
            mission_id = mission_ids[(slug, annee)]
            with_mission_ids.append((agg, mission_id))
        await loader.upsert_depenses(db, annee, with_mission_ids)

    return source_url_par_annee


# ---------------------------------------------------------------------------
# Etape recettes
# ---------------------------------------------------------------------------


async def _charger_recettes(
    db: AsyncSession, client: httpx.AsyncClient, annees: list[int]
) -> dict[int, float]:
    """Telecharge, normalise et charge les recettes de plusieurs annees.

    Retourne le mapping {annee: total PSR (Md EUR->EUR, cf. plus bas)} des
    "prelevements sur recettes" exclus de `recette` (PSR collectivites
    territoriales + PSR Union europeenne, cf. `normalize.
    extract_prelevements_sur_recettes`), a fournir ensuite a
    `loader.recalculer_annee_budget` pour deduire `recettes_nettes`, selon
    la methodologie du tableau d'equilibre officiel du budget de l'Etat.
    """
    tous_les_aggregats: list[normalize.RecetteAggregat] = []
    psr_par_annee: dict[int, float] = {}
    for annee in annees:
        dataset_id = sources.RECETTES_DATASETS_RECORDS[annee]
        raw = await _fetch_all_records(client, dataset_id)
        records = normalize.normalize_recettes_records_json(raw, annee)
        aggregats = normalize.aggregate_recettes(records)
        tous_les_aggregats.extend(aggregats)
        logger.info(
            "recettes %d: %d lignes brutes -> %d types agreges (Recettes fiscales/non "
            "fiscales uniquement, PSR exclus)",
            annee,
            len(records),
            len(aggregats),
        )

        psr = normalize.extract_prelevements_sur_recettes(raw, annee)
        psr_par_annee[annee] = psr.total
        # Tracabilite: la source (PSR) n'est pas stockee en base pour cette
        # passe (cf. loader.recalculer_annee_budget), donc ce log est le
        # seul enregistrement du montant exclu/soustrait - coherent avec le
        # principe de sourcage systematique du projet.
        logger.info(
            "annee %d: PSR exclus des recettes = %.1f Md EUR (collectivites %.1f + UE %.1f)",
            annee,
            psr.total / 1e9,
            psr.collectivites / 1e9,
            psr.union_europeenne / 1e9,
        )
    await loader.upsert_recettes(db, tous_les_aggregats)
    return psr_par_annee


async def _charger_recettes_legifrance(
    db: AsyncSession, client: httpx.AsyncClient, annees: list[int]
) -> tuple[dict[int, float], dict[int, float]]:
    """Telecharge, normalise et charge les recettes 2015/2021/2026 (API Legifrance/PISTE).

    Troisieme source de recettes (aux cotes de `_charger_recettes` -
    data.economie.gouv.fr 2024-2025 - et `_charger_recettes_cour_des_comptes`
    - Cour des comptes 2016-2020/2022-2023): le texte de la LFI elle-meme,
    via l'API Legifrance (cf. `api.etl.sources`, docstring de module, et
    `_fetch_lfi_jorf`/`_extract_etats_html` ci-dessus). Le meme appel
    `/consult/jorf` sert aussi aux depenses (cf. `_fetch_depenses_annee`)
    - mutualise via `_lfi_jorf_cache`.

    Rattrapage brut/net CIBLE (PAS le meme principe que `_charger_recettes_
    cour_des_comptes`, qui regrossit TOUJOURS la mission entiere - source
    differente, methodologie differente, cf. plus bas): applique
    SEULEMENT aux annees listees dans `api.etl.sources.
    LFI_REMBOURSEMENTS_IMPOTS_ETAT_SEUL` (2026 actuellement), et seulement
    sur le programme "impots d'Etat" (PAS "impots locaux") de la mission
    "Remboursements et degrevements" - cf. docstring de `loader.
    get_remboursements_degrevements_impots_etat_cp` pour le detail complet
    du bug trouve en construisant ce rattrapage.

    Etabli empiriquement en comparant, POUR CHAQUE annee individuellement,
    le deficit calcule au "Solde" officiel de son propre article
    d'equilibre (celui qui precede immediatement Etat A dans le texte de
    loi - article 147 pour 2026, 93 pour 2021, 49 pour 2015) plutot que de
    supposer qu'une regle verifiee sur une annee se generalise:
    - LFI 2026: les montants d'Etat A ("Impot NET sur le revenu", etc.)
      sont dans une convention qui necessite d'ajouter le CP du programme
      "impots d'Etat" aux recettes pour retomber sur le solde officiel
      (-133,5 Md EUR) - sans ce rattrapage, deficit calcule ~274,7 Md EUR.
    - LFI 2015 et 2021: AUCUN rattrapage necessaire - la somme brute des
      lignes d'Etat A (hors PSR) correspond DEJA exactement a la ligne
      "Recettes fiscales brutes + non fiscales" du tableau d'equilibre
      officiel (verifie au euro pres pour les 2 annees), qui est ensuite
      comparee a des depenses elles-memes BRUTES (la mission "Remboursements
      et degrevements" s'annule mathematiquement des DEUX cotes de
      l'equation quand aucun des deux n'est ajuste - c'est le cas different
      de 2026, dont le tableau d'equilibre ne presente pas cette meme
      symetrie brute/nette). Bug reel trouve en verifiant explicitement:
      un premier essai reutilisant le rattrapage "mission entiere" (comme
      pour la Cour des comptes) pour 2015 donnait un deficit de -13,6 Md
      EUR (surplus implausible) au lieu du solde officiel -74,2 Md EUR;
      un deuxieme essai reutilisant le rattrapage "impots d'Etat seul" de
      2026 pour 2021 donnait 43,0 Md EUR au lieu du solde officiel exact
      -172,4 Md EUR - dans les deux cas, seule la comparaison directe a
      l'article d'equilibre officiel de CHAQUE annee a revele l'erreur.

    Retourne DEUX mappings {annee: montant en EUROS}: les PSR (a retrancher,
    comme `_charger_recettes`/`_charger_recettes_cour_des_comptes`) et les
    rattrapages brut/net (a ajouter). Les seconds ne transitent pas par la
    table `recette`: ce ne sont pas des recettes, et les y ranger faussait le
    type AUTRES, publie tel quel dans le camembert du tableau de bord.
    """
    tous_les_aggregats: list[normalize.RecetteAggregat] = []
    psr_par_annee: dict[int, float] = {}
    remboursements_par_annee: dict[int, float] = {}
    for annee in annees:
        text_cid = sources.LFI_TEXT_CID_PAR_ANNEE[annee]
        jorf = await _fetch_lfi_jorf(client, text_cid)
        etat_a, _etat_b = _extract_etats_html(jorf)
        unite_milliers = annee in sources.LFI_ETAT_A_MILLIERS_EUROS

        records = normalize.normalize_recettes_legifrance(
            etat_a, annee, unite_milliers=unite_milliers
        )

        if annee in sources.LFI_REMBOURSEMENTS_IMPOTS_ETAT_SEUL:
            rd_cp = await loader.get_remboursements_degrevements_impots_etat_cp(db, annee)
            if rd_cp:
                remboursements_par_annee[annee] = rd_cp
                logger.info(
                    "annee %d (Legifrance/PISTE): rattrapage brut/net de %.1f Md EUR "
                    "(programme 'impots d'Etat' de 'Remboursements et degrevements'), "
                    "ajoute au TOTAL de l'annee et non a la table `recette` - ce n'est "
                    "pas une recette, cf. loader.recalculer_annee_budget",
                    annee,
                    rd_cp / 1e9,
                )

        aggregats = normalize.aggregate_recettes(records)
        tous_les_aggregats.extend(aggregats)
        logger.info(
            "recettes %d (Legifrance/PISTE): %d lignes brutes -> %d types agreges " "(PSR exclus)",
            annee,
            len(records),
            len(aggregats),
        )

        psr = normalize.extract_prelevements_sur_recettes_legifrance(
            etat_a, annee, unite_milliers=unite_milliers
        )
        psr_par_annee[annee] = psr.total
        logger.info(
            "annee %d (Legifrance/PISTE): PSR exclus des recettes = %.1f Md EUR "
            "(collectivites %.1f + UE %.1f)",
            annee,
            psr.total / 1e9,
            psr.collectivites / 1e9,
            psr.union_europeenne / 1e9,
        )
    await loader.upsert_recettes(db, tous_les_aggregats)
    return psr_par_annee, remboursements_par_annee


def _decode_zip_member(data: bytes) -> str:
    """Decode le contenu d'un membre de ZIP Cour des comptes en texte.

    Les fichiers retenus (voir `api.etl.sources.
    RECETTES_COUR_DES_COMPTES_FICHIER_IMPOT`/`_EQUILIBRE`) sont tous en
    UTF-8 (verifie a l'inspection reelle) mais d'AUTRES membres de ces
    memes ZIP (non utilises ici) sont en CP1252/Latin-1: on essaie donc
    plusieurs codecs par prudence plutot que de supposer l'UTF-8, Latin-1
    en dernier recours ne pouvant jamais echouer (accepte tout octet).
    """
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1")


async def _fetch_zip_member(client: httpx.AsyncClient, zip_url: str, member_name: str) -> str:
    """Telecharge un ZIP et en decode un membre CSV donne en texte."""
    zip_bytes = await _get_bytes(client, zip_url)
    with zipfile.ZipFile(BytesIO(zip_bytes)) as archive:
        return _decode_zip_member(archive.read(member_name))


async def _charger_recettes_cour_des_comptes(
    db: AsyncSession, client: httpx.AsyncClient, annees: list[int]
) -> dict[int, float]:
    """Telecharge, normalise et charge les recettes 2016-2020/2022-2023 (Cour des comptes).

    Source distincte de `_charger_recettes` (data.economie.gouv.fr,
    2024-2025): les rapports annuels "Le budget de l'Etat en <annee>" de la
    Cour des comptes, seule source identifiee couvrant 2016-2023 (2015 et
    2021 exclus - voir `api.etl.sources.RECETTES_COUR_DES_COMPTES_ZIP_URLS`).
    Chaque millesime telecharge un ZIP et en extrait deux membres CSV: le
    tableau "recettes fiscales nettes par impot" (toujours present pour les
    annees couvertes) et, quand disponible (absent pour 2023 - cf.
    `api.etl.sources.RECETTES_COUR_DES_COMPTES_FICHIER_EQUILIBRE`), le
    "tableau d'equilibre" fournissant les recettes non fiscales (ajoutees
    au bucket `TypeRecette.AUTRES`, comme pour 2024-2025 ou elles n'ont pas
    de type dedie) et les PSR (retournes ici, a deduire en aval par
    `loader.recalculer_annee_budget` - meme methodologie que pour
    2024-2025).

    Retourne le mapping {annee: total PSR en EUROS}, comme `_charger_
    recettes`. Une annee sans tableau d'equilibre (2023) n'a PAS d'entree
    dans ce mapping: `recalculer_annee_budget` utilisera alors son defaut
    de 0.0 (aucun PSR deduit) - limitation connue et documentee (cf.
    `api.etl.sources.RECETTES_COUR_DES_COMPTES_FICHIER_EQUILIBRE`), pas un
    oubli.

    Rattrapage "brut/net" (voir `loader.get_remboursements_degrevements_cp`
    pour le detail complet): les tableaux Cour des comptes exploites ici
    sont explicitement en recettes fiscales NETTES, alors que les depenses
    deja chargees par le pipeline "depenses" existant (`upsert_depenses`)
    sont sur une base BRUTE (elles somment la mission "Remboursements et
    degrevements" comme une depense a part entiere, sans la retrancher).
    Sans rattrapage, le deficit calcule par `recalculer_annee_budget`
    serait systematiquement SURESTIME d'environ ce montant (~130-150 Md
    EUR/an) - constate a l'execution reelle. On ajoute donc, pour CHAQUE
    annee traitee ici, le CP deja charge de cette mission au bucket
    `TypeRecette.AUTRES` (le seul bucket "fourre-tout" du modele actuel -
    cf. `TypeRecette`), afin que la somme totale des recettes redevienne
    comparable a la base brute des depenses. Vaut 0.0 (sans effet) pour une
    annee sans depenses chargees (2016-2017): `annee_budget` n'y sera de
    toute facon pas calcule (cf. `recalculer_annee_budget`).
    """
    tous_les_aggregats: list[normalize.RecetteAggregat] = []
    psr_par_annee: dict[int, float] = {}
    for annee in annees:
        zip_url = sources.RECETTES_COUR_DES_COMPTES_ZIP_URLS[annee]
        delimiter = sources.RECETTES_COUR_DES_COMPTES_DELIMITER[annee]
        fichier_impot = sources.RECETTES_COUR_DES_COMPTES_FICHIER_IMPOT[annee]

        impot_text = await _fetch_zip_member(client, zip_url, fichier_impot)
        records = normalize.normalize_recettes_cour_des_comptes(impot_text, annee, delimiter)

        fichier_equilibre = sources.RECETTES_COUR_DES_COMPTES_FICHIER_EQUILIBRE.get(annee)
        if fichier_equilibre is not None:
            equilibre_text = await _fetch_zip_member(client, zip_url, fichier_equilibre)
            non_fiscal, psr = normalize.extract_non_fiscal_et_psr_cour_des_comptes(
                equilibre_text, annee, delimiter
            )
            records = [
                *records,
                normalize.RecetteRecord(annee=annee, type=TypeRecette.AUTRES, montant=non_fiscal),
            ]
            psr_par_annee[annee] = psr.total
            logger.info(
                "annee %d (Cour des comptes): recettes non fiscales LFI = %.1f Md EUR, "
                "PSR exclus = %.1f Md EUR (collectivites %.1f + UE %.1f)",
                annee,
                non_fiscal / 1e9,
                psr.total / 1e9,
                psr.collectivites / 1e9,
                psr.union_europeenne / 1e9,
            )
        else:
            logger.warning(
                "annee %d (Cour des comptes): pas de tableau d'equilibre disponible - "
                "recettes non fiscales et PSR NON pris en compte (recette limitee a la "
                "fiscalite nette par impot)",
                annee,
            )

        rd_cp = await loader.get_remboursements_degrevements_cp(db, annee)
        if rd_cp:
            records = [
                *records,
                normalize.RecetteRecord(annee=annee, type=TypeRecette.AUTRES, montant=rd_cp),
            ]
            logger.info(
                "annee %d (Cour des comptes): +%.1f Md EUR ajoutes a AUTRES (mission "
                "'Remboursements et degrevements' deja chargee comme depense - rattrapage "
                "brut/net, cf. loader.get_remboursements_degrevements_cp)",
                annee,
                rd_cp / 1e9,
            )

        aggregats = normalize.aggregate_recettes(records)
        tous_les_aggregats.extend(aggregats)
        logger.info(
            "recettes %d (Cour des comptes, colonne LFI): %d types agreges",
            annee,
            len(aggregats),
        )

    await loader.upsert_recettes(db, tous_les_aggregats)
    return psr_par_annee


# ---------------------------------------------------------------------------
# Etape indicateurs macro (PIB nominal, population)
# ---------------------------------------------------------------------------


async def _charger_indicateurs(db: AsyncSession, client: httpx.AsyncClient) -> None:
    """Telecharge, normalise et charge le PIB nominal et la population.

    A la difference des depenses/recettes, ces sources ne sont pas
    decoupees par annee (un CSV et un xlsx couvrant chacun tout
    l'historique disponible): cette etape recharge donc systematiquement
    tout l'historique a chaque run, independamment du filtre `--annees`.
    """
    pib_csv = await _get_bytes(client, sources.PIB_CSV_URL)
    pib = normalize.normalize_pib_csv(pib_csv)
    source_pib_url = {annee: sources.PIB_CSV_URL for annee in pib}
    logger.info("PIB (CSV principal): %d annees normalisees (jusqu'a %d)", len(pib), max(pib))

    for annee, url in sources.PIB_COMPLEMENT_XLSX_URLS.items():
        contenu = await _get_bytes(client, url)
        pib[annee] = normalize.normalize_pib_complement_insee_premiere(contenu, annee)
        source_pib_url[annee] = url
    logger.info(
        "PIB (complement Insee Premiere): %d annees ajoutees (%s)",
        len(sources.PIB_COMPLEMENT_XLSX_URLS),
        sorted(sources.PIB_COMPLEMENT_XLSX_URLS),
    )

    population_xlsx = await _get_bytes(client, sources.POPULATION_XLSX_URL)
    population = normalize.normalize_population_xlsx(population_xlsx)
    logger.info(
        "population: %d annees normalisees (%d-%d)",
        len(population),
        min(population),
        max(population),
    )

    await loader.upsert_indicateurs_macro(
        db, pib, population, source_pib_url, sources.POPULATION_XLSX_URL
    )


# ---------------------------------------------------------------------------
# Etape depenses fiscales (niches fiscales)
# ---------------------------------------------------------------------------


async def _charger_depenses_fiscales(db: AsyncSession, client: httpx.AsyncClient) -> None:
    """Telecharge, normalise et charge les depenses fiscales (niche fiscale).

    A la difference de `_charger_indicateurs`, cette source ne fournit
    qu'un seul millesime figé (2021, cf. `sources.DEPENSE_FISCALE_ANNEE`) et
    ne changera plus jamais: pas de raison de la retelecharger a chaque run
    complet (`run_etl(depenses_fiscales=False)` par defaut), uniquement via
    `--depenses-fiscales-only`.
    """
    contenu = await _get_bytes(
        client,
        sources.attachment_url(
            sources.DEPENSE_FISCALE_DATASET_ID, sources.DEPENSE_FISCALE_ATTACHMENT_ID
        ),
    )
    records = normalize.normalize_depenses_fiscales_xlsx(contenu, sources.DEPENSE_FISCALE_ANNEE)
    n = await loader.upsert_depenses_fiscales(db, sources.DEPENSE_FISCALE_ANNEE, records)
    logger.info("depenses fiscales %d: %d mesures chargees", sources.DEPENSE_FISCALE_ANNEE, n)


# ---------------------------------------------------------------------------
# Etape marches publics (DECP)
# ---------------------------------------------------------------------------


async def _charger_marches(db: AsyncSession, client: httpx.AsyncClient) -> None:
    """Telecharge (export Parquet en masse), normalise et charge l'integralite
    des ~689 000 marches publics.

    A la difference de `depenses`/`recettes` (rechargees a chaque run par
    defaut) mais pour une raison differente de `depenses_fiscales` (millesime
    fige): cette source EST mise a jour quotidiennement, mais retelecharger
    82,6 Mo et refaire un delete+reinsert complet de la table (5 index a
    reconstruire, dont un GIN trigram) a CHAQUE run de routine est un cout
    recurrent reel pour un domaine ou la fraicheur au jour pres n'a aucune
    valeur produit - reste `False` par defaut (`run_etl(marches=False)`),
    uniquement via `--marches-only`, sur une cadence decouplee (recommande:
    hebdomadaire, pas a chaque run depenses/recettes).
    """
    contenu = await _get_bytes(client, sources.parquet_export_url(sources.MARCHES_DATASET_ID))
    batches = normalize.normalize_marches_parquet(contenu)
    n = await loader.upsert_marches(db, batches)
    logger.info("marches publics: %d lignes chargees", n)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _parse_annees(spec: str) -> list[int]:
    """Parse une specification d'annees: "2019-2025", "2024,2025" ou combinaison."""
    annees: set[int] = set()
    for morceau in spec.split(","):
        morceau = morceau.strip()
        if not morceau:
            continue
        if "-" in morceau:
            debut_str, fin_str = morceau.split("-", 1)
            debut, fin = int(debut_str), int(fin_str)
            annees.update(range(debut, fin + 1))
        else:
            annees.add(int(morceau))
    return sorted(annees)


async def run_etl(
    annees: Iterable[int],
    *,
    depenses: bool,
    recettes: bool,
    indicateurs: bool = True,
    depenses_fiscales: bool = False,
    marches: bool = False,
) -> None:
    annees_list = sorted(set(annees))
    depenses_annees = [a for a in annees_list if a in sources.DEPENSES_ANNEES] if depenses else []
    # Les recettes proviennent de deux sources distinctes selon l'annee (cf.
    # docstring de module): OpenDataSoft (2024-2025) et Cour des comptes
    # (2016-2020, 2022-2023). Une annee couverte par les deux listes (aucun
    # cas actuel) serait traitee par les deux, ce qui n'est pas souhaitable
    # mais n'arrive pas en pratique (les deux plages sont disjointes).
    recettes_annees = [a for a in annees_list if a in sources.RECETTES_ANNEES] if recettes else []
    recettes_annees_ccomptes = (
        [a for a in annees_list if a in sources.RECETTES_COUR_DES_COMPTES_ANNEES]
        if recettes
        else []
    )
    recettes_annees_legifrance = (
        [a for a in annees_list if a in sources.RECETTES_LEGIFRANCE_ANNEES] if recettes else []
    )

    hors_perimetre = [
        a
        for a in annees_list
        if a not in sources.DEPENSES_ANNEES
        and a not in sources.RECETTES_ANNEES
        and a not in sources.RECETTES_COUR_DES_COMPTES_ANNEES
        and a not in sources.RECETTES_LEGIFRANCE_ANNEES
    ]
    for a in hors_perimetre:
        logger.warning(
            "annee %d hors perimetre de cette passe d'ingestion (depenses: 2012-2026 "
            "hors 2006-2010, recettes: 2015-2026), ignoree",
            a,
        )

    logger.info(
        "demarrage ETL: depenses=%s recettes(opendatasoft)=%s recettes(cour des comptes)=%s "
        "recettes(legifrance)=%s indicateurs=%s depenses_fiscales=%s marches=%s",
        depenses_annees or "aucune",
        recettes_annees or "aucune",
        recettes_annees_ccomptes or "aucune",
        recettes_annees_legifrance or "aucune",
        indicateurs,
        depenses_fiscales,
        marches,
    )

    source_url_par_annee: dict[int, str] = {}
    psr_par_annee: dict[int, float] = {}
    # Rattrapage brut/net par annee, AJOUTE au total (symetrique du PSR, qui en
    # est retranche). Volontairement hors de la table `recette`: ce n'est pas
    # une recette, et l'y ranger faussait le type AUTRES publie tel quel.
    remboursements_par_annee: dict[int, float] = {}

    # follow_redirects=True: les ressources data.gouv.fr (PIB nominal) sont
    # exposees via une URL stable qui redirige (302) vers l'hebergement
    # statique reel du fichier - a la difference des endpoints
    # data.economie.gouv.fr utilises pour depenses/recettes, qui repondent
    # directement en 200.
    async with (
        httpx.AsyncClient(follow_redirects=True) as client,
        async_session_maker() as db,
    ):
        try:
            if depenses_annees:
                source_url_par_annee = await _charger_depenses(db, client, depenses_annees)
            if recettes_annees:
                psr_par_annee.update(await _charger_recettes(db, client, recettes_annees))
            if recettes_annees_ccomptes:
                psr_par_annee.update(
                    await _charger_recettes_cour_des_comptes(db, client, recettes_annees_ccomptes)
                )
            if recettes_annees_legifrance:
                psr_legifrance, remboursements = await _charger_recettes_legifrance(
                    db, client, recettes_annees_legifrance
                )
                psr_par_annee.update(psr_legifrance)
                remboursements_par_annee.update(remboursements)
            if indicateurs:
                await _charger_indicateurs(db, client)
            if depenses_fiscales:
                await _charger_depenses_fiscales(db, client)
            if marches:
                await _charger_marches(db, client)

            # Recalcule l'agregat annee_budget pour toute annee demandee ou
            # depenses ET recettes sont desormais presentes en base (que ce
            # soit charge lors de ce run ou d'un run precedent).
            #
            # Uniquement si `depenses` ou `recettes` est demande par ce run:
            # `--indicateurs-only` ne doit PAS toucher `annee_budget` (hors
            # de son perimetre). Sans cette garde, ce recalcul s'executerait
            # quand meme pour toute annee de `annees_list` deja chargee lors
            # d'un run precedent - avec un PSR par defaut de 0.0 (non
            # deduit, cf. limitation ci-dessous), corrompant silencieusement
            # `recettes_nettes`/`deficit` d'une annee deja correctement
            # calculee (constate a l'execution reelle: cf. JOURNAL).
            #
            # Limitation connue (inchangee): `psr_par_annee` ne contient que
            # les PSR des annees de recettes traitees PENDANT ce run. Un run
            # `--depenses-only` portant sur une annee dont les recettes ont
            # ete chargees lors d'un run precedent recalculera
            # `recettes_nettes` avec un PSR par defaut de 0.0 (non deduit) -
            # cf. docstring de `loader.recalculer_annee_budget`. Non
            # bloquant pour cette passe (le run complet 2016-2025 traite
            # toujours depenses+recettes ensemble), documente pour une
            # passe future si des runs partiels reguliers sont introduits.
            if depenses or recettes:
                for annee in annees_list:
                    source_url = source_url_par_annee.get(
                        annee
                    ) or sources.default_depenses_source_url(annee)
                    if source_url is None:
                        continue
                    await loader.recalculer_annee_budget(
                        db,
                        annee,
                        source_url,
                        psr_par_annee.get(annee, 0.0),
                        remboursements_par_annee.get(annee, 0.0),
                    )

            await loader.enregistrer_ingestion_terminee(db)
            await db.commit()
            logger.info("ETL termine avec succes, transaction validee")
        except Exception:
            await db.rollback()
            logger.exception("echec de l'ETL, transaction annulee")
            raise


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(
        description="Pipeline ETL BudgetCitoyen (depenses/recettes Etat)"
    )
    parser.add_argument(
        "--annees",
        default="2012-2026",
        help="Plage/liste d'annees (ex: '2012-2026', '2024,2025'). Defaut: 2012-2026.",
    )
    groupe = parser.add_mutually_exclusive_group()
    groupe.add_argument("--depenses-only", action="store_true", help="Ne charger que les depenses.")
    groupe.add_argument("--recettes-only", action="store_true", help="Ne charger que les recettes.")
    groupe.add_argument(
        "--indicateurs-only",
        action="store_true",
        help="Ne charger que les indicateurs macro (PIB, population).",
    )
    groupe.add_argument(
        "--depenses-fiscales-only",
        action="store_true",
        help="Ne charger que les depenses fiscales (niches fiscales).",
    )
    groupe.add_argument(
        "--marches-only",
        action="store_true",
        help="Ne charger que les marches publics (DECP, ~689 000 lignes).",
    )
    args = parser.parse_args(argv)

    annees = _parse_annees(args.annees)
    seulement_un_domaine_a_part = args.depenses_fiscales_only or args.marches_only
    depenses = not (args.recettes_only or args.indicateurs_only or seulement_un_domaine_a_part)
    recettes = not (args.depenses_only or args.indicateurs_only or seulement_un_domaine_a_part)
    indicateurs = not (args.depenses_only or args.recettes_only or seulement_un_domaine_a_part)
    depenses_fiscales = args.depenses_fiscales_only
    marches = args.marches_only

    asyncio.run(
        run_etl(
            annees,
            depenses=depenses,
            recettes=recettes,
            indicateurs=indicateurs,
            depenses_fiscales=depenses_fiscales,
            marches=marches,
        )
    )


if __name__ == "__main__":
    main()
