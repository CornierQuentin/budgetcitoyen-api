"""CLI d'orchestration du pipeline ETL: telechargement -> normalisation -> chargement.

Usage:
    python -m api.etl.run [--annees 2019-2025] [--depenses-only] [--recettes-only]

Ingere les depenses de l'Etat (budget general) pour 2019-2025 et les
recettes du budget general pour 2024-2025 (seules annees disponibles sous
forme structuree sur data.economie.gouv.fr - trou de donnees reel pour
2015-2023, documente dans le README/JOURNAL du projet, pas une limitation de
ce script).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Iterable
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from api.db.session import async_session_maker
from api.etl import loader, normalize, sources

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
    await loader.upsert_mission_aliases(db, alias_with_ids)

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


async def _charger_recettes(db: AsyncSession, client: httpx.AsyncClient, annees: list[int]) -> None:
    tous_les_aggregats: list[normalize.RecetteAggregat] = []
    for annee in annees:
        dataset_id = sources.RECETTES_DATASETS_RECORDS[annee]
        raw = await _fetch_all_records(client, dataset_id)
        records = normalize.normalize_recettes_records_json(raw, annee)
        aggregats = normalize.aggregate_recettes(records)
        tous_les_aggregats.extend(aggregats)
        logger.info(
            "recettes %d: %d lignes brutes -> %d types agreges",
            annee,
            len(records),
            len(aggregats),
        )
    await loader.upsert_recettes(db, tous_les_aggregats)


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


async def run_etl(annees: Iterable[int], *, depenses: bool, recettes: bool) -> None:
    annees_list = sorted(set(annees))
    depenses_annees = [a for a in annees_list if a in sources.DEPENSES_ANNEES] if depenses else []
    recettes_annees = [a for a in annees_list if a in sources.RECETTES_ANNEES] if recettes else []

    hors_perimetre = [
        a
        for a in annees_list
        if a not in sources.DEPENSES_ANNEES and a not in sources.RECETTES_ANNEES
    ]
    for a in hors_perimetre:
        logger.warning("annee %d hors perimetre de cette passe d'ingestion (2019-2025), ignoree", a)

    logger.info(
        "demarrage ETL: depenses=%s recettes=%s",
        depenses_annees or "aucune",
        recettes_annees or "aucune",
    )

    source_url_par_annee: dict[int, str] = {}

    async with httpx.AsyncClient() as client, async_session_maker() as db:
        try:
            if depenses_annees:
                source_url_par_annee = await _charger_depenses(db, client, depenses_annees)
            if recettes_annees:
                await _charger_recettes(db, client, recettes_annees)

            # Recalcule l'agregat annee_budget pour toute annee demandee ou
            # depenses ET recettes sont desormais presentes en base (que ce
            # soit charge lors de ce run ou d'un run precedent).
            for annee in annees_list:
                source_url = source_url_par_annee.get(annee) or sources.default_depenses_source_url(
                    annee
                )
                if source_url is None:
                    continue
                await loader.recalculer_annee_budget(db, annee, source_url)

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
        default="2019-2025",
        help="Plage/liste d'annees (ex: '2019-2025', '2024,2025'). Defaut: 2019-2025.",
    )
    groupe = parser.add_mutually_exclusive_group()
    groupe.add_argument("--depenses-only", action="store_true", help="Ne charger que les depenses.")
    groupe.add_argument("--recettes-only", action="store_true", help="Ne charger que les recettes.")
    args = parser.parse_args(argv)

    annees = _parse_annees(args.annees)
    depenses = not args.recettes_only
    recettes = not args.depenses_only

    asyncio.run(run_etl(annees, depenses=depenses, recettes=recettes))


if __name__ == "__main__":
    main()
