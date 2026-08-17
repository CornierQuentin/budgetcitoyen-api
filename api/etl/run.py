"""CLI d'orchestration du pipeline ETL: telechargement -> normalisation -> chargement.

Usage:
    python -m api.etl.run [--annees 2019-2025]
        [--depenses-only | --recettes-only | --indicateurs-only]

Ingere les depenses de l'Etat (budget general) pour 2019-2025, les recettes
du budget general pour 2016-2020/2022-2025 (deux sources cohabitent: le
portail data.economie.gouv.fr pour 2024-2025, et les rapports annuels "Le
budget de l'Etat en <annee>" de la Cour des comptes pour 2016-2020 et
2022-2023 - voir `_charger_recettes` et `_charger_recettes_cour_des_comptes`
respectivement), et les indicateurs macro (PIB nominal, population - voir
`_charger_indicateurs`). 2015 et 2021 restent des trous reels (aucune des
deux sources ne fournit de tableau exploitable pour ces annees - voir
`api.etl.sources.RECETTES_COUR_DES_COMPTES_ZIP_URLS`).
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
    annee sans depenses chargees (2016-2018): `annee_budget` n'y sera de
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
    annees: Iterable[int], *, depenses: bool, recettes: bool, indicateurs: bool = True
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

    hors_perimetre = [
        a
        for a in annees_list
        if a not in sources.DEPENSES_ANNEES
        and a not in sources.RECETTES_ANNEES
        and a not in sources.RECETTES_COUR_DES_COMPTES_ANNEES
    ]
    for a in hors_perimetre:
        logger.warning(
            "annee %d hors perimetre de cette passe d'ingestion (depenses: 2019-2025, "
            "recettes: 2016-2020/2022-2025), ignoree",
            a,
        )

    logger.info(
        "demarrage ETL: depenses=%s recettes(opendatasoft)=%s recettes(cour des comptes)=%s "
        "indicateurs=%s",
        depenses_annees or "aucune",
        recettes_annees or "aucune",
        recettes_annees_ccomptes or "aucune",
        indicateurs,
    )

    source_url_par_annee: dict[int, str] = {}
    psr_par_annee: dict[int, float] = {}

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
            if indicateurs:
                await _charger_indicateurs(db, client)

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
            # bloquant pour cette passe (le run complet 2019-2025 traite
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
                        db, annee, source_url, psr_par_annee.get(annee, 0.0)
                    )

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
    groupe.add_argument(
        "--indicateurs-only",
        action="store_true",
        help="Ne charger que les indicateurs macro (PIB, population).",
    )
    args = parser.parse_args(argv)

    annees = _parse_annees(args.annees)
    depenses = not (args.recettes_only or args.indicateurs_only)
    recettes = not (args.depenses_only or args.indicateurs_only)
    indicateurs = not (args.depenses_only or args.recettes_only)

    asyncio.run(run_etl(annees, depenses=depenses, recettes=recettes, indicateurs=indicateurs))


if __name__ == "__main__":
    main()
