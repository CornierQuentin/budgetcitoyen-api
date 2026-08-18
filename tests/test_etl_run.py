"""Tests d'orchestration ETL (`api.etl.run`).

Ce module etait a 0% de couverture (seul vrai trou confirme sur un run
pytest --cov honnete, cf. JOURNAL.md) alors qu'il concentre la logique la
plus a risque du pipeline: c'est un bug ici (mauvais cablage entre etapes,
mauvaise gestion d'erreur) qui a deja cause de vrais incidents en
production (PSR mal comptabilise, DELETE non borne sur mission_alias -
voir JOURNAL.md). On isole ce module de la couche reseau (httpx, via des
`httpx.MockTransport` ou des bouchons `monkeypatch`) et, pour les etapes
`_charger_*`, de la couche `normalize` (deja testee independamment a 96%
dans test_etl_normalize.py): on verifie ici le CABLAGE (quelle fonction
est appelee avec quels arguments, comment les etapes s'enchainent, comment
une erreur se propage), pas la logique de parsing elle-meme.

Pas de recherche d'exhaustivite sur les ~15 branches par annee de
`_fetch_depenses_annee` (chacune delegue a une fonction `normalize.*` deja
testee): un echantillon representatif de chaque forme de branche suffit
pour couvrir la logique de dispatch elle-meme.
"""

from __future__ import annotations

import asyncio
import zipfile
from io import BytesIO

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from api.db.session import engine as _prod_engine
from api.etl import loader, normalize, run, sources
from api.models.indicateur_macro import IndicateurMacro
from api.models.mission import Mission
from api.models.recette import Recette, TypeRecette


async def _no_sleep(*_args: object, **_kwargs: object) -> None:
    return None


@pytest_asyncio.fixture(autouse=True)
async def _dispose_moteur_production_apres_chaque_test():
    """`run_etl` utilise le moteur SQLAlchemy applicatif (`api.db.session.engine`,
    global, cree une seule fois a l'import), pas la fixture `db_engine`
    (NullPool dedie par test, cf. conftest.py). Sans ce nettoyage, son pool par
    defaut peut reutiliser une connexion asyncpg liee a la boucle d'evenements
    d'un test precedent - deja fermee par pytest-asyncio (une boucle par test) -
    provoquant une erreur "another operation is in progress" sur un test
    suivant. Dispose le pool apres chaque test pour forcer une connexion fraiche
    liee a la boucle du test suivant."""
    yield
    await _prod_engine.dispose()


# ---------------------------------------------------------------------------
# _parse_annees
# ---------------------------------------------------------------------------


def test_parse_annees_plage() -> None:
    assert run._parse_annees("2019-2022") == [2019, 2020, 2021, 2022]


def test_parse_annees_liste() -> None:
    assert run._parse_annees("2019,2021,2023") == [2019, 2021, 2023]


def test_parse_annees_combinaison_et_dedoublonne() -> None:
    assert run._parse_annees("2019-2021,2020,2023") == [2019, 2020, 2021, 2023]


def test_parse_annees_ignore_segments_vides() -> None:
    assert run._parse_annees("2019-2020,,2022") == [2019, 2020, 2022]


# ---------------------------------------------------------------------------
# _get_json / _get_bytes: retries reseau
# ---------------------------------------------------------------------------


async def test_get_json_reussit_du_premier_coup() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run._get_json(client, "https://example.test/x")

    assert result == {"ok": True}


async def test_get_json_reessaie_puis_reussit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    appels = {"n": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        appels["n"] += 1
        if appels["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run._get_json(client, "https://example.test/x")

    assert result == {"ok": True}
    assert appels["n"] == 3


async def test_get_json_echoue_apres_le_max_de_tentatives(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await run._get_json(client, "https://example.test/x")


async def test_get_bytes_reussit() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"hello")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await run._get_bytes(client, "https://example.test/x")

    assert result == b"hello"


async def test_get_bytes_echoue_apres_le_max_de_tentatives(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await run._get_bytes(client, "https://example.test/x")


# ---------------------------------------------------------------------------
# _fetch_all_records: pagination
# ---------------------------------------------------------------------------


async def test_fetch_all_records_pagine_jusqua_total_count() -> None:
    page1 = [{"id": i} for i in range(100)]
    page2 = [{"id": i} for i in range(100, 150)]
    pages = [
        {"results": page1, "total_count": 150},
        {"results": page2, "total_count": 150},
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=pages.pop(0))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        rows = await run._fetch_all_records(client, "un-jeu-de-donnees")

    assert [r["id"] for r in rows] == list(range(150))


async def test_fetch_all_records_page_vide_arrete_la_pagination() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"results": [], "total_count": 0})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        rows = await run._fetch_all_records(client, "un-jeu-de-donnees")

    assert rows == []


# ---------------------------------------------------------------------------
# _decode_zip_member: repli d'encodage
# ---------------------------------------------------------------------------


def test_decode_zip_member_utf8() -> None:
    assert run._decode_zip_member("héllo".encode("utf-8-sig")) == "héllo"


def test_decode_zip_member_repli_cp1252() -> None:
    # 'é' seul en cp1252 (0xE9) n'est pas un octet UTF-8 valide (lead byte
    # sans continuation) : doit basculer sur le deuxieme codec essaye.
    assert run._decode_zip_member("café".encode("cp1252")) == "café"


def test_decode_zip_member_repli_latin1_en_dernier_recours() -> None:
    # 0x81 est un octet non assigne en cp1252 (UnicodeDecodeError) tout en
    # etant invalide en UTF-8 seul : seul le repli latin-1 (qui accepte tout
    # octet) peut aboutir.
    assert run._decode_zip_member(b"\x81") == "\x81"


async def test_fetch_attachment_text_decode_avec_l_encodage_fourni(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_get_bytes(_client: httpx.AsyncClient, _url: str) -> bytes:
        return "café".encode("cp1252")

    monkeypatch.setattr(run, "_get_bytes", _fake_get_bytes)

    async with httpx.AsyncClient() as client:
        text = await run._fetch_attachment_text(
            client, "dataset-x", "attachment-y", encoding="cp1252"
        )

    assert text == "café"


async def test_fetch_zip_member_extrait_et_decode_le_bon_membre(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("cible.csv", "a;b\n1;2")
        archive.writestr("autre.csv", "ignore-moi")

    async def _fake_get_bytes(_client: httpx.AsyncClient, _url: str) -> bytes:
        return buffer.getvalue()

    monkeypatch.setattr(run, "_get_bytes", _fake_get_bytes)

    async with httpx.AsyncClient() as client:
        text = await run._fetch_zip_member(client, "https://example.test/archive.zip", "cible.csv")

    assert text == "a;b\n1;2"


# ---------------------------------------------------------------------------
# _fetch_depenses_annee: dispatch par annee
# ---------------------------------------------------------------------------


async def test_fetch_depenses_annee_json_records(monkeypatch: pytest.MonkeyPatch) -> None:
    annee = next(iter(sources.DEPENSES_DATASETS_RECORDS))
    sentinel = [object()]

    async def _fake_fetch_all_records(_client: httpx.AsyncClient, _dataset_id: str) -> list:
        return [{"brut": True}]

    monkeypatch.setattr(run, "_fetch_all_records", _fake_fetch_all_records)
    monkeypatch.setattr(normalize, "normalize_depenses_records_json", lambda raw, a: sentinel)

    async with httpx.AsyncClient() as client:
        records, source_url = await run._fetch_depenses_annee(client, annee)

    assert records is sentinel
    assert source_url == sources.records_url(sources.DEPENSES_DATASETS_RECORDS[annee])


async def test_fetch_depenses_annee_2012_cas_special_trois_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = [object()]

    async def _fake_fetch_all_records(_client: httpx.AsyncClient, _dataset_id: str) -> list:
        return [{}]

    monkeypatch.setattr(run, "_fetch_all_records", _fake_fetch_all_records)
    monkeypatch.setattr(normalize, "normalize_depenses_2012", lambda *_a, **_k: sentinel)

    async with httpx.AsyncClient() as client:
        records, source_url = await run._fetch_depenses_annee(client, 2012)

    assert records is sentinel
    ids = sources.DEPENSES_DATASETS_2012_2014[2012]
    assert source_url == sources.records_url(ids["montants"])


async def test_fetch_depenses_annee_2016_cas_attachment(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = [object()]

    async def _fake_fetch_attachment_text(
        _client: httpx.AsyncClient, _dataset_id: str, _attachment_id: str, encoding: str = "cp1252"
    ) -> str:
        return "texte-brut"

    monkeypatch.setattr(run, "_fetch_attachment_text", _fake_fetch_attachment_text)
    monkeypatch.setattr(normalize, "normalize_depenses_2016", lambda text, a: sentinel)

    async with httpx.AsyncClient() as client:
        records, source_url = await run._fetch_depenses_annee(client, 2016)

    assert records is sentinel
    dataset_id = sources.DEPENSES_DATASETS_ATTACHMENTS[2016]
    attachment_id = sources.DEPENSES_ATTACHMENT_IDS[2016]["detaillee"]
    assert source_url == sources.attachment_url(dataset_id, attachment_id)


async def test_fetch_depenses_annee_2020_cas_double_attachment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = [object()]

    async def _fake_fetch_attachment_text(
        _client: httpx.AsyncClient, _dataset_id: str, _attachment_id: str, encoding: str = "cp1252"
    ) -> str:
        return "texte-brut"

    monkeypatch.setattr(run, "_fetch_attachment_text", _fake_fetch_attachment_text)
    monkeypatch.setattr(
        normalize, "normalize_depenses_2020", lambda nomenclature, credits_, a: sentinel
    )

    async with httpx.AsyncClient() as client:
        records, source_url = await run._fetch_depenses_annee(client, 2020)

    assert records is sentinel
    dataset_id = sources.DEPENSES_DATASETS_ATTACHMENTS[2020]
    ids = sources.DEPENSES_ATTACHMENT_IDS[2020]
    assert source_url == sources.attachment_url(dataset_id, ids["credits"])


async def test_fetch_depenses_annee_2021_2022_branche_partagee(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = [object()]

    async def _fake_fetch_attachment_text(
        _client: httpx.AsyncClient, _dataset_id: str, _attachment_id: str, encoding: str = "cp1252"
    ) -> str:
        return "texte-brut"

    monkeypatch.setattr(run, "_fetch_attachment_text", _fake_fetch_attachment_text)
    monkeypatch.setattr(
        normalize, "normalize_depenses_attachment_detaillee", lambda text, a: sentinel
    )

    async with httpx.AsyncClient() as client:
        records_2021, _ = await run._fetch_depenses_annee(client, 2021)
        records_2022, _ = await run._fetch_depenses_annee(client, 2022)

    assert records_2021 is sentinel
    assert records_2022 is sentinel


async def test_fetch_depenses_annee_leve_pour_une_annee_hors_perimetre() -> None:
    async with httpx.AsyncClient() as client:
        with pytest.raises(ValueError, match="2015"):
            await run._fetch_depenses_annee(client, 2015)


# ---------------------------------------------------------------------------
# _charger_depenses / _charger_recettes / _charger_recettes_cour_des_comptes
# / _charger_indicateurs: cablage fetch -> normalize -> load (DB reelle)
# ---------------------------------------------------------------------------


def _depense_record(annee: int) -> normalize.DepenseRecord:
    return normalize.DepenseRecord(
        annee=annee,
        mission_code="JA",
        mission_libelle="Justice",
        programme_code="101",
        programme_libelle="Programme test",
        action_code="01",
        action_libelle="Action test",
        ae=1000.0,
        cp=900.0,
    )


async def test_charger_depenses_enchaine_fetch_normalize_et_load(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    annee = 2024

    async def _fake_fetch(_client: httpx.AsyncClient, a: int) -> tuple[list, str]:
        return [_depense_record(a)], "https://source.test/depenses"

    monkeypatch.setattr(run, "_fetch_depenses_annee", _fake_fetch)

    async with httpx.AsyncClient() as client:
        source_url_par_annee = await run._charger_depenses(db_session, client, [annee])
    await db_session.commit()

    assert source_url_par_annee == {annee: "https://source.test/depenses"}
    mission = (await db_session.execute(select(Mission).where(Mission.annee == annee))).scalar_one()
    assert mission.nom_officiel == "Justice"


async def test_charger_recettes_enchaine_fetch_normalize_et_load(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    annee = 2024
    recette_records = [normalize.RecetteRecord(annee=annee, type=TypeRecette.TVA, montant=100.0)]
    psr = normalize.PrelevementsSurRecettes(annee=annee, collectivites=5.0, union_europeenne=2.0)

    async def _fake_fetch_all_records(_client: httpx.AsyncClient, _dataset_id: str) -> list:
        return [{"brut": True}]

    monkeypatch.setattr(run, "_fetch_all_records", _fake_fetch_all_records)
    monkeypatch.setattr(
        normalize, "normalize_recettes_records_json", lambda raw, a: recette_records
    )
    monkeypatch.setattr(normalize, "extract_prelevements_sur_recettes", lambda raw, a: psr)

    async with httpx.AsyncClient() as client:
        psr_par_annee = await run._charger_recettes(db_session, client, [annee])
    await db_session.commit()

    assert psr_par_annee == {annee: 7.0}
    recette = (
        await db_session.execute(
            select(Recette).where(Recette.annee == annee, Recette.type == TypeRecette.TVA)
        )
    ).scalar_one()
    assert recette.montant_net == 100.0


async def test_charger_recettes_cour_des_comptes_avec_tableau_equilibre(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    annee = 2019
    assert sources.RECETTES_COUR_DES_COMPTES_FICHIER_EQUILIBRE.get(annee) is not None

    # Mission "RD" deja chargee avec des depenses (cote pipeline "depenses") :
    # exerce la branche de rattrapage brut/net (`rd_cp`), qui a deja ete la
    # source d'un vrai bug de deficit surestime (cf. docstring de
    # `loader.get_remboursements_degrevements_cp`).
    mapping = await loader.upsert_missions(
        db_session,
        [
            normalize.MissionYearRow(
                slug="remboursements-et-degrevements",
                nom_normalise="remboursements et degrevements",
                nom_officiel="Remboursements et degrevements",
                annee=annee,
                code_mission="RD",
            )
        ],
    )
    await db_session.commit()
    rd_aggregat = normalize.DepenseAggregat(
        annee=annee,
        mission_code="RD",
        mission_libelle="Remboursements et degrevements",
        programme_code="RD-P1",
        programme_libelle="Programme RD",
        action_code="RD-A1",
        action_libelle="Action RD",
        ae=200.0,
        cp=200.0,
    )
    await loader.upsert_depenses(
        db_session, annee, [(rd_aggregat, mapping[("remboursements-et-degrevements", annee)])]
    )
    await db_session.commit()

    fiscal_records = [normalize.RecetteRecord(annee=annee, type=TypeRecette.IR, montant=50.0)]
    psr = normalize.PrelevementsSurRecettes(annee=annee, collectivites=1.0, union_europeenne=1.0)

    async def _fake_fetch_zip_member(
        _client: httpx.AsyncClient, _zip_url: str, _member_name: str
    ) -> str:
        return "contenu-brut"

    monkeypatch.setattr(run, "_fetch_zip_member", _fake_fetch_zip_member)
    monkeypatch.setattr(
        normalize, "normalize_recettes_cour_des_comptes", lambda text, a, d: fiscal_records
    )
    monkeypatch.setattr(
        normalize,
        "extract_non_fiscal_et_psr_cour_des_comptes",
        lambda text, a, d: (30.0, psr),
    )

    async with httpx.AsyncClient() as client:
        psr_par_annee = await run._charger_recettes_cour_des_comptes(db_session, client, [annee])
    await db_session.commit()

    assert psr_par_annee == {annee: 2.0}
    autres = (
        await db_session.execute(
            select(Recette).where(Recette.annee == annee, Recette.type == TypeRecette.AUTRES)
        )
    ).scalar_one()
    # Non fiscal (30.0) + rd_cp (200.0, depense RD seedee ci-dessus)
    assert autres.montant_net == 230.0


async def test_charger_recettes_cour_des_comptes_sans_tableau_equilibre(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    annee = next(
        a
        for a in sources.RECETTES_COUR_DES_COMPTES_ANNEES
        if sources.RECETTES_COUR_DES_COMPTES_FICHIER_EQUILIBRE.get(a) is None
    )
    fiscal_records = [normalize.RecetteRecord(annee=annee, type=TypeRecette.IR, montant=50.0)]

    async def _fake_fetch_zip_member(
        _client: httpx.AsyncClient, _zip_url: str, _member_name: str
    ) -> str:
        return "contenu-brut"

    monkeypatch.setattr(run, "_fetch_zip_member", _fake_fetch_zip_member)
    monkeypatch.setattr(
        normalize, "normalize_recettes_cour_des_comptes", lambda text, a, d: fiscal_records
    )

    with caplog.at_level("WARNING"):
        async with httpx.AsyncClient() as client:
            psr_par_annee = await run._charger_recettes_cour_des_comptes(
                db_session, client, [annee]
            )
    await db_session.commit()

    assert annee not in psr_par_annee
    assert "pas de tableau d'equilibre" in caplog.text


async def test_charger_indicateurs_enchaine_fetch_normalize_et_load(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_get_bytes(_client: httpx.AsyncClient, _url: str) -> bytes:
        return b"peu-importe"

    monkeypatch.setattr(run, "_get_bytes", _fake_get_bytes)
    monkeypatch.setattr(normalize, "normalize_pib_csv", lambda content: {2024: 2_800_000_000_000.0})
    monkeypatch.setattr(
        normalize, "normalize_pib_complement_insee_premiere", lambda content, a: 2_900_000_000_000.0
    )
    monkeypatch.setattr(normalize, "normalize_population_xlsx", lambda content: {2024: 68_000_000})

    async with httpx.AsyncClient() as client:
        await run._charger_indicateurs(db_session, client)
    await db_session.commit()

    indicateur = (
        await db_session.execute(select(IndicateurMacro).where(IndicateurMacro.annee == 2024))
    ).scalar_one()
    assert indicateur.population == 68_000_000


# ---------------------------------------------------------------------------
# run_etl: orchestration de haut niveau
# ---------------------------------------------------------------------------


def _patch_chargeurs(monkeypatch: pytest.MonkeyPatch, appels: list[str]) -> None:
    async def _fake_charger_depenses(db, client, annees):
        appels.append("depenses")
        return {a: "https://source.test/depenses" for a in annees}

    async def _fake_charger_recettes(db, client, annees):
        appels.append("recettes")
        return {}

    async def _fake_charger_recettes_ccomptes(db, client, annees):
        appels.append("recettes_ccomptes")
        return {}

    async def _fake_charger_indicateurs(db, client):
        appels.append("indicateurs")

    monkeypatch.setattr(run, "_charger_depenses", _fake_charger_depenses)
    monkeypatch.setattr(run, "_charger_recettes", _fake_charger_recettes)
    monkeypatch.setattr(run, "_charger_recettes_cour_des_comptes", _fake_charger_recettes_ccomptes)
    monkeypatch.setattr(run, "_charger_indicateurs", _fake_charger_indicateurs)


async def test_run_etl_appelle_toutes_les_etapes_demandees(
    monkeypatch: pytest.MonkeyPatch, db_engine: AsyncEngine
) -> None:
    # `db_engine` non utilise directement : `run_etl` appelle desormais reellement
    # `loader.enregistrer_ingestion_terminee` (via le moteur applicatif global, pas
    # ce fixture) juste avant son commit - la fixture cree la table `ingestion_log`
    # (comme le reste du schema) au moment ou ce test en a besoin, et la nettoie
    # ensuite (cf. sa docstring dans conftest.py).
    appels: list[str] = []
    _patch_chargeurs(monkeypatch, appels)
    recalculs: list[int] = []

    async def _fake_recalculer(db, annee, source_url, psr=0.0):
        recalculs.append(annee)
        return None

    monkeypatch.setattr(run.loader, "recalculer_annee_budget", _fake_recalculer)

    # 2025 : couvert par les depenses ET par les recettes OpenDataSoft
    # (`RECETTES_ANNEES`), pas par la Cour des comptes.
    annee = 2025
    assert annee in sources.RECETTES_ANNEES
    assert annee not in sources.RECETTES_COUR_DES_COMPTES_ANNEES
    await run.run_etl([annee], depenses=True, recettes=True, indicateurs=True)

    assert appels == ["depenses", "recettes", "indicateurs"]
    assert recalculs == [annee]


async def test_run_etl_route_les_recettes_cour_des_comptes(
    monkeypatch: pytest.MonkeyPatch, db_engine: AsyncEngine
) -> None:
    # `db_engine` : voir le commentaire de test_run_etl_appelle_toutes_les_etapes_demandees.
    appels: list[str] = []
    _patch_chargeurs(monkeypatch, appels)
    monkeypatch.setattr(run.loader, "recalculer_annee_budget", lambda *a, **k: _none_coro())

    # 2019 : couvert par les depenses ET par la Cour des comptes
    # (`RECETTES_COUR_DES_COMPTES_ANNEES`), pas par OpenDataSoft.
    annee = 2019
    assert annee in sources.RECETTES_COUR_DES_COMPTES_ANNEES
    assert annee not in sources.RECETTES_ANNEES
    await run.run_etl([annee], depenses=True, recettes=True, indicateurs=False)

    assert appels == ["depenses", "recettes_ccomptes"]


async def test_run_etl_indicateurs_only_ne_touche_pas_annee_budget(
    monkeypatch: pytest.MonkeyPatch, db_engine: AsyncEngine
) -> None:
    # `db_engine` : voir le commentaire de test_run_etl_appelle_toutes_les_etapes_demandees.
    appels: list[str] = []
    _patch_chargeurs(monkeypatch, appels)

    async def _fake_recalculer(*_a, **_k):
        pytest.fail("recalculer_annee_budget ne doit pas etre appele en mode --indicateurs-only")

    monkeypatch.setattr(run.loader, "recalculer_annee_budget", _fake_recalculer)

    await run.run_etl([2024], depenses=False, recettes=False, indicateurs=True)

    assert appels == ["indicateurs"]


async def test_run_etl_annee_hors_perimetre_logge_un_avertissement(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, db_engine: AsyncEngine
) -> None:
    # `db_engine` : voir le commentaire de test_run_etl_appelle_toutes_les_etapes_demandees.
    appels: list[str] = []
    _patch_chargeurs(monkeypatch, appels)
    monkeypatch.setattr(run.loader, "recalculer_annee_budget", lambda *a, **k: _none_coro())

    with caplog.at_level("WARNING"):
        await run.run_etl([2015], depenses=True, recettes=True, indicateurs=False)

    assert "hors perimetre" in caplog.text


async def _none_coro() -> None:
    return None


async def test_run_etl_annule_la_transaction_si_une_etape_echoue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_charger_depenses(db, client, annees):
        raise RuntimeError("echec simule")

    monkeypatch.setattr(run, "_charger_depenses", _fake_charger_depenses)

    with pytest.raises(RuntimeError, match="echec simule"):
        await run.run_etl([2024], depenses=True, recettes=False, indicateurs=False)


# ---------------------------------------------------------------------------
# main(): parsing CLI
# ---------------------------------------------------------------------------


def test_main_annees_par_defaut_et_toutes_etapes_actives(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_etl(annees, *, depenses, recettes, indicateurs=True):
        captured["annees"] = list(annees)
        captured["depenses"] = depenses
        captured["recettes"] = recettes
        captured["indicateurs"] = indicateurs

    monkeypatch.setattr(run, "run_etl", _fake_run_etl)

    run.main([])

    assert captured["annees"] == run._parse_annees("2012-2025")
    assert captured == {
        "annees": run._parse_annees("2012-2025"),
        "depenses": True,
        "recettes": True,
        "indicateurs": True,
    }


def test_main_depenses_only(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_etl(annees, *, depenses, recettes, indicateurs=True):
        captured["depenses"] = depenses
        captured["recettes"] = recettes
        captured["indicateurs"] = indicateurs

    monkeypatch.setattr(run, "run_etl", _fake_run_etl)

    run.main(["--depenses-only", "--annees", "2024"])

    assert captured == {"depenses": True, "recettes": False, "indicateurs": False}


def test_main_indicateurs_only(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_etl(annees, *, depenses, recettes, indicateurs=True):
        captured["depenses"] = depenses
        captured["recettes"] = recettes
        captured["indicateurs"] = indicateurs

    monkeypatch.setattr(run, "run_etl", _fake_run_etl)

    run.main(["--indicateurs-only"])

    assert captured == {"depenses": False, "recettes": False, "indicateurs": True}
