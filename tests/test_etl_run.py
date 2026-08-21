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
import json
import zipfile
from datetime import date
from io import BytesIO
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from api.db.session import engine as _prod_engine
from api.etl import loader, normalize, run, sources
from api.etl.normalize import DepenseFiscaleRecord, MarcheRecord
from api.models.depense_fiscale import DepenseFiscale, StatutMontant
from api.models.indicateur_macro import IndicateurMacro
from api.models.marche_public import MarchePublic
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
# API Legifrance (PISTE): _get_piste_token / _fetch_lfi_jorf / _extract_etats_html
# ---------------------------------------------------------------------------


class _FakeSettings:
    piste_oauth_url = "https://oauth.test/token"
    piste_client_id = "id-test"
    piste_client_secret = "secret-test"
    piste_api_base_url = "https://api.test/legifrance"


async def test_get_piste_token_reussit_du_premier_coup(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(run, "get_settings", lambda: _FakeSettings())

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == _FakeSettings.piste_oauth_url
        return httpx.Response(200, json={"access_token": "tok-123"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        token = await run._get_piste_token(client)

    assert token == "tok-123"


async def test_get_piste_token_reessaie_puis_reussit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    monkeypatch.setattr(run, "get_settings", lambda: _FakeSettings())
    appels = {"n": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        appels["n"] += 1
        if appels["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, json={"access_token": "tok-abc"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        token = await run._get_piste_token(client)

    assert token == "tok-abc"
    assert appels["n"] == 3


async def test_get_piste_token_echoue_apres_le_max_de_tentatives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(asyncio, "sleep", _no_sleep)
    monkeypatch.setattr(run, "get_settings", lambda: _FakeSettings())

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(401)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await run._get_piste_token(client)


async def test_fetch_lfi_jorf_met_en_cache_par_text_cid(monkeypatch: pytest.MonkeyPatch) -> None:
    """Un meme textCid demande deux fois (ex depenses puis recettes du meme
    run) ne doit declencher qu'un seul aller-retour reseau (token + jorf).
    """
    run._lfi_jorf_cache.clear()
    monkeypatch.setattr(run, "get_settings", lambda: _FakeSettings())

    appels_token = {"n": 0}

    async def _fake_get_piste_token(_client: httpx.AsyncClient) -> str:
        appels_token["n"] += 1
        return "tok"

    monkeypatch.setattr(run, "_get_piste_token", _fake_get_piste_token)

    appels_jorf = {"n": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        appels_jorf["n"] += 1
        return httpx.Response(200, json={"title": "ok"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        data1 = await run._fetch_lfi_jorf(client, "JORFTEXT000TEST")
        data2 = await run._fetch_lfi_jorf(client, "JORFTEXT000TEST")

    assert data1 == {"title": "ok"}
    assert data2 == {"title": "ok"}
    assert appels_jorf["n"] == 1
    assert appels_token["n"] == 1


def _load_jorf_etats_annexes_sample() -> dict:
    path = Path(__file__).parent / "fixtures" / "lfi2026_jorf_etats_annexes_sample.json"
    data: dict = json.loads(path.read_text(encoding="utf-8"))
    return data


def test_extract_etats_html_isole_la_premiere_table_de_chaque_etat() -> None:
    """La fixture imite la structure reelle: l'article marque "ETATS
    LEGISLATIFS ANNEXES" est niche a 2 niveaux de `sections` (le parcours
    recursif doit le trouver), et Etat A y contient 2 tables concatenees
    (Budget general + Budgets annexes) - seule la 1ere doit etre retenue.
    """
    jorf = _load_jorf_etats_annexes_sample()
    etat_a, etat_b = run._extract_etats_html(jorf)

    assert etat_a.count("<table") == 1
    assert "1101" in etat_a
    assert "Contrôle et exploitation aériens" not in etat_a

    assert etat_b.count("<table") == 1
    assert "Action extérieure de l'Etat" in etat_b


def test_extract_etats_html_leve_si_marqueur_absent() -> None:
    with pytest.raises(ValueError, match="etats legislatifs annexes"):
        run._extract_etats_html({"articles": [], "sections": []})


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


async def test_fetch_depenses_annee_2026_cas_legifrance(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = [object()]
    text_cid = sources.LFI_TEXT_CID_PAR_ANNEE[2026]

    async def _fake_fetch_lfi_jorf(_client: httpx.AsyncClient, cid: str) -> dict:
        assert cid == text_cid
        return {"fake": "jorf"}

    monkeypatch.setattr(run, "_fetch_lfi_jorf", _fake_fetch_lfi_jorf)
    monkeypatch.setattr(run, "_extract_etats_html", lambda jorf: ("etat-a", "etat-b"))
    monkeypatch.setattr(normalize, "normalize_depenses_legifrance", lambda etat_b, a: sentinel)

    async with httpx.AsyncClient() as client:
        records, source_url = await run._fetch_depenses_annee(client, 2026)

    assert records is sentinel
    assert source_url == sources.legifrance_url(text_cid)


async def test_fetch_depenses_annee_2015_cas_legifrance(monkeypatch: pytest.MonkeyPatch) -> None:
    """2015 comble un trou reel du backfill historique via la meme source
    Legifrance que 2026 (meme branche de dispatch, meme normalizer)."""
    sentinel = [object()]
    text_cid = sources.LFI_TEXT_CID_PAR_ANNEE[2015]

    async def _fake_fetch_lfi_jorf(_client: httpx.AsyncClient, cid: str) -> dict:
        assert cid == text_cid
        return {"fake": "jorf"}

    monkeypatch.setattr(run, "_fetch_lfi_jorf", _fake_fetch_lfi_jorf)
    monkeypatch.setattr(run, "_extract_etats_html", lambda jorf: ("etat-a", "etat-b"))
    monkeypatch.setattr(normalize, "normalize_depenses_legifrance", lambda etat_b, a: sentinel)

    async with httpx.AsyncClient() as client:
        records, source_url = await run._fetch_depenses_annee(client, 2015)

    assert records is sentinel
    assert source_url == sources.legifrance_url(text_cid)


async def test_fetch_depenses_annee_2021_reste_route_vers_l_attachment_existant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """2021 est dans LFI_TEXT_CID_PAR_ANNEE (pour les recettes) mais ses
    depenses doivent rester routees vers la source attachment CSV existante
    (branche `elif annee in (2021, 2022):`, placee AVANT la branche
    Legifrance dans le dispatch) - jamais vers Legifrance, qui ne serait
    meme pas appelee ici (verifie en ne bouchonnant PAS `_fetch_lfi_jorf`:
    un appel intempestif ferait echouer ce test avec une vraie tentative
    reseau).
    """
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
        records, source_url = await run._fetch_depenses_annee(client, 2021)

    assert records is sentinel
    dataset_id = sources.DEPENSES_DATASETS_ATTACHMENTS[2021]
    attachment_id = sources.DEPENSES_ATTACHMENT_IDS[2021]["detaillee"]
    assert source_url == sources.attachment_url(dataset_id, attachment_id)


async def test_fetch_depenses_annee_leve_pour_une_annee_hors_perimetre() -> None:
    # 2008: trou reel documente (depenses 2006-2010) - 2015 est desormais
    # couvert (source Legifrance), plus un exemple valide de trou.
    async with httpx.AsyncClient() as client:
        with pytest.raises(ValueError, match="2008"):
            await run._fetch_depenses_annee(client, 2008)


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


async def test_charger_recettes_legifrance_enchaine_fetch_normalize_et_load(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    annee = 2026
    recette_records = [normalize.RecetteRecord(annee=annee, type=TypeRecette.TICPE, montant=100.0)]
    psr = normalize.PrelevementsSurRecettes(annee=annee, collectivites=5.0, union_europeenne=2.0)

    async def _fake_fetch_lfi_jorf(_client: httpx.AsyncClient, _text_cid: str) -> dict:
        return {"fake": "jorf"}

    monkeypatch.setattr(run, "_fetch_lfi_jorf", _fake_fetch_lfi_jorf)
    monkeypatch.setattr(run, "_extract_etats_html", lambda jorf: ("etat-a", "etat-b"))
    monkeypatch.setattr(
        normalize,
        "normalize_recettes_legifrance",
        lambda etat_a, a, **_kwargs: recette_records,
    )
    monkeypatch.setattr(
        normalize,
        "extract_prelevements_sur_recettes_legifrance",
        lambda etat_a, a, **_kwargs: psr,
    )

    async with httpx.AsyncClient() as client:
        psr_par_annee, remboursements = await run._charger_recettes_legifrance(
            db_session, client, [annee]
        )
    await db_session.commit()

    assert psr_par_annee == {annee: 7.0}
    # Aucun rattrapage pour cette annee (pas de mission RD seedee).
    assert remboursements == {}
    recette = (
        await db_session.execute(
            select(Recette).where(Recette.annee == annee, Recette.type == TypeRecette.TICPE)
        )
    ).scalar_one()
    assert recette.montant_net == 100.0


async def test_charger_recettes_legifrance_calcule_unite_milliers_par_annee(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`unite_milliers` (cf. `api.etl.sources.LFI_ETAT_A_MILLIERS_EUROS`) doit
    valoir True pour 2015 et False pour toute autre annee de cette source -
    verifie ici le CABLAGE (quel argument est effectivement transmis aux 2
    fonctions de normalisation), pas la conversion elle-meme (deja testee
    dans test_etl_normalize.py).
    """
    appels_unite: dict[int, list[bool]] = {}

    async def _fake_fetch_lfi_jorf(_client: httpx.AsyncClient, _text_cid: str) -> dict:
        return {"fake": "jorf"}

    def _fake_normalize(etat_a: str, annee: int, *, unite_milliers: bool = False) -> list:
        appels_unite.setdefault(annee, []).append(unite_milliers)
        return []

    def _fake_extract_psr(
        etat_a: str, annee: int, *, unite_milliers: bool = False
    ) -> normalize.PrelevementsSurRecettes:
        appels_unite.setdefault(annee, []).append(unite_milliers)
        return normalize.PrelevementsSurRecettes(
            annee=annee, collectivites=0.0, union_europeenne=0.0
        )

    monkeypatch.setattr(run, "_fetch_lfi_jorf", _fake_fetch_lfi_jorf)
    monkeypatch.setattr(run, "_extract_etats_html", lambda jorf: ("etat-a", "etat-b"))
    monkeypatch.setattr(normalize, "normalize_recettes_legifrance", _fake_normalize)
    monkeypatch.setattr(
        normalize, "extract_prelevements_sur_recettes_legifrance", _fake_extract_psr
    )

    async with httpx.AsyncClient() as client:
        await run._charger_recettes_legifrance(db_session, client, [2015, 2021, 2026])
    await db_session.commit()

    assert appels_unite[2015] == [True, True]
    assert appels_unite[2021] == [False, False]
    assert appels_unite[2026] == [False, False]


async def test_charger_recettes_legifrance_regrossit_avec_remboursements_et_degrevements(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bug reel trouve et corrige a l'execution du run complet sur la LFI 2026:
    sans ce rattrapage, le deficit calcule ressortait a ~274,7 Md EUR au
    lieu des ~133,5 Md EUR officiels (tableau d'equilibre, article 147).
    Un premier correctif (regrossir toute la mission, comme pour `_charger_
    recettes_cour_des_comptes`) etait encore FAUX de ~4,4 Md EUR: seul le
    programme "impots d'Etat" doit etre regrossi, pas "impots locaux" (cf.
    docstring de `loader.get_remboursements_degrevements_impots_etat_cp`) -
    ce test seede donc les 2 programmes et verifie que seul le premier
    contribue.
    """
    annee = 2026
    mapping = await loader.upsert_missions(
        db_session,
        [
            normalize.MissionYearRow(
                slug="remboursements-et-degrevements",
                nom_normalise="remboursements et degrevements",
                nom_officiel="Remboursements et degrevements",
                annee=annee,
                code_mission=None,
            )
        ],
    )
    await db_session.commit()
    mission_id = mapping[("remboursements-et-degrevements", annee)]
    rd_etat = normalize.DepenseAggregat(
        annee=annee,
        mission_code="",
        mission_libelle="Remboursements et degrevements",
        programme_code="hash-etat",
        programme_libelle="Remboursements et dégrèvements d'impôts d'Etat",
        action_code="hash-etat",
        action_libelle="Remboursements et dégrèvements d'impôts d'Etat",
        ae=141174362742.0,
        cp=141174362742.0,
    )
    rd_locaux = normalize.DepenseAggregat(
        annee=annee,
        mission_code="",
        mission_libelle="Remboursements et degrevements",
        programme_code="hash-locaux",
        programme_libelle="Remboursements et dégrèvements d'impôts locaux",
        action_code="hash-locaux",
        action_libelle="Remboursements et dégrèvements d'impôts locaux",
        ae=4426000000.0,
        cp=4426000000.0,
    )
    await loader.upsert_depenses(
        db_session, annee, [(rd_etat, mission_id), (rd_locaux, mission_id)]
    )
    await db_session.commit()

    recette_records = [normalize.RecetteRecord(annee=annee, type=TypeRecette.IR, montant=100.0)]
    psr = normalize.PrelevementsSurRecettes(annee=annee, collectivites=0.0, union_europeenne=0.0)

    async def _fake_fetch_lfi_jorf(_client: httpx.AsyncClient, _text_cid: str) -> dict:
        return {"fake": "jorf"}

    monkeypatch.setattr(run, "_fetch_lfi_jorf", _fake_fetch_lfi_jorf)
    monkeypatch.setattr(run, "_extract_etats_html", lambda jorf: ("etat-a", "etat-b"))
    monkeypatch.setattr(
        normalize,
        "normalize_recettes_legifrance",
        lambda etat_a, a, **_kwargs: recette_records,
    )
    monkeypatch.setattr(
        normalize,
        "extract_prelevements_sur_recettes_legifrance",
        lambda etat_a, a, **_kwargs: psr,
    )

    async with httpx.AsyncClient() as client:
        _psr, remboursements = await run._charger_recettes_legifrance(db_session, client, [annee])
    await db_session.commit()

    # Le rattrapage est RETOURNE pour etre ajoute au total de l'annee...
    assert remboursements[annee] == pytest.approx(141174362742.0)
    # ...et n'entre PAS dans la table `recette`: ce n'est pas une recette, et
    # l'y ranger faussait le type AUTRES, publie tel quel dans le camembert du
    # tableau de bord (141 Md EUR sur 247, soit pres de la moitie).
    autres = (
        await db_session.execute(
            select(Recette).where(Recette.annee == annee, Recette.type == TypeRecette.AUTRES)
        )
    ).scalar_one_or_none()
    assert autres is None


async def test_charger_recettes_legifrance_2015_n_applique_aucun_rattrapage(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A la difference de 2026, la LFI 2015 (article 49) n'a besoin d'AUCUN
    rattrapage brut/net cote recettes: la somme brute des lignes d'Etat A
    (hors PSR) correspond deja exactement a la ligne "recettes brutes" du
    tableau d'equilibre officiel, comparee a des depenses elles-memes
    brutes - la mission "Remboursements et degrevements" s'annule
    mathematiquement des 2 cotes sans intervention. 2 bugs reels trouves
    en verifiant ce tableau plutot que de supposer une convention deja
    verifiee ailleurs: appliquer le rattrapage "impots d'Etat seul" (2026)
    donnait un deficit de 43,0 Md EUR au lieu du solde officiel -172,4 Md
    EUR (pour 2021); appliquer le rattrapage "mission entiere" (Cour des
    comptes) donnait -13,6 Md EUR (un surplus implausible) au lieu de
    -74,2 Md EUR officiels (pour 2015, teste ici) - dans les 2 cas, seule
    l'ABSENCE totale de rattrapage etait correcte. Une mission "RD" est
    seedee ici avec un CP non nul pour bien verifier qu'elle est ignoree,
    pas juste absente.
    """
    annee = 2015
    assert annee not in sources.LFI_REMBOURSEMENTS_IMPOTS_ETAT_SEUL
    mapping = await loader.upsert_missions(
        db_session,
        [
            normalize.MissionYearRow(
                slug="remboursements-et-degrevements",
                nom_normalise="remboursements et degrevements",
                nom_officiel="Remboursements et degrevements",
                annee=annee,
                code_mission=None,
            )
        ],
    )
    await db_session.commit()
    mission_id = mapping[("remboursements-et-degrevements", annee)]
    rd_etat = normalize.DepenseAggregat(
        annee=annee,
        mission_code="",
        mission_libelle="Remboursements et degrevements",
        programme_code="hash-etat",
        programme_libelle="Remboursements et dégrèvements d'impôts d'Etat",
        action_code="hash-etat",
        action_libelle="Remboursements et dégrèvements d'impôts d'Etat",
        ae=87800000000.0,
        cp=87800000000.0,
    )
    await loader.upsert_depenses(db_session, annee, [(rd_etat, mission_id)])
    await db_session.commit()

    recette_records = [normalize.RecetteRecord(annee=annee, type=TypeRecette.IR, montant=100.0)]
    psr = normalize.PrelevementsSurRecettes(annee=annee, collectivites=0.0, union_europeenne=0.0)

    async def _fake_fetch_lfi_jorf(_client: httpx.AsyncClient, _text_cid: str) -> dict:
        return {"fake": "jorf"}

    monkeypatch.setattr(run, "_fetch_lfi_jorf", _fake_fetch_lfi_jorf)
    monkeypatch.setattr(run, "_extract_etats_html", lambda jorf: ("etat-a", "etat-b"))
    monkeypatch.setattr(
        normalize,
        "normalize_recettes_legifrance",
        lambda etat_a, a, **_kwargs: recette_records,
    )
    monkeypatch.setattr(
        normalize,
        "extract_prelevements_sur_recettes_legifrance",
        lambda etat_a, a, **_kwargs: psr,
    )

    async with httpx.AsyncClient() as client:
        await run._charger_recettes_legifrance(db_session, client, [annee])
    await db_session.commit()

    # Aucune ligne AUTRES ajoutee: seul le RecetteRecord IR bouchonne est present.
    autres = (
        await db_session.execute(
            select(Recette).where(Recette.annee == annee, Recette.type == TypeRecette.AUTRES)
        )
    ).scalar_one_or_none()
    assert autres is None


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


async def test_charger_depenses_fiscales_enchaine_fetch_normalize_et_load(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_get_bytes(_client: httpx.AsyncClient, _url: str) -> bytes:
        return b"peu-importe"

    record = DepenseFiscaleRecord(
        annee=sources.DEPENSE_FISCALE_ANNEE,
        numero="1",
        categorie="Impôt sur le revenu",
        sous_categorie="Sous-categorie",
        sous_sous_categorie=None,
        libelle="Libelle",
        beneficiaire="Menages",
        montant_millions=12.0,
        statut_montant=StatutMontant.CHIFFRE,
        methode_chiffrage=None,
    )
    monkeypatch.setattr(run, "_get_bytes", _fake_get_bytes)
    monkeypatch.setattr(normalize, "normalize_depenses_fiscales_xlsx", lambda content, a: [record])

    async with httpx.AsyncClient() as client:
        await run._charger_depenses_fiscales(db_session, client)
    await db_session.commit()

    lignes = (await db_session.execute(select(DepenseFiscale))).scalars().all()
    assert len(lignes) == 1
    assert lignes[0].annee == sources.DEPENSE_FISCALE_ANNEE
    assert lignes[0].numero == "1"


async def test_charger_marches_enchaine_fetch_normalize_et_load(
    db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_get_bytes(_client: httpx.AsyncClient, _url: str) -> bytes:
        return b"peu-importe"

    record = MarcheRecord(
        marche_id_source="1",
        nature="Marché",
        objet="Objet",
        objet_recherche="objet",
        codecpv="45000000-7",
        codecpv_division="45",
        procedure="Procédure adaptée",
        acheteur_siret="12345678900011",
        titulaire_siret="98765432100022",
        titulaire_id_type="SIRET",
        dureemois=12,
        datenotification=date(2024, 1, 1),
        datepublicationdonnees=None,
        montant=1000.0,
        formeprix=None,
        offresrecues=None,
        marcheinnovant=None,
    )
    monkeypatch.setattr(run, "_get_bytes", _fake_get_bytes)
    monkeypatch.setattr(normalize, "normalize_marches_parquet", lambda content: iter([[record]]))

    async with httpx.AsyncClient() as client:
        await run._charger_marches(db_session, client)
    await db_session.commit()

    lignes = (await db_session.execute(select(MarchePublic))).scalars().all()
    assert len(lignes) == 1
    assert lignes[0].marche_id_source == "1"


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

    async def _fake_charger_recettes_legifrance(db, client, annees):
        appels.append("recettes_legifrance")
        # Deux mappings desormais: PSR (a retrancher) et rattrapage (a ajouter).
        return {}, {}

    async def _fake_charger_indicateurs(db, client):
        appels.append("indicateurs")

    async def _fake_charger_depenses_fiscales(db, client):
        appels.append("depenses_fiscales")

    async def _fake_charger_marches(db, client):
        appels.append("marches")

    monkeypatch.setattr(run, "_charger_depenses", _fake_charger_depenses)
    monkeypatch.setattr(run, "_charger_recettes", _fake_charger_recettes)
    monkeypatch.setattr(run, "_charger_recettes_cour_des_comptes", _fake_charger_recettes_ccomptes)
    monkeypatch.setattr(run, "_charger_recettes_legifrance", _fake_charger_recettes_legifrance)
    monkeypatch.setattr(run, "_charger_depenses_fiscales", _fake_charger_depenses_fiscales)
    monkeypatch.setattr(run, "_charger_indicateurs", _fake_charger_indicateurs)
    monkeypatch.setattr(run, "_charger_marches", _fake_charger_marches)


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

    async def _fake_recalculer(db, annee, source_url, psr=0.0, remboursements=0.0):
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


async def test_run_etl_route_les_recettes_legifrance(
    monkeypatch: pytest.MonkeyPatch, db_engine: AsyncEngine
) -> None:
    # `db_engine` : voir le commentaire de test_run_etl_appelle_toutes_les_etapes_demandees.
    appels: list[str] = []
    _patch_chargeurs(monkeypatch, appels)
    monkeypatch.setattr(run.loader, "recalculer_annee_budget", lambda *a, **k: _none_coro())

    # 2026 : couvert par les depenses ET par Legifrance/PISTE
    # (`RECETTES_LEGIFRANCE_ANNEES`), pas par OpenDataSoft ni la Cour des comptes.
    annee = 2026
    assert annee in sources.RECETTES_LEGIFRANCE_ANNEES
    assert annee not in sources.RECETTES_ANNEES
    assert annee not in sources.RECETTES_COUR_DES_COMPTES_ANNEES
    await run.run_etl([annee], depenses=True, recettes=True, indicateurs=False)

    assert appels == ["depenses", "recettes_legifrance"]


async def test_run_etl_depenses_fiscales_only_ne_touche_pas_annee_budget(
    monkeypatch: pytest.MonkeyPatch, db_engine: AsyncEngine
) -> None:
    # `db_engine` : voir le commentaire de test_run_etl_appelle_toutes_les_etapes_demandees.
    appels: list[str] = []
    _patch_chargeurs(monkeypatch, appels)

    async def _fake_recalculer(*_a, **_k):
        pytest.fail(
            "recalculer_annee_budget ne doit pas etre appele en mode --depenses-fiscales-only"
        )

    monkeypatch.setattr(run.loader, "recalculer_annee_budget", _fake_recalculer)

    await run.run_etl(
        [2021], depenses=False, recettes=False, indicateurs=False, depenses_fiscales=True
    )

    assert appels == ["depenses_fiscales"]


async def test_run_etl_marches_only_ne_touche_pas_annee_budget(
    monkeypatch: pytest.MonkeyPatch, db_engine: AsyncEngine
) -> None:
    # `db_engine` : voir le commentaire de test_run_etl_appelle_toutes_les_etapes_demandees.
    appels: list[str] = []
    _patch_chargeurs(monkeypatch, appels)

    async def _fake_recalculer(*_a, **_k):
        pytest.fail("recalculer_annee_budget ne doit pas etre appele en mode --marches-only")

    monkeypatch.setattr(run.loader, "recalculer_annee_budget", _fake_recalculer)

    await run.run_etl([2024], depenses=False, recettes=False, indicateurs=False, marches=True)

    assert appels == ["marches"]


async def test_run_etl_par_defaut_n_appelle_pas_marches(
    monkeypatch: pytest.MonkeyPatch, db_engine: AsyncEngine
) -> None:
    """`marches` reste `False` par defaut sur run_etl(): un run complet de
    routine ne doit jamais retelecharger/recharger les 689 000 lignes sans
    demande explicite (--marches-only)."""
    # `db_engine` : voir le commentaire de test_run_etl_appelle_toutes_les_etapes_demandees.
    appels: list[str] = []
    _patch_chargeurs(monkeypatch, appels)
    monkeypatch.setattr(run.loader, "recalculer_annee_budget", lambda *a, **k: _none_coro())

    await run.run_etl([2025], depenses=True, recettes=True, indicateurs=True)

    assert "marches" not in appels


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
        # 2008: trou reel documente (depenses 2006-2010) - 2015 est
        # desormais couvert (source Legifrance), plus un exemple valide.
        await run.run_etl([2008], depenses=True, recettes=True, indicateurs=False)

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

    async def _fake_run_etl(
        annees, *, depenses, recettes, indicateurs=True, depenses_fiscales=False, marches=False
    ):
        captured["annees"] = list(annees)
        captured["depenses"] = depenses
        captured["recettes"] = recettes
        captured["indicateurs"] = indicateurs
        captured["depenses_fiscales"] = depenses_fiscales
        captured["marches"] = marches

    monkeypatch.setattr(run, "run_etl", _fake_run_etl)

    run.main([])

    assert captured["annees"] == run._parse_annees("2012-2026")
    assert captured == {
        "annees": run._parse_annees("2012-2026"),
        "depenses": True,
        "recettes": True,
        "indicateurs": True,
        "depenses_fiscales": False,
        "marches": False,
    }


def test_main_depenses_only(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_etl(
        annees, *, depenses, recettes, indicateurs=True, depenses_fiscales=False, marches=False
    ):
        captured["depenses"] = depenses
        captured["recettes"] = recettes
        captured["indicateurs"] = indicateurs
        captured["depenses_fiscales"] = depenses_fiscales
        captured["marches"] = marches

    monkeypatch.setattr(run, "run_etl", _fake_run_etl)

    run.main(["--depenses-only", "--annees", "2024"])

    assert captured == {
        "depenses": True,
        "recettes": False,
        "indicateurs": False,
        "depenses_fiscales": False,
        "marches": False,
    }


def test_main_indicateurs_only(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_etl(
        annees, *, depenses, recettes, indicateurs=True, depenses_fiscales=False, marches=False
    ):
        captured["depenses"] = depenses
        captured["recettes"] = recettes
        captured["indicateurs"] = indicateurs
        captured["depenses_fiscales"] = depenses_fiscales
        captured["marches"] = marches

    monkeypatch.setattr(run, "run_etl", _fake_run_etl)

    run.main(["--indicateurs-only"])

    assert captured == {
        "depenses": False,
        "recettes": False,
        "indicateurs": True,
        "depenses_fiscales": False,
        "marches": False,
    }


def test_main_depenses_fiscales_only(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_etl(
        annees, *, depenses, recettes, indicateurs=True, depenses_fiscales=False, marches=False
    ):
        captured["depenses"] = depenses
        captured["recettes"] = recettes
        captured["indicateurs"] = indicateurs
        captured["depenses_fiscales"] = depenses_fiscales
        captured["marches"] = marches

    monkeypatch.setattr(run, "run_etl", _fake_run_etl)

    run.main(["--depenses-fiscales-only"])

    assert captured == {
        "depenses": False,
        "recettes": False,
        "indicateurs": False,
        "depenses_fiscales": True,
        "marches": False,
    }


def test_main_marches_only(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_etl(
        annees, *, depenses, recettes, indicateurs=True, depenses_fiscales=False, marches=False
    ):
        captured["depenses"] = depenses
        captured["recettes"] = recettes
        captured["indicateurs"] = indicateurs
        captured["depenses_fiscales"] = depenses_fiscales
        captured["marches"] = marches

    monkeypatch.setattr(run, "run_etl", _fake_run_etl)

    run.main(["--marches-only"])

    assert captured == {
        "depenses": False,
        "recettes": False,
        "indicateurs": False,
        "depenses_fiscales": False,
        "marches": True,
    }
