"""Tests unitaires du module de normalisation ETL (aucune dependance reseau).

Les fixtures utilisees sont de petits extraits, representatifs des formats
source reels observes sur data.economie.gouv.fr, copies dans
`tests/fixtures/`.
"""

import json
from pathlib import Path

import pytest

from api.etl.normalize import (
    aggregate_depenses,
    aggregate_recettes,
    build_mission_alias_rows,
    build_mission_rows,
    clean_montant,
    normalize_depenses_2020,
    normalize_depenses_attachment_detaillee,
    normalize_depenses_records_json,
    normalize_mission_name,
    normalize_recettes_records_json,
    resolve_mission_identities,
)
from api.models.recette import TypeRecette

FIXTURES = Path(__file__).parent / "fixtures"


def _load_json(name: str) -> list[dict]:
    data = json.loads((FIXTURES / name).read_text(encoding="utf-8"))
    return list(data["results"])


def _load_csv_cp1252(name: str) -> str:
    return (FIXTURES / name).read_bytes().decode("cp1252")


# ---------------------------------------------------------------------------
# clean_montant
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, 0.0),
        ("", 0.0),
        ("   ", 0.0),
        (42, 42.0),
        (42.5, 42.5),
        ("484 226 865", 484226865.0),
        ("484\xa0226\xa0865", 484226865.0),
        ("484 226 865", 484226865.0),
        ("1 234,56", 1234.56),
        ("-1 000 000", -1000000.0),
        ("1234.56", 1234.56),
    ],
)
def test_clean_montant(raw: float | int | str | None, expected: float) -> None:
    assert clean_montant(raw) == pytest.approx(expected)


# ---------------------------------------------------------------------------
# normalize_mission_name
# ---------------------------------------------------------------------------


def test_normalize_mission_name_slugifie_les_accents_et_la_ponctuation() -> None:
    assert normalize_mission_name("Défense", 2025) == "defense"
    assert normalize_mission_name("Conseil et contrôle de l'État", 2019) == (
        "conseil-et-controle-de-l-etat"
    )


# ---------------------------------------------------------------------------
# Depenses: API records JSON (2023-2025)
# ---------------------------------------------------------------------------


def test_normalize_depenses_records_json_2025_filtre_hors_budget_general() -> None:
    raw = _load_json("depenses_2025_sample.json")
    records = normalize_depenses_records_json(raw, 2025)

    # 3 lignes BG dans la fixture, 1 ligne BA exclue
    assert len(records) == 3
    assert all(r.annee == 2025 for r in records)
    codes_mission = {r.mission_code for r in records}
    assert codes_mission == {"TB", "DA"}


def test_normalize_depenses_records_json_2025_aggregation_par_action() -> None:
    raw = _load_json("depenses_2025_sample.json")
    records = normalize_depenses_records_json(raw, 2025)
    aggregats = aggregate_depenses(records)

    # 2 sous-lignes TB/103/103-01 doivent fusionner en une seule action
    par_action = {(a.mission_code, a.programme_code, a.action_code): a for a in aggregats}
    assert len(aggregats) == 2
    action_tb = par_action[("TB", "103", "103-01")]
    assert action_tb.ae == pytest.approx(3500000.0)
    assert action_tb.cp == pytest.approx(466537422.0)
    assert action_tb.mission_libelle.startswith("Travail")


def test_normalize_depenses_records_json_2019_filtre_et_prefixe_action() -> None:
    raw = _load_json("depenses_2019_sample.json")
    records = normalize_depenses_records_json(raw, 2019)

    # 2 lignes "Budget général", 1 ligne "Comptes d'affectation speciale" exclue
    assert len(records) == 2
    assert all(r.mission_code == "CA" for r in records)
    assert all(r.action_code == "165-07" for r in records)

    aggregats = aggregate_depenses(records)
    assert len(aggregats) == 1
    assert aggregats[0].ae == pytest.approx(183844 + 16156)
    assert aggregats[0].cp == pytest.approx(183844 + 16156)


# ---------------------------------------------------------------------------
# Depenses 2020: double fichier a joindre
# ---------------------------------------------------------------------------


def test_normalize_depenses_2020_joint_nomenclature_et_credits() -> None:
    nomenclature = _load_csv_cp1252("lfi2020_nomenclature_sample.csv")
    credits_csv = _load_csv_cp1252("lfi2020_credits_sample.csv")

    records = normalize_depenses_2020(nomenclature, credits_csv, annee=2020)

    # 2 lignes BG dans credits.csv, 1 ligne BA exclue
    assert len(records) == 2
    assert all(r.annee == 2020 for r in records)
    assert all(r.mission_code == "JA" for r in records)
    assert records[0].mission_libelle == "Justice"
    assert records[0].programme_libelle == "Accès au droit et à la justice"
    assert records[0].action_libelle == "Aide juridictionnelle"

    aggregats = aggregate_depenses(records)
    assert len(aggregats) == 1
    assert aggregats[0].ae == pytest.approx(50000 + 484226865)
    assert aggregats[0].cp == pytest.approx(50000 + 484226865)


# ---------------------------------------------------------------------------
# Depenses 2021-2022: fichier "detaillee"
# ---------------------------------------------------------------------------


def test_normalize_depenses_attachment_detaillee_filtre_et_somme_t2_ht2() -> None:
    csv_text = _load_csv_cp1252("lfi2021_detaillee_sample.csv")
    records = normalize_depenses_attachment_detaillee(csv_text, annee=2021)

    # 3 lignes BG (2 Justice + 1 Defense), 1 ligne BA exclue
    assert len(records) == 3
    assert all(r.annee == 2021 for r in records)

    aggregats = aggregate_depenses(records)
    par_cle = {(a.mission_code, a.programme_code, a.action_code): a for a in aggregats}
    assert len(aggregats) == 2

    justice = par_cle[("JA", "101", "101-01")]
    # colonne "AE ( T2 + HT2) LFI" / "CP ( T2 + HT2) LFI": 45000 + 533000000
    assert justice.ae == pytest.approx(45000 + 533000000)
    assert justice.cp == pytest.approx(45000 + 533000000)
    assert justice.action_libelle == "Aide juridictionnelle"

    defense = par_cle[("DA", "146", "146-09")]
    assert defense.ae == pytest.approx(100000.0)


# ---------------------------------------------------------------------------
# Recettes
# ---------------------------------------------------------------------------


def test_normalize_recettes_records_json_mappe_les_codes_connus() -> None:
    raw = _load_json("recettes_2025_sample.json")
    records = normalize_recettes_records_json(raw, 2025)

    par_type: dict[TypeRecette, list[float]] = {}
    for r in records:
        par_type.setdefault(r.type, []).append(r.montant)

    assert par_type[TypeRecette.IR] == [pytest.approx(90000000000.0)]
    assert par_type[TypeRecette.IS] == [pytest.approx(70000000000.0)]
    assert par_type[TypeRecette.TVA] == [pytest.approx(100000000000.0)]
    assert par_type[TypeRecette.TICPE] == [pytest.approx(15000000000.0)]
    # codes 1303 et 3119 (non references) tombent dans AUTRES
    assert len(par_type[TypeRecette.AUTRES]) == 2


def test_aggregate_recettes_somme_par_type_brut_egal_net() -> None:
    raw = _load_json("recettes_2025_sample.json")
    records = normalize_recettes_records_json(raw, 2025)
    aggregats = aggregate_recettes(records)

    par_type = {a.type: a for a in aggregats}
    assert len(aggregats) == 5  # IR, IS, TVA, TICPE, AUTRES
    autres = par_type[TypeRecette.AUTRES]
    assert autres.montant_net == pytest.approx(305000000.0 - 1000000000.0)
    assert autres.montant_brut == autres.montant_net


# ---------------------------------------------------------------------------
# Resolution d'identite des missions
# ---------------------------------------------------------------------------


def test_resolve_mission_identities_stabilise_le_slug_sur_le_libelle_le_plus_recent() -> None:
    raw_2021 = _load_csv_cp1252("lfi2021_detaillee_sample.csv")
    records_2021 = normalize_depenses_attachment_detaillee(raw_2021, 2021)
    aggregats_2021 = aggregate_depenses(records_2021)

    raw_2025 = _load_json("depenses_2025_sample.json")
    records_2025 = normalize_depenses_records_json(raw_2025, 2025)
    aggregats_2025 = aggregate_depenses(records_2025)

    depenses_par_annee = {2021: aggregats_2021, 2025: aggregats_2025}
    identites = resolve_mission_identities(depenses_par_annee)

    # "DA" (Defense/Défense) est present dans les deux annees avec le meme
    # libelle -> une seule identite logique, code_mission conserve.
    identite_da = identites["DA"]
    assert identite_da.code_mission == "DA"
    assert identite_da.slug == "defense"

    mission_rows = build_mission_rows(depenses_par_annee, identites)
    slugs_da = {r.slug for r in mission_rows if r.code_mission == "DA"}
    assert slugs_da == {"defense"}
    annees_da = sorted(r.annee for r in mission_rows if r.code_mission == "DA")
    assert annees_da == [2021, 2025]

    alias_rows = build_mission_alias_rows(depenses_par_annee, identites)
    alias_da = next(a for a in alias_rows if a.slug == "defense")
    assert alias_da.annee_debut == 2021
    assert alias_da.annee_fin == 2025


def test_resolve_mission_identities_libelle_change_garde_un_slug_stable() -> None:
    """Simule un changement d'intitule d'une annee sur l'autre pour un meme code."""
    from api.etl.normalize import DepenseAggregat

    ancien = DepenseAggregat(
        annee=2020,
        mission_code="JA",
        mission_libelle="Justice",
        programme_code="101",
        programme_libelle="Programme",
        action_code="101-01",
        action_libelle="Action",
        ae=1.0,
        cp=1.0,
    )
    nouveau = DepenseAggregat(
        annee=2024,
        mission_code="JA",
        mission_libelle="Justice et libertés",
        programme_code="101",
        programme_libelle="Programme",
        action_code="101-01",
        action_libelle="Action",
        ae=1.0,
        cp=1.0,
    )
    depenses_par_annee = {2020: [ancien], 2024: [nouveau]}
    identites = resolve_mission_identities(depenses_par_annee)

    identite = identites["JA"]
    # Le libelle canonique retenu est celui de l'annee la plus recente (2024)
    assert identite.nom_normalise == "Justice et libertés"
    assert identite.slug == "justice-et-libertes"

    mission_rows = build_mission_rows(depenses_par_annee, identites)
    # Le slug reste le meme pour les deux annees malgre le changement de libelle
    slugs = {r.slug for r in mission_rows}
    assert slugs == {"justice-et-libertes"}
    nom_officiel_2020 = next(r.nom_officiel for r in mission_rows if r.annee == 2020)
    assert nom_officiel_2020 == "Justice"

    alias_rows = build_mission_alias_rows(depenses_par_annee, identites)
    alias_par_libelle = {a.nom_csv: a for a in alias_rows}
    assert alias_par_libelle["Justice"].annee_debut == 2020
    assert alias_par_libelle["Justice"].annee_fin == 2020
    assert alias_par_libelle["Justice et libertés"].annee_debut == 2024
    assert alias_par_libelle["Justice et libertés"].annee_fin == 2024
