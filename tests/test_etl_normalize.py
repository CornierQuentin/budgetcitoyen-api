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
    extract_prelevements_sur_recettes,
    normalize_depenses_2020,
    normalize_depenses_attachment_detaillee,
    normalize_depenses_records_json,
    normalize_mission_name,
    normalize_pib_complement_insee_premiere,
    normalize_pib_csv,
    normalize_population_xlsx,
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


def _load_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


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


def test_normalize_depenses_records_json_2023_ignore_champ_lfi_casse() -> None:
    """Le dataset 2023 suit un schema proche du CSV "detaillee", pas du JSON 2025.

    Ses colonnes calculees `*_lfi_2023` valent une constante 4.0 sur toutes
    les lignes (bug constate cote source): le montant doit etre recalcule a
    partir de PLF + amendements, pas lu depuis ces colonnes cassees.
    """
    raw = _load_json("depenses_2023_sample.json")
    records = normalize_depenses_records_json(raw, 2023)

    # 2 lignes BG (Justice), 1 ligne CCF exclue
    assert len(records) == 2
    assert all(r.mission_code == "JA" for r in records)
    assert all(r.action_code == "166-05" for r in records)
    # Aucune valeur ne doit provenir du champ casse (toujours 4.0 en source)
    assert all(r.ae != 4.0 for r in records)

    aggregats = aggregate_depenses(records)
    assert len(aggregats) == 1
    assert aggregats[0].ae == pytest.approx(1000000.0 + 55000.0)
    assert aggregats[0].cp == pytest.approx(1000000.0 + 55000.0)
    assert aggregats[0].programme_code == "166"


def test_normalize_depenses_records_json_2024_schema_ae_plf() -> None:
    """Le dataset 2024 utilise ae_plf/cp_plf (pas de decomposition T2/HT2)."""
    raw = _load_json("depenses_2024_sample.json")
    records = normalize_depenses_records_json(raw, 2024)

    # 2 lignes BG (Culture), 1 ligne BA exclue
    assert len(records) == 2
    assert all(r.mission_code == "CB" for r in records)
    assert all(r.action_code == "224-07" for r in records)

    aggregats = aggregate_depenses(records)
    assert len(aggregats) == 1
    assert aggregats[0].ae == pytest.approx(76099174.0 + 5000000.0)
    assert aggregats[0].cp == pytest.approx(74172725.0 + 5000000.0)


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
    # code 1303 (non reference) tombe dans AUTRES; le code 3119 est une
    # ligne PSR ("Prelevements sur les recettes...collectivites
    # territoriales") et ne doit PAS apparaitre du tout dans les records.
    assert len(par_type[TypeRecette.AUTRES]) == 1


def test_normalize_recettes_records_json_exclut_les_lignes_psr() -> None:
    """Les lignes PSR (type_de_recettes commencant par "Prelevement") ne sont pas
    des recettes: elles ne doivent jamais atterrir dans `recette` (dont le
    `type` IR/TVA/IS/TICPE/AUTRES n'a pas de sens pour un prelevement
    reverse aux collectivites/UE).
    """
    raw = _load_json("recettes_2025_sample.json")
    records = normalize_recettes_records_json(raw, 2025)

    # 5 lignes "Recettes fiscales" dans la fixture, 1 ligne PSR exclue
    assert len(records) == 5
    assert sum(r.montant for r in records) == pytest.approx(
        90000000000.0 + 70000000000.0 + 100000000000.0 + 15000000000.0 + 305000000.0
    )


def test_aggregate_recettes_somme_par_type_brut_egal_net() -> None:
    raw = _load_json("recettes_2025_sample.json")
    records = normalize_recettes_records_json(raw, 2025)
    aggregats = aggregate_recettes(records)

    par_type = {a.type: a for a in aggregats}
    assert len(aggregats) == 5  # IR, IS, TVA, TICPE, AUTRES
    autres = par_type[TypeRecette.AUTRES]
    assert autres.montant_net == pytest.approx(305000000.0)
    assert autres.montant_brut == autres.montant_net


# ---------------------------------------------------------------------------
# Recettes: prelevements sur recettes (PSR) - methodologie tableau d'equilibre
# ---------------------------------------------------------------------------


def test_extract_prelevements_sur_recettes_isole_collectivites_et_ue() -> None:
    """Chiffres proches de l'exemple 2025 reel (recettes-du-budget-general PLF25):
    fiscales ~500,35 Md, non fiscales ~20,55 Md, PSR collectivites ~44,19 Md,
    PSR UE ~23,32 Md -> recettes_nettes ~453,4 Md.
    """
    raw = _load_json("recettes_2025_psr_sample.json")
    psr = extract_prelevements_sur_recettes(raw, 2025)

    assert psr.collectivites == pytest.approx(44188897951.0)
    assert psr.union_europeenne == pytest.approx(23320855052.0)
    assert psr.total == pytest.approx(44188897951.0 + 23320855052.0)


def test_normalize_recettes_records_json_psr_sample_exclut_bien_les_psr() -> None:
    raw = _load_json("recettes_2025_psr_sample.json")
    records = normalize_recettes_records_json(raw, 2025)

    # Seules les 3 lignes "Recettes fiscales"/"Recettes non fiscales" restent
    # (les 2 lignes PSR - collectivites et UE - sont exclues).
    assert len(records) == 3
    recettes_budgetaires = sum(r.montant for r in records)
    assert recettes_budgetaires == pytest.approx(500349453469.0 + 20548548212.0)

    psr = extract_prelevements_sur_recettes(raw, 2025)
    recettes_nettes = recettes_budgetaires - psr.total
    # ~453,4 Md EUR, coherent avec les ordres de grandeur officiels du
    # deficit budgetaire 2025 une fois compare aux depenses (~594 Md).
    assert recettes_nettes == pytest.approx(453388248678.0)
    assert recettes_nettes / 1e9 == pytest.approx(453.4, abs=0.1)


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


# ---------------------------------------------------------------------------
# Indicateurs macro: PIB nominal et population
# ---------------------------------------------------------------------------


def test_normalize_pib_csv_convertit_millions_en_euros() -> None:
    raw = _load_bytes("pib_courant_sample.csv")
    pib = normalize_pib_csv(raw)

    assert pib == {
        2022: pytest.approx(2_639_092_000_000.0),
        2021: pytest.approx(2_502_118_000_000.0),
        2020: pytest.approx(2_317_832_000_000.0),
        1950: pytest.approx(15_513_000_000.0),
    }


def test_normalize_pib_csv_accepte_une_chaine_deja_decodee() -> None:
    texte = "annee,pib\n2022,2639092\n"
    pib = normalize_pib_csv(texte)

    assert pib == {2022: pytest.approx(2_639_092_000_000.0)}


def test_normalize_pib_complement_insee_premiere_extrait_le_niveau_nominal() -> None:
    """Utilise une feuille "Figure 1" minimale, reproduisant la structure
    reelle observee sur l'edition Insee Premiere 2024 ("Les comptes de la
    Nation en 2024", IP2053): colonne A=libelle, B/C/D=evolutions en volume
    (annees precedentes), E="En milliards d'euros" (niveau nominal de
    l'annee courante) - PAS les colonnes d'evolution en volume qui la
    precedent.
    """
    raw = _load_bytes("pib_complement_2024_sample.xlsx")
    pib = normalize_pib_complement_insee_premiere(raw, 2024)

    assert pib == pytest.approx(2_919_900_000_000.0)


def test_normalize_pib_complement_insee_premiere_leve_si_annee_ne_correspond_pas() -> None:
    raw = _load_bytes("pib_complement_2024_sample.xlsx")

    with pytest.raises(ValueError):
        normalize_pib_complement_insee_premiere(raw, 2025)


def test_normalize_population_xlsx_lit_l_onglet_fr() -> None:
    """Utilise une feuille "FR" minimale reproduisant la structure reelle du
    fichier INSEE "1_Pop_annu_compo_evol.xlsx": 3 lignes d'en-tete avant les
    donnees, annees anciennes marquees "nd " (non disponible, exclues), et
    dernieres annees suffixees " (p)" (provisoire, prefixe 4 chiffres
    extrait).
    """
    raw = _load_bytes("population_fr_sample.xlsx")
    population = normalize_population_xlsx(raw)

    # L'annee 1980 vaut "nd " (non disponible) dans la fixture -> exclue.
    assert 1980 not in population
    assert population == {
        2020: 67441850,
        2021: 67697091,
        2022: 68060207,
    }
