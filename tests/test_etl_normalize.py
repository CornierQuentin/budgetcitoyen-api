"""Tests unitaires du module de normalisation ETL (aucune dependance reseau).

Les fixtures utilisees sont de petits extraits, representatifs des formats
source reels observes sur data.economie.gouv.fr, copies dans
`tests/fixtures/`.
"""

import io
import json
from pathlib import Path

import pandas as pd
import pytest

from api.etl import normalize
from api.etl.normalize import (
    aggregate_depenses,
    aggregate_recettes,
    build_mission_alias_rows,
    build_mission_rows,
    clean_montant,
    extract_non_fiscal_et_psr_cour_des_comptes,
    extract_prelevements_sur_recettes,
    extract_prelevements_sur_recettes_legifrance,
    normalize_depenses_2012,
    normalize_depenses_2013,
    normalize_depenses_2014,
    normalize_depenses_2016,
    normalize_depenses_2017,
    normalize_depenses_2018,
    normalize_depenses_2020,
    normalize_depenses_attachment_detaillee,
    normalize_depenses_fiscales_xlsx,
    normalize_depenses_legifrance,
    normalize_depenses_records_json,
    normalize_marches_parquet,
    normalize_mission_name,
    normalize_pib_complement_insee_premiere,
    normalize_pib_csv,
    normalize_population_xlsx,
    normalize_recettes_cour_des_comptes,
    normalize_recettes_legifrance,
    normalize_recettes_records_json,
    resolve_mission_identities,
)
from api.models.depense_fiscale import StatutMontant
from api.models.recette import TypeRecette

FIXTURES = Path(__file__).parent / "fixtures"


def _load_json(name: str) -> list[dict]:
    data = json.loads((FIXTURES / name).read_text(encoding="utf-8"))
    return list(data["results"])


def _load_csv_cp1252(name: str) -> str:
    return (FIXTURES / name).read_bytes().decode("cp1252")


def _load_csv_utf8(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _load_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def _load_html(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


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
# Depenses 2012: dataset "dotation BG" + 2 nomenclatures a joindre
# ---------------------------------------------------------------------------


def test_normalize_depenses_2012_joint_les_2_nomenclatures() -> None:
    montants = _load_json("depenses_2012_montants_sample.json")
    nomenclature_mp = _load_json("depenses_2012_nomenclature_mission_programme_sample.json")
    nomenclature_dest = _load_json("depenses_2012_nomenclature_destination_sample.json")

    records = normalize_depenses_2012(montants, nomenclature_mp, nomenclature_dest, annee=2012)

    assert len(records) == 4
    assert all(r.annee == 2012 for r in records)

    aggregats = aggregate_depenses(records)
    par_cle = {(a.mission_code, a.programme_code, a.action_code): a for a in aggregats}
    assert len(aggregats) == 3

    # Defense/146/08: 2 lignes categorie (51 et 64) sommees par aggregate_depenses,
    # code mission et libelle programme resolus via la nomenclature mission-programme
    # (filtree "Budget général", la ligne CAS "Pensions"/741 du fixture est ignoree),
    # libelle action resolu via la nomenclature destination (2 lignes sous-action
    # partageant le meme libelle_action - seule la premiere est retenue).
    defense = par_cle[("DA", "146", "146-08")]
    assert defense.mission_libelle == "Défense"
    assert defense.programme_libelle == "Équipement des forces"
    assert defense.action_libelle == "Projection - mobilité - soutien"
    assert defense.ae == pytest.approx(1237084791 + 3000000)
    assert defense.cp == pytest.approx(786549540 + 3654071)

    recherche = par_cle[("RA", "231", "231-01")]
    assert recherche.programme_libelle == "Vie étudiante"
    assert recherche.action_libelle == "Aides directes"
    assert recherche.ae == pytest.approx(17485145)

    # Solidarite/137/02: programme absent de la nomenclature mission-programme
    # ET action absente de la nomenclature destination (fixtures volontairement
    # lacunaires sur ce cas) -> repli sur les codes bruts, sans code mission.
    orpheline = par_cle[("", "137", "137-02")]
    assert orpheline.mission_libelle == "Solidarité, insertion et égalité des chances"
    assert orpheline.programme_libelle == "137"
    assert orpheline.action_libelle == "02"
    assert orpheline.ae == 0.0
    assert orpheline.cp == 0.0


# ---------------------------------------------------------------------------
# Depenses 2013: meme famille que 2012, sans code mission disponible
# ---------------------------------------------------------------------------


def test_normalize_depenses_2013_sans_code_mission() -> None:
    montants = _load_json("depenses_2013_montants_sample.json")
    nomenclature_programme = _load_json("depenses_2013_nomenclature_programme_sample.json")
    nomenclature_dest = _load_json("depenses_2013_nomenclature_destination_sample.json")

    records = normalize_depenses_2013(
        montants, nomenclature_programme, nomenclature_dest, annee=2013
    )

    assert len(records) == 4
    # Aucune source 2013 n'expose de code mission LOLF: toujours vide.
    assert all(r.mission_code == "" for r in records)

    aggregats = aggregate_depenses(records)
    par_cle = {(a.mission_code, a.programme_code, a.action_code): a for a in aggregats}
    assert len(aggregats) == 3

    gfp = par_cle[("", "156", "156-09")]
    assert gfp.programme_libelle == (
        "Gestion fiscale et financière de l'État et du secteur public local"
    )
    assert gfp.action_libelle == "Soutien"
    assert gfp.ae == pytest.approx(30575404 + 591606608)
    assert gfp.cp == pytest.approx(47134660 + 591606608)

    securite = par_cle[("", "176", "176-01")]
    assert securite.programme_libelle == "Police nationale"
    assert securite.action_libelle == "Ordre public et protection de la souveraineté"

    # Defense/212/02: programme absent de la nomenclature programme ET action
    # absente de la nomenclature destination (fixtures volontairement
    # lacunaires) -> repli sur les codes bruts pour les deux libelles.
    orpheline = par_cle[("", "212", "212-02")]
    assert orpheline.programme_libelle == "212"
    assert orpheline.action_libelle == "02"
    assert orpheline.ae == 0.0
    assert orpheline.cp == 0.0


# ---------------------------------------------------------------------------
# Depenses 2014: 1 seule nomenclature BG-filtree, piege colonnes CP texte
# ---------------------------------------------------------------------------


def test_normalize_depenses_2014_cp_texte_avec_espaces_de_milliers() -> None:
    montants = _load_json("depenses_2014_montants_sample.json")
    nomenclature_dest = _load_json("depenses_2014_nomenclature_destination_sample.json")

    records = normalize_depenses_2014(montants, nomenclature_dest, annee=2014)

    assert len(records) == 3
    assert all(r.mission_code == "" for r in records)
    # Le piege du millesime: cplf ("1 112 702") est une CHAINE avec des espaces
    # de milliers alors qu'aelf (1112702) est un entier JSON propre - verifie
    # ici que le montant CP nettoye est bien un flottant numeriquement egal.
    culture_categorie_64 = records[0]
    assert culture_categorie_64.cp == pytest.approx(1112702.0)
    assert isinstance(culture_categorie_64.cp, float)

    aggregats = aggregate_depenses(records)
    par_cle = {(a.mission_code, a.programme_code, a.action_code): a for a in aggregats}
    assert len(aggregats) == 2

    # Culture/175/08: 2 lignes categorie (64 et 72), CP text-avec-espaces
    # sur les deux, sommees correctement malgre le typage source.
    culture = par_cle[("", "175", "175-08")]
    assert culture.programme_libelle == "Patrimoines"
    assert culture.action_libelle == "Acquisition et enrichissement des collections publiques"
    assert culture.ae == pytest.approx(1112702 + 2118745)
    assert culture.cp == pytest.approx(1112702 + 2118745)

    # GFP/221/04: absent de la nomenclature destination (fixture volontairement
    # lacunaire) -> repli sur les codes bruts pour les deux libelles. La ligne
    # CAS "Désendettement de l'État"/761 du fixture (type_de_mission != "Budget
    # général") est ignoree, meme si elle avait matche par accident.
    orpheline = par_cle[("", "221", "221-04")]
    assert orpheline.programme_libelle == "221"
    assert orpheline.action_libelle == "04"
    assert orpheline.cp == 0.0


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
# Depenses 2016: piece jointe "BG-Action_Titre" dediee (BG only), 3 lignes
# d'en-tete parasites, pas de code mission ni de libelle programme
# ---------------------------------------------------------------------------


def test_normalize_depenses_2016_saute_les_lignes_parasites_et_agrege_par_titre() -> None:
    csv_text = _load_csv_cp1252("lfi2016_bg_action_titre_sample.csv")
    records = normalize_depenses_2016(csv_text, annee=2016)

    # 3 lignes de donnees dans la fixture (2 lignes Titre pour la meme action
    # "Action exterieure de l'Etat"/105/1, 1 ligne "Defense"/144/3).
    assert len(records) == 3
    assert all(r.annee == 2016 for r in records)
    # Pas de code mission dans ce format: mission_code reste vide (repli sur
    # le slug du libelle assure par mission_key/resolve_mission_identities).
    assert all(r.mission_code == "" for r in records)
    # Pas de libelle programme dans ce format: replie sur le code programme.
    assert all(r.programme_libelle == r.programme_code for r in records)

    aggregats = aggregate_depenses(records)
    par_cle = {(a.mission_code, a.programme_code, a.action_code): a for a in aggregats}
    # 2 actions distinctes: la 2eme ligne Titre de l'action 105-1 (libelle
    # vide dans la source, artefact d'export en cellules fusionnees) doit se
    # sommer dans la MEME action, pas en creer une nouvelle.
    assert len(aggregats) == 2

    action_exterieure = par_cle[("", "105", "105-1")]
    assert action_exterieure.ae == pytest.approx(59992865 + 30206166)
    assert action_exterieure.cp == pytest.approx(59992865 + 30206166)
    assert action_exterieure.mission_libelle == "Action extérieure de l'État"
    # Le libelle action ne doit provenir que de la PREMIERE ligne Titre (qui
    # le porte), pas etre ecrase par la seconde (libelle vide en source).
    assert action_exterieure.action_libelle == "Coordination de l'action diplomatique"

    defense = par_cle[("", "144", "144-3")]
    # AE et CP different reellement sur cette ligne (147 956 304 vs 136 969 924).
    assert defense.ae == pytest.approx(147956304)
    assert defense.cp == pytest.approx(136969924)
    assert defense.mission_libelle == "Défense"


# ---------------------------------------------------------------------------
# Depenses 2017: piece jointe "BG-Action_Categorie" dediee (BG only), pas de
# lignes parasites mais colonne "Libelle" repetee 3x (parsing par indice)
# ---------------------------------------------------------------------------


def test_normalize_depenses_2017_parse_par_indice_et_agrege_par_categorie() -> None:
    csv_text = _load_csv_cp1252("lfi2017_bg_action_categorie_sample.csv")
    records = normalize_depenses_2017(csv_text, annee=2017)

    # 3 lignes de donnees (2 categories pour la meme action 105-01, 1 ligne
    # Defense/144-03).
    assert len(records) == 3
    assert all(r.annee == 2017 for r in records)
    assert all(r.mission_code == "" for r in records)
    # Contrairement a 2016, ce format fournit un vrai libelle programme (pas
    # de repli sur le code).
    action_exterieure_records = [r for r in records if r.action_code == "105-01"]
    assert all(
        r.programme_libelle == "Action de la France en Europe et dans le monde"
        for r in action_exterieure_records
    )
    assert all(
        r.action_libelle == "Coordination de l'action diplomatique"
        for r in action_exterieure_records
    )

    aggregats = aggregate_depenses(records)
    par_cle = {(a.mission_code, a.programme_code, a.action_code): a for a in aggregats}
    assert len(aggregats) == 2

    action_exterieure = par_cle[("", "105", "105-01")]
    # Colonne "AE-LF"/"CP-LF" (finale votee), pas AE-LF_N1/AE-PLF/AE-AMT.
    assert action_exterieure.ae == pytest.approx(41516956 + 20378460)
    assert action_exterieure.cp == pytest.approx(41516956 + 20378460)

    defense = par_cle[("", "144", "144-03")]
    assert defense.ae == pytest.approx(158238792)
    assert defense.cp == pytest.approx(139619851)
    assert defense.programme_libelle == "Environnement et prospective de la politique de défense"


# ---------------------------------------------------------------------------
# Depenses 2018: fichier unique, BG+CAS+CCF fusionnes
# ---------------------------------------------------------------------------


def test_normalize_depenses_2018_filtre_budget_general_et_somme_categories() -> None:
    csv_text = _load_csv_cp1252("lfi2018_act_cat_tit_sample.csv")
    records = normalize_depenses_2018(csv_text, annee=2018)

    # 7 lignes "Budget général" dans la fixture (5 categories AA/105/01 + 2
    # categories AB/216/01), 1 ligne CAS et 1 ligne CCF exclues.
    assert len(records) == 7
    assert all(r.annee == 2018 for r in records)
    assert {r.mission_code for r in records} == {"AA", "AB"}

    aggregats = aggregate_depenses(records)
    par_cle = {(a.mission_code, a.programme_code, a.action_code): a for a in aggregats}
    # 2 actions distinctes: AA/105/01 (5 categories sommees) et AB/216/01 (2
    # categories sommees).
    assert len(aggregats) == 2

    action_aa = par_cle[("AA", "105", "105-01")]
    assert action_aa.ae == pytest.approx(42355399 + 21483744 + 1249791 + 21982846 + 2783145)
    assert action_aa.cp == pytest.approx(42355399 + 21483744 + 1249791 + 21982846 + 2783145)
    assert action_aa.mission_libelle == "Action extérieure de l'État"
    assert action_aa.action_libelle == "Coordination de l'action diplomatique"

    action_ab = par_cle[("AB", "216", "216-01")]
    assert action_ab.ae == pytest.approx(194765112 + 130104072)
    assert action_ab.cp == pytest.approx(194765112 + 130104072)


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
# Recettes 2016-2020/2022/2023: rapports Cour des comptes ("Le budget de
# l'Etat en <annee>"), tableau "recettes fiscales nettes par impot" +
# "tableau d'equilibre" (recettes non fiscales, PSR)
# ---------------------------------------------------------------------------


def test_normalize_recettes_cour_des_comptes_2016_pandas_index_parasite() -> None:
    """Le fichier 2016 porte une colonne d'index pandas parasite en tete de
    chaque ligne (artefact `DataFrame.to_csv()` sans `index=False` cote
    source) et un titre fusionne sur la premiere ligne: le parseur doit
    localiser la colonne LFI et les libelles malgre cette structure.
    """
    csv_text = _load_csv_utf8("recettes_ccomptes_2016_impot_sample.csv")
    records = normalize_recettes_cour_des_comptes(csv_text, 2016, delimiter=";")

    par_type = {r.type: r.montant for r in records}
    assert par_type[TypeRecette.IR] == pytest.approx(72.14e9)
    assert par_type[TypeRecette.IS] == pytest.approx(32.84e9)
    assert par_type[TypeRecette.TICPE] == pytest.approx(15.85e9)
    assert par_type[TypeRecette.TVA] == pytest.approx(144.62e9)
    assert par_type[TypeRecette.AUTRES] == pytest.approx(22.41e9)
    # La ligne "Recettes nettes" (total) n'est pas un type reconnu.
    assert len(records) == 5


def test_normalize_recettes_cour_des_comptes_2017_libelles_net_et_ticpe_en_toutes_lettres() -> None:
    """2017 utilise "Impot NET sur le revenu"/"...sur les societes" (pas le
    libelle "sec" de 2016) et ecrit la TICPE en toutes lettres ("Taxe
    interieure de consommation sur les produits energetiques") plutot que
    l'abreviation - et sa colonne LFI n'a PAS de suffixe d'annee ("LFI" tout
    court, contrairement a "LFI 2016"). Les lignes de sous-detail
    "...dont ..." (valeurs "-" non numeriques dans plusieurs colonnes) ne
    doivent pas etre confondues avec une ligne d'impot.
    """
    csv_text = _load_csv_utf8("recettes_ccomptes_2017_impot_sample.csv")
    records = normalize_recettes_cour_des_comptes(csv_text, 2017, delimiter=";")

    par_type = {r.type: r.montant for r in records}
    assert par_type[TypeRecette.IR] == pytest.approx(73.4e9)
    assert par_type[TypeRecette.IS] == pytest.approx(29.1e9)
    assert par_type[TypeRecette.TICPE] == pytest.approx(10.6e9)
    assert par_type[TypeRecette.TVA] == pytest.approx(149.3e9)
    assert par_type[TypeRecette.AUTRES] == pytest.approx(30.0e9)
    assert len(records) == 5


def test_normalize_recettes_cour_des_comptes_2020_ignore_la_ligne_non_fiscale() -> None:
    """Le fichier 2020 (graphique) inclut une ligne "Recettes non fiscales"
    en plus de la decomposition par impot: elle ne doit PAS atterrir dans
    TypeRecette.AUTRES (qui ne doit contenir QUE "Autres recettes
    fiscales") - les recettes non fiscales sont traitees a part par
    `extract_non_fiscal_et_psr_cour_des_comptes`. Ce fichier a aussi un
    BOM UTF-8 et un en-tete avec cellules quotees contenant des retours a
    la ligne (ex: '"LFI\\n2020"').
    """
    csv_text = _load_csv_utf8("recettes_ccomptes_2020_impot_sample.csv")
    records = normalize_recettes_cour_des_comptes(csv_text, 2020, delimiter=";")

    par_type = {r.type: r.montant for r in records}
    assert par_type[TypeRecette.AUTRES] == pytest.approx(28.801e9)
    assert len(records) == 5


def test_normalize_recettes_cour_des_comptes_2022_delimiteur_virgule_et_unite_trompeuse() -> None:
    """2022 est delimite par une virgule (pas un point-virgule) et son
    en-tete "Designation des recettes (M.€)" est TROMPEUR: les valeurs sont
    en realite en Md EUR (confirme par recoupement avec le total agrege),
    pas en M EUR comme le laisserait penser le libelle - `_md_ou_m_vers_
    euros` doit se fier a l'ordre de grandeur, pas au texte de l'en-tete.
    La ligne "Total" ne doit pas etre comptee comme un impot.
    """
    csv_text = _load_csv_utf8("recettes_ccomptes_2022_impot_sample.csv")
    records = normalize_recettes_cour_des_comptes(csv_text, 2022, delimiter=",")

    par_type = {r.type: r.montant for r in records}
    assert par_type[TypeRecette.IR] == pytest.approx(82.361e9)
    assert par_type[TypeRecette.TVA] == pytest.approx(98.3552e9)
    assert len(records) == 5
    # Si l'unite avait ete lue comme M€ au lieu de Md€, ce total serait
    # ~1000x plus petit (~287 millions au lieu de ~287 milliards).
    assert sum(r.montant for r in records) == pytest.approx(287.5722e9)


def test_normalize_recettes_cour_des_comptes_leve_si_colonne_lfi_introuvable() -> None:
    with pytest.raises(ValueError, match="colonne LFI introuvable"):
        normalize_recettes_cour_des_comptes("Impôt sur le revenu;70;71\n", 2019, delimiter=";")


def test_extract_non_fiscal_et_psr_cour_des_comptes_2016_unite_m_euros() -> None:
    """Le tableau d'equilibre 2016 est en M EUR (contrairement au tableau
    "par impot" de la meme annee, en Md EUR): `_md_ou_m_vers_euros` doit
    detecter les deux unites correctement au sein du meme millesime.
    """
    csv_text = _load_csv_utf8("recettes_ccomptes_2016_equilibre_sample.csv")
    non_fiscal, psr = extract_non_fiscal_et_psr_cour_des_comptes(csv_text, 2016, delimiter=";")

    assert non_fiscal == pytest.approx(15648e6)
    assert psr.union_europeenne == pytest.approx(20169e6)
    assert psr.collectivites == pytest.approx(47305e6)
    assert psr.total == pytest.approx(67474e6)


def test_extract_non_fiscal_et_psr_cour_des_comptes_2020_labels_avec_parentheses() -> None:
    """2020 suffixe chaque libelle d'une lettre entre parentheses (ex:
    "Recettes non fiscales (b)", "PSR au profit de l'Union européenne (c)")
    - ne doit pas empecher le rapprochement. La ligne "Recettes fiscales
    nettes (a)" (qui contient bien "fiscales" mais PAS "non fiscales") ne
    doit pas etre confondue avec "Recettes non fiscales (b)".
    """
    csv_text = _load_csv_utf8("recettes_ccomptes_2020_equilibre_sample.csv")
    non_fiscal, psr = extract_non_fiscal_et_psr_cour_des_comptes(csv_text, 2020, delimiter=";")

    assert non_fiscal == pytest.approx(14364.273254e6)
    assert psr.union_europeenne == pytest.approx(21480.0e6)
    assert psr.collectivites == pytest.approx(41246.7400009999e6)


def test_extract_non_fiscal_et_psr_cour_des_comptes_2022_libelles_abreges_ue_ct() -> None:
    """2022 (delimite par une virgule) abrege les PSR en "UE"/pas d'abreviation
    CT explicite dans ce fichier - le libelle complet
    "collectivites territoriales" reste detecte via le fragment "collectivit".
    """
    csv_text = _load_csv_utf8("recettes_ccomptes_2022_equilibre_sample.csv")
    non_fiscal, psr = extract_non_fiscal_et_psr_cour_des_comptes(csv_text, 2022, delimiter=",")

    assert non_fiscal == pytest.approx(20177e6)
    assert psr.union_europeenne == pytest.approx(26359e6)
    assert psr.collectivites == pytest.approx(43241e6)


def test_extract_non_fiscal_et_psr_cour_des_comptes_leve_si_ligne_manquante() -> None:
    """2023 n'a pas de tableau d'equilibre exploitable (cf. `api.etl.sources.
    RECETTES_COUR_DES_COMPTES_FICHIER_EQUILIBRE`): si on lui passait quand
    meme le tableau "par impot" (qui n'a pas de ligne PSR/non fiscale), la
    fonction doit lever plutot que renvoyer silencieusement des zeros.
    """
    csv_text = _load_csv_utf8("recettes_ccomptes_2023_impot_sample.csv")
    with pytest.raises(ValueError, match="tableau d'equilibre Cour des comptes incomplet"):
        extract_non_fiscal_et_psr_cour_des_comptes(csv_text, 2023, delimiter=",")


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


# ---------------------------------------------------------------------------
# LFI 2026+ (API Legifrance/PISTE): Etat B (depenses), Etat A (recettes)
# ---------------------------------------------------------------------------


def test_normalize_depenses_legifrance_2026_mission_programme_par_align() -> None:
    """`align` distingue Mission (center) de Programme (left): la fixture
    contient 2 missions, 6 lignes Programme (dont 3 "Dont titre 2" exclues).
    """
    etat_b_html = _load_html("lfi2026_etat_b_sample.html")
    records = normalize_depenses_legifrance(etat_b_html, annee=2026)

    assert len(records) == 6
    assert all(r.annee == 2026 for r in records)
    # Pas de code mission ni de code programme dans cette source: repli sur
    # le slug (mission_code vide) et sur le libelle programme prefixe par la
    # mission (programme_code), cf. docstring de la fonction.
    assert all(r.mission_code == "" for r in records)
    assert {r.mission_libelle for r in records} == {
        "Action extérieure de l'Etat",
        "Administration générale et territoriale de l'Etat",
    }


def test_normalize_depenses_legifrance_2026_exclut_dont_titre_2_et_total() -> None:
    """ "Dont titre 2" (memo deja inclus dans le total programme) et "Total"
    (cross-check uniquement) ne doivent jamais apparaitre comme lignes de
    depense - sous peine de doubler les montants.
    """
    etat_b_html = _load_html("lfi2026_etat_b_sample.html")
    records = normalize_depenses_legifrance(etat_b_html, annee=2026)

    assert all(r.programme_libelle not in ("Dont titre 2", "Total") for r in records)

    aggregats = aggregate_depenses(records)
    # Le total AE/CP somme (hors "Dont titre 2"/"Total") doit retomber
    # exactement sur la ligne "Total" de la fixture (8 445 629 452 / 8 535
    # 968 788), verifie a la main sur les 2 missions/5 programmes de la
    # fixture.
    assert sum(a.ae for a in aggregats) == pytest.approx(8445629452.0)
    assert sum(a.cp for a in aggregats) == pytest.approx(8535968788.0)


def test_normalize_depenses_legifrance_2026_action_synthetique_et_programme_code_hash() -> None:
    """Aucune decomposition par action dans cette source: une action
    synthetique unique est creee par programme (memes libelle/code que le
    programme). `programme_code` est un hash court (<=16 caracteres, tient
    dans `programme.code`/`action.code`, limites a 50 caracteres en base -
    une simple concatenation de libelles la depasserait, cf. docstring de
    la fonction) du couple (mission, programme), deterministe et stable
    d'un appel a l'autre, pour eviter qu'un meme libelle de programme dans
    2 missions differentes ne fusionne a tort dans `aggregate_depenses`
    (cle mission_code/programme_code/action_code, ici mission_code vide
    pour toutes les lignes).
    """
    etat_b_html = _load_html("lfi2026_etat_b_sample.html")
    records = normalize_depenses_legifrance(etat_b_html, annee=2026)

    codes = [r.programme_code for r in records]
    assert len(codes) == len(set(codes))  # tous distincts
    for r in records:
        assert len(r.programme_code) <= 50
        assert r.action_code == r.programme_code
        assert r.action_libelle == r.programme_libelle

    # Determinisme: reparser la meme fixture doit produire les memes codes.
    records_bis = normalize_depenses_legifrance(etat_b_html, annee=2026)
    assert [r.programme_code for r in records_bis] == codes

    aggregats = aggregate_depenses(records)
    # 6 programmes distincts dans la fixture -> 6 aggregats (une seule
    # action par programme, aucune fusion).
    assert len(aggregats) == 6


def test_normalize_recettes_legifrance_2026_mappe_accises_vers_ticpe() -> None:
    """La reforme fiscale 2026 eclate l'ancienne ligne TICPE (code 1501) en 4
    codes (1501 ex-TICPE, 1502 ex-TICGN, 1503 ex-TICFE, 1504 "Autres taxes
    interieures") - les 4 sont regroupes dans le bucket TICPE (decision
    validee avec l'utilisateur, cf. `api.etl.sources.
    CODE_LIGNE_RECETTE_VERS_TYPE`).
    """
    etat_a_html = _load_html("lfi2026_etat_a_sample.html")
    records = normalize_recettes_legifrance(etat_a_html, annee=2026)

    ticpe_total = sum(r.montant for r in records if r.type == TypeRecette.TICPE)
    assert ticpe_total == pytest.approx(17469533401.0 + 2226300000.0 + 5585300000.0 + 9000000.0)
    assert ticpe_total == pytest.approx(25290133401.0)


def test_normalize_recettes_legifrance_2026_ignore_les_lignes_de_categorie() -> None:
    """Les lignes de categorie parentes (ex "1. Recettes fiscales", "11. Impot
    net sur le revenu" - qui dupliquent le montant de leur unique ligne
    numerotee "1101") ne doivent JAMAIS etre sommees en plus de leur(s)
    ligne(s) de detail, sous peine de compter deux fois.
    """
    etat_a_html = _load_html("lfi2026_etat_a_sample.html")
    records = normalize_recettes_legifrance(etat_a_html, annee=2026)

    ir_total = sum(r.montant for r in records if r.type == TypeRecette.IR)
    # Une seule ligne IR (code 1101) dans la fixture: si la ligne de
    # categorie "11." avait ete comptee en plus, le total serait double.
    assert ir_total == pytest.approx(99836208951.0)


def test_normalize_recettes_legifrance_2026_categorie_sans_enfant_est_une_feuille() -> None:
    """Cas reel observe sur la LFI 2026: la categorie "18. Autres remboursements
    et degrevements d'impots d'Etat" n'a AUCUNE ligne numerotee en dessous
    (contrairement a "11."-"17." qui en ont chacune au moins une) - son
    propre montant doit alors etre utilise comme une ligne de detail (pas
    ignore comme une categorie ordinaire), et tombe dans AUTRES faute de
    code_ligne_recettes associe.
    """
    etat_a_html = _load_html("lfi2026_etat_a_sample.html")
    records = normalize_recettes_legifrance(etat_a_html, annee=2026)

    autres_total = sum(r.montant for r in records if r.type == TypeRecette.AUTRES)
    # 2110 (1 257 454 531) + la categorie-feuille "18." (-10 461 709 884).
    assert autres_total == pytest.approx(1257454531.0 - 10461709884.0)


def test_normalize_recettes_legifrance_2026_exclut_les_lignes_psr() -> None:
    """Les lignes PSR (categorie "3.", codes 31xx/32xx) ne doivent jamais
    apparaitre dans `RecetteRecord` - traitees a part par
    `extract_prelevements_sur_recettes_legifrance`.
    """
    etat_a_html = _load_html("lfi2026_etat_a_sample.html")
    records = normalize_recettes_legifrance(etat_a_html, annee=2026)

    # 1101, 1301, 1501-1504, 1601, categorie-feuille 18, 2110 = 9 lignes;
    # aucune des 4 lignes PSR (31/3101/3106/32/3201... en realite 2 lignes
    # de detail: 3101, 3106, 3201) ne doit apparaitre.
    assert len(records) == 9


def test_extract_prelevements_sur_recettes_legifrance_2026_isole_collectivites_et_ue() -> None:
    etat_a_html = _load_html("lfi2026_etat_a_sample.html")
    psr = extract_prelevements_sur_recettes_legifrance(etat_a_html, annee=2026)

    assert psr.collectivites == pytest.approx(27405973591.0 + 17418111813.0)
    assert psr.union_europeenne == pytest.approx(28439880549.0)
    assert psr.total == pytest.approx(psr.collectivites + psr.union_europeenne)


def test_normalize_recettes_legifrance_2026_recettes_nettes_coherentes() -> None:
    """Verification forte de bout en bout: recettes (hors PSR) - PSR doit
    retomber exactement sur "Total des recettes, nettes des prelevements"
    de la fixture (204 092 159 647), calcule a la main a partir des memes
    lignes.
    """
    etat_a_html = _load_html("lfi2026_etat_a_sample.html")
    records = normalize_recettes_legifrance(etat_a_html, annee=2026)
    psr = extract_prelevements_sur_recettes_legifrance(etat_a_html, annee=2026)

    recettes_nettes = sum(r.montant for r in records) - psr.total
    assert recettes_nettes == pytest.approx(204092159647.0)


# ---------------------------------------------------------------------------
# LFI 2015/2021: memes normalizers Legifrance, reutilises pour combler les
# trous du backfill historique (recettes 2015/2021, depenses 2015)
# ---------------------------------------------------------------------------


def test_normalize_recettes_legifrance_2015_convertit_milliers_en_euros() -> None:
    """L'Etat A de la LFI 2015 est exprime en MILLIERS d'euros ("(En milliers
    d'euros)", verifie dans l'en-tete de la table source) - `unite_milliers=
    True` doit multiplier chaque montant par 1000. IR (ligne 1101) vaut
    "75 305 000" milliers dans la fixture -> 75 305 000 000 EUR.
    """
    etat_a_html = _load_html("lfi2015_etat_a_sample.html")
    records = normalize_recettes_legifrance(etat_a_html, annee=2015, unite_milliers=True)

    ir_total = sum(r.montant for r in records if r.type == TypeRecette.IR)
    assert ir_total == pytest.approx(75305000000.0)


def test_normalize_recettes_legifrance_2015_sans_conversion_donne_des_milliers() -> None:
    """Sans `unite_milliers=True` (comportement par defaut), les montants
    restent tels quels dans la source - sous-evalues d'un facteur 1000 pour
    2015. Ce test verifie explicitement que le defaut ne convertit PAS
    (le mauvais defaut serait un bug silencieux beaucoup plus dangereux
    qu'un oubli explicite de le passer).
    """
    etat_a_html = _load_html("lfi2015_etat_a_sample.html")
    records = normalize_recettes_legifrance(etat_a_html, annee=2015)

    ir_total = sum(r.montant for r in records if r.type == TypeRecette.IR)
    assert ir_total == pytest.approx(75305000.0)


def test_extract_prelevements_sur_recettes_legifrance_2015_convertit_milliers_en_euros() -> None:
    etat_a_html = _load_html("lfi2015_etat_a_sample.html")
    psr = extract_prelevements_sur_recettes_legifrance(etat_a_html, annee=2015, unite_milliers=True)

    # 3101 (36 607 053) + 3103 (18 662) milliers -> EUR
    assert psr.collectivites == pytest.approx((36607053.0 + 18662.0) * 1000)
    assert psr.union_europeenne == pytest.approx(20742000.0 * 1000)


def test_normalize_recettes_legifrance_2021_meme_structure_que_2026_sans_conversion() -> None:
    """LFI 2021: meme structure qu'Etat A 2026, mais deja en euros (pas de
    conversion necessaire, contrairement a 2015).
    """
    etat_a_html = _load_html("lfi2021_etat_a_sample.html")
    records = normalize_recettes_legifrance(etat_a_html, annee=2021, unite_milliers=False)

    par_type = {r.type: r.montant for r in records}
    assert par_type[TypeRecette.IR] == pytest.approx(92835138856.0)
    assert par_type[TypeRecette.IS] == pytest.approx(62984885027.0)
    assert par_type[TypeRecette.TICPE] == pytest.approx(19194042064.0)
    assert par_type[TypeRecette.TVA] == pytest.approx(145493491163.0)


def test_normalize_depenses_legifrance_2015_exclut_dont_titre_2_et_totaux() -> None:
    """Bug reel trouve a l'execution contre les vraies donnees 2015 (pas
    detectable avec une fixture calquee sur le format 2026 seul): la ligne
    de total final s'appelle "Totaux" (PLURIEL) pour la LFI 2015, pas
    "Total" comme 2021/2026 - sans l'exclure aussi, elle est comptee comme
    un programme normal et double le total (15 037 775 416 attendu contre
    ~30 Md si la ligne "Totaux" de la fixture, qui vaut exactement le
    total reel, etait comptee en plus).
    """
    etat_b_html = _load_html("lfi2015_etat_b_sample.html")
    records = normalize_depenses_legifrance(etat_b_html, annee=2015)

    assert all(r.programme_libelle not in ("Dont titre 2", "Totaux", "Total") for r in records)

    aggregats = aggregate_depenses(records)
    assert sum(a.ae for a in aggregats) == pytest.approx(15037775416.0)
    assert sum(a.cp for a in aggregats) == pytest.approx(14325062285.0)


# ---------------------------------------------------------------------------
# Depenses fiscales (niches fiscales), annexe "Voies et moyens" Tome II du PLF
# ---------------------------------------------------------------------------


def test_normalize_depenses_fiscales_xlsx_lit_les_10_mesures_de_la_fixture() -> None:
    """La fixture est un extrait REEL de 10 mesures (memes 4 feuilles/en-tetes
    que le fichier source PLF2023, verifie a l'inspection directe du fichier
    reel avant construction de la fixture) couvrant les 4 statuts de montant
    et les 2 champs nullables (sous_sous_categorie, methode_chiffrage).
    """
    raw = _load_bytes("depenses_fiscales_2021_sample.xlsx")
    records = normalize_depenses_fiscales_xlsx(raw, annee=2021)

    assert len(records) == 10
    assert all(r.annee == 2021 for r in records)
    par_numero = {r.numero: r for r in records}

    chiffre = par_numero["40107"]
    assert chiffre.categorie == "Impôts locaux"
    assert chiffre.sous_categorie == "Cotisation sur la valeur ajoutée des entreprises"
    assert chiffre.sous_sous_categorie == "Exonérations compensées par l'Etat"
    assert chiffre.beneficiaire == "Entreprises"
    assert chiffre.statut_montant == StatutMontant.CHIFFRE
    assert chiffre.montant_millions == pytest.approx(1.0)
    assert chiffre.methode_chiffrage is not None

    assert par_numero["40101"].statut_montant == StatutMontant.EPSILON
    assert par_numero["40101"].montant_millions is None

    assert par_numero["110267"].statut_montant == StatutMontant.AUCUN_EFFET
    assert par_numero["110267"].montant_millions is None

    assert par_numero["110307"].statut_montant == StatutMontant.NON_CALCULABLE
    assert par_numero["110307"].montant_millions is None

    # 320105/430101 n'ont pas de sous-sous-categorie dans la fixture (comme
    # dans le fichier reel, 53/465 mesures dans ce cas) - jamais une chaine
    # vide inventee.
    assert par_numero["320105"].sous_sous_categorie is None

    # 110268/110307 n'ont pas de methode de chiffrage renseignee dans la
    # fixture (comme 85/465 mesures dans le fichier reel).
    assert par_numero["110268"].methode_chiffrage is None


def test_normalize_depenses_fiscales_xlsx_exclut_les_non_chiffrables_d_un_total_naif() -> None:
    """Un total naif somme uniquement les statuts CHIFFRE: epsilon/nc/aucun
    effet ne doivent jamais etre traites comme 0 (fausserait le total).
    """
    raw = _load_bytes("depenses_fiscales_2021_sample.xlsx")
    records = normalize_depenses_fiscales_xlsx(raw, annee=2021)

    chiffres = [r for r in records if r.statut_montant == StatutMontant.CHIFFRE]
    non_chiffres = [r for r in records if r.statut_montant != StatutMontant.CHIFFRE]

    assert len(chiffres) == 6
    assert len(non_chiffres) == 4
    assert all(r.montant_millions is None for r in non_chiffres)
    total = sum(r.montant_millions for r in chiffres)
    assert total == pytest.approx(118.0)


# ---------------------------------------------------------------------------
# Marches publics (DECP, dataset decp-2022-marches-valides)
# ---------------------------------------------------------------------------


def test_normalize_marches_parquet_lit_les_10_marches_de_la_fixture() -> None:
    """La fixture est un extrait REEL de 10 marches couvrant les pieges
    confirmes a l'inspection directe du fichier reel: 2 lignes partageant le
    meme `id` source "2024" (l'id n'est pas une cle fiable, pas de dedup
    attendu), 2 lignes `titulaire_typeidentifiant_1 == "TVA"` (pas SIRET), 2
    lignes `codecpv` prefixe "INX ", plusieurs lignes `offresrecues ==
    "MQ NC"` (non numerique)."""
    raw = _load_bytes("marches_sample.parquet")
    batches = list(normalize_marches_parquet(raw, batch_size=4))

    records = [r for batch in batches for r in batch]
    assert len(records) == 10
    # Chunking respecte: 3 lots de 4/4/2 pour 10 enregistrements.
    assert [len(b) for b in batches] == [4, 4, 2]

    ids_2024 = [r for r in records if r.marche_id_source == "2024"]
    assert len(ids_2024) == 2  # pas de dedup: la collision d'id est preservee telle quelle

    non_siret = [r for r in records if r.titulaire_id_type == "TVA"]
    assert len(non_siret) == 2

    codes_inx = [r for r in records if r.codecpv.startswith("INX")]
    assert len(codes_inx) == 2
    assert all(r.codecpv_division == "IN" for r in codes_inx)

    sans_offres = [r for r in records if r.offresrecues is None]
    assert len(sans_offres) > 0  # au moins un "MQ NC" -> None, jamais 0

    premier = records[0]
    assert premier.marche_id_source == "2024202400014"
    assert premier.objet_recherche == "services de telecommunications - lot 02 - telephonie mobile"
    assert premier.codecpv_division == "64"
    assert premier.marcheinnovant is True
    assert premier.offresrecues == 2


def test_normalize_marches_parquet_marcheinnovant_oui_non_convertis_en_booleen() -> None:
    raw = _load_bytes("marches_sample.parquet")
    records = [r for batch in normalize_marches_parquet(raw) for r in batch]

    assert any(r.marcheinnovant is True for r in records)
    assert any(r.marcheinnovant is False for r in records)


# ---------------------------------------------------------------------------
# normalize_depenses_lfi_xls (fichier LFI exploitable du ministere, 2026)
# ---------------------------------------------------------------------------


def _fixture_lfi_2026() -> bytes:
    return (Path(__file__).parent / "fixtures" / "lfi_2026_credits_sample.xlsx").read_bytes()


def test_normalize_depenses_lfi_xls_garde_le_budget_general_seul() -> None:
    """Budgets annexes et comptes speciaux sont hors perimetre de toute la serie."""
    records = normalize.normalize_depenses_lfi_xls(_fixture_lfi_2026(), 2026)

    # La fixture contient 5 lignes BG et 2 hors BG.
    assert len(records) == 5
    assert {r.annee for r in records} == {2026}


def test_normalize_depenses_lfi_xls_expose_numeros_et_actions() -> None:
    """Ce que l'Etat B du Journal officiel ne donne pas: numeros et actions."""
    records = normalize.normalize_depenses_lfi_xls(_fixture_lfi_2026(), 2026)

    p178 = [r for r in records if r.programme_code == "178"]
    # 4 lignes pour 3 actions: deux lignes portent la meme action (le fichier
    # descend jusqu'a la sous-action), et c'est `aggregate_depenses` qui les
    # somme en aval, pas ce normalizer.
    assert len(p178) == 4
    # Code d'action au format "<programme>-<action sur 2 chiffres>", identique
    # aux autres annees pour que la nomenclature reste comparable.
    assert {r.action_code for r in p178} == {"178-01", "178-02", "178-03"}
    assert all(r.mission_libelle == "Défense" for r in p178)
    assert all(r.mission_code == "DA" for r in p178)


def test_normalize_depenses_lfi_xls_lit_les_montants_votes() -> None:
    """Le fichier expose aussi les colonnes PLF et amendements: confondre les
    trois donnerait un total plausible mais faux."""
    records = normalize.normalize_depenses_lfi_xls(_fixture_lfi_2026(), 2026)

    p200 = next(r for r in records if r.programme_code == "200")
    # Ligne unique du programme 200 dans la fixture, montant vote.
    assert p200.cp > 0
    assert p200.ae > 0
    # Le libelle porte "d'Etat" ACCENTUE dans cette source, la ou l'Etat B
    # ecrit "d'Etat": c'est ce qui a impose d'elargir le filtre du rattrapage
    # brut/net (cf. loader.get_remboursements_degrevements_impots_etat_cp).
    assert "État" in p200.programme_libelle


def test_normalize_depenses_lfi_xls_refuse_une_colonne_de_montant_ambigue() -> None:
    """Une erreur de colonne de montant serait invisible et fausserait l'annee:
    on leve plutot que de retomber sur une colonne voisine."""
    df = pd.read_excel(io.BytesIO(_fixture_lfi_2026()))
    df = df.drop(columns=[c for c in df.columns if str(c).startswith("CP (T2 + Hors T2) LFI")])
    tampon = io.BytesIO()
    df.to_excel(tampon, index=False)

    with pytest.raises(ValueError, match="colonne"):
        normalize.normalize_depenses_lfi_xls(tampon.getvalue(), 2026)
