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
    extract_non_fiscal_et_psr_cour_des_comptes,
    extract_prelevements_sur_recettes,
    normalize_depenses_2012,
    normalize_depenses_2013,
    normalize_depenses_2014,
    normalize_depenses_2016,
    normalize_depenses_2017,
    normalize_depenses_2018,
    normalize_depenses_2020,
    normalize_depenses_attachment_detaillee,
    normalize_depenses_records_json,
    normalize_mission_name,
    normalize_pib_complement_insee_premiere,
    normalize_pib_csv,
    normalize_population_xlsx,
    normalize_recettes_cour_des_comptes,
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


def _load_csv_utf8(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


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
