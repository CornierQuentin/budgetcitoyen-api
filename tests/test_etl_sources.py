"""Tests des constructeurs d'URL et helpers de resolution de sources ETL.

Fonctions pures (aucun appel reseau): verifient uniquement la forme des URLs
produites et le routage par annee de `default_depenses_source_url`.
"""

from api.etl import sources


def test_records_url_construit_l_url_de_l_endpoint_records() -> None:
    url = sources.records_url("mon-dataset")

    assert url == f"{sources.API_EXPLORE_V21}/mon-dataset/records"


def test_dataset_metadata_url_construit_l_url_catalog() -> None:
    url = sources.dataset_metadata_url("mon-dataset")

    assert url == f"{sources.API_EXPLORE_V21}/mon-dataset"


def test_attachment_url_construit_l_url_de_telechargement() -> None:
    url = sources.attachment_url("mon-dataset", "mon-attachment")

    assert url == f"{sources.API_EXPLORE_V21}/mon-dataset/attachments/mon-attachment"


def test_default_depenses_source_url_annee_records() -> None:
    # 2019 est dans DEPENSES_DATASETS_RECORDS.
    url = sources.default_depenses_source_url(2019)

    assert url is not None
    assert sources.DEPENSES_DATASETS_RECORDS[2019] in url


def test_default_depenses_source_url_annee_2012_2014() -> None:
    # 2012 est dans DEPENSES_DATASETS_2012_2014 (dataset "montants").
    url = sources.default_depenses_source_url(2012)

    assert url is not None
    assert sources.DEPENSES_DATASETS_2012_2014[2012]["montants"] in url


def test_default_depenses_source_url_annee_2020_cas_particulier() -> None:
    # 2020 a un traitement dedie (piece jointe "credits", pas "detaillee").
    url = sources.default_depenses_source_url(2020)

    assert url is not None
    assert sources.DEPENSES_ATTACHMENT_IDS[2020]["credits"] in url


def test_default_depenses_source_url_annee_attachments_standard() -> None:
    # 2016/2017/2018/2021/2022: piece jointe "detaillee".
    for annee in (2016, 2017, 2018, 2021, 2022):
        url = sources.default_depenses_source_url(annee)
        assert url is not None
        assert sources.DEPENSES_ATTACHMENT_IDS[annee]["detaillee"] in url


def test_default_depenses_source_url_annee_hors_perimetre_retourne_none() -> None:
    # 2015: trou reel documente, hors perimetre de toutes les sources connues.
    assert sources.default_depenses_source_url(2015) is None


def test_legifrance_url_construit_l_url_publique_du_texte() -> None:
    url = sources.legifrance_url("JORFTEXT000053508155")

    assert url == "https://www.legifrance.gouv.fr/jorf/id/JORFTEXT000053508155"


def test_default_depenses_source_url_annee_2026_legifrance() -> None:
    # 2026: LFI via l'API Legifrance/PISTE, source_url = page publique
    # Legifrance (pas l'endpoint API, protege par OAuth).
    url = sources.default_depenses_source_url(2026)

    assert url == sources.legifrance_url(sources.LFI_TEXT_CID_PAR_ANNEE[2026])


def test_2026_couvert_par_depenses_et_recettes_legifrance() -> None:
    assert 2026 in sources.DEPENSES_ANNEES
    assert 2026 in sources.RECETTES_LEGIFRANCE_ANNEES
    # 2026 ne doit apparaitre dans AUCUNE des 2 autres listes de recettes
    # (double traitement non gere par `api.etl.run.run_etl`).
    assert 2026 not in sources.RECETTES_ANNEES
    assert 2026 not in sources.RECETTES_COUR_DES_COMPTES_ANNEES
