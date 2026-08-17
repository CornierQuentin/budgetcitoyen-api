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
