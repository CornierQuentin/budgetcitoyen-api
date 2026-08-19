"""Tests de la logique metier des marches publics (pagination + filtres)."""

from datetime import date

from sqlalchemy.ext.asyncio import AsyncSession

from api.models.marche_public import MarchePublic
from api.services import marche_service


def _marche(**overrides: object) -> MarchePublic:
    defaults: dict = {
        "marche_id_source": "1",
        "nature": "Marché",
        "objet": "Objet du marché",
        "objet_recherche": "objet du marche",
        "codecpv": "45000000-7",
        "codecpv_division": "45",
        "procedure": "Procédure adaptée",
        "acheteur_siret": "12345678900011",
        "titulaire_siret": "98765432100022",
        "titulaire_id_type": "SIRET",
        "dureemois": 12,
        "datenotification": date(2024, 1, 1),
        "datepublicationdonnees": None,
        "montant": 10000.0,
        "formeprix": None,
        "offresrecues": None,
        "marcheinnovant": None,
    }
    defaults.update(overrides)
    return MarchePublic(**defaults)


async def _seed(db: AsyncSession, *marches: MarchePublic) -> None:
    db.add_all(marches)
    await db.commit()


async def test_lister_marches_vide_retourne_liste_vide_pas_une_erreur(
    db_session: AsyncSession,
) -> None:
    items, total = await marche_service.lister_marches(db_session)

    assert items == []
    assert total == 0


async def test_lister_marches_pagine(db_session: AsyncSession) -> None:
    await _seed(
        db_session,
        _marche(marche_id_source="1", datenotification=date(2024, 1, 1)),
        _marche(marche_id_source="2", datenotification=date(2024, 1, 2)),
        _marche(marche_id_source="3", datenotification=date(2024, 1, 3)),
    )

    items, total = await marche_service.lister_marches(db_session, page=1, page_size=2)

    assert total == 3
    assert len(items) == 2
    # Tri fixe par datenotification decroissant.
    assert [m.marche_id_source for m in items] == ["3", "2"]

    items_page2, total_page2 = await marche_service.lister_marches(db_session, page=2, page_size=2)
    assert total_page2 == 3
    assert [m.marche_id_source for m in items_page2] == ["1"]


async def test_lister_marches_filtre_par_recherche_texte_insensible_aux_accents(
    db_session: AsyncSession,
) -> None:
    await _seed(
        db_session,
        _marche(
            marche_id_source="1",
            objet="Travaux de rénovation",
            objet_recherche="travaux de renovation",
        ),
        _marche(
            marche_id_source="2",
            objet="Fourniture de bureau",
            objet_recherche="fourniture de bureau",
        ),
    )

    items, total = await marche_service.lister_marches(db_session, q="renovation")

    assert total == 1
    assert items[0].marche_id_source == "1"


async def test_lister_marches_filtre_par_plage_de_dates(db_session: AsyncSession) -> None:
    await _seed(
        db_session,
        _marche(marche_id_source="1", datenotification=date(2023, 6, 1)),
        _marche(marche_id_source="2", datenotification=date(2024, 6, 1)),
        _marche(marche_id_source="3", datenotification=date(2025, 6, 1)),
    )

    items, total = await marche_service.lister_marches(
        db_session, date_debut=date(2024, 1, 1), date_fin=date(2024, 12, 31)
    )

    assert total == 1
    assert items[0].marche_id_source == "2"


async def test_lister_marches_filtre_par_plage_de_montant(db_session: AsyncSession) -> None:
    await _seed(
        db_session,
        _marche(marche_id_source="1", montant=100.0),
        _marche(marche_id_source="2", montant=5000.0),
        _marche(marche_id_source="3", montant=1_000_000.0),
    )

    items, total = await marche_service.lister_marches(
        db_session, montant_min=1000.0, montant_max=10000.0
    )

    assert total == 1
    assert items[0].marche_id_source == "2"


async def test_lister_marches_filtre_par_division_cpv(db_session: AsyncSession) -> None:
    await _seed(
        db_session,
        _marche(marche_id_source="1", codecpv_division="45"),
        _marche(marche_id_source="2", codecpv_division="33"),
    )

    items, total = await marche_service.lister_marches(db_session, cpv_division="33")

    assert total == 1
    assert items[0].marche_id_source == "2"


async def test_repartition_cpv_agrege_par_division(db_session: AsyncSession) -> None:
    await _seed(
        db_session,
        _marche(marche_id_source="1", codecpv_division="45", montant=1000.0),
        _marche(marche_id_source="2", codecpv_division="45", montant=2000.0),
        _marche(marche_id_source="3", codecpv_division="33", montant=500.0),
    )

    resultat = await marche_service.repartition_cpv(db_session)

    par_division = {division: (montant, nombre) for division, montant, nombre in resultat}
    assert par_division["45"] == (3000.0, 2)
    assert par_division["33"] == (500.0, 1)


async def test_bornes_retourne_min_max_dates_et_montants(db_session: AsyncSession) -> None:
    await _seed(
        db_session,
        _marche(marche_id_source="1", datenotification=date(2020, 1, 1), montant=10.0),
        _marche(marche_id_source="2", datenotification=date(2025, 1, 1), montant=999.0),
    )

    date_min, date_max, montant_min, montant_max = await marche_service.bornes(db_session)

    assert date_min == date(2020, 1, 1)
    assert date_max == date(2025, 1, 1)
    assert montant_min == 10.0
    assert montant_max == 999.0


async def test_bornes_sur_table_vide_retourne_none(db_session: AsyncSession) -> None:
    date_min, date_max, montant_min, montant_max = await marche_service.bornes(db_session)

    assert date_min is None
    assert date_max is None
    assert montant_min is None
    assert montant_max is None
