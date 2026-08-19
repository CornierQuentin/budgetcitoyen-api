"""Tests du router /api/v1/marches (marches publics, DECP)."""

from datetime import date

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.marche_public import MarchePublic


async def _seed_marches(db_session: AsyncSession) -> None:
    db_session.add_all(
        [
            MarchePublic(
                marche_id_source="1",
                nature="Marché",
                objet="Travaux de rénovation d'une école",
                objet_recherche="travaux de renovation d'une ecole",
                codecpv="45000000-7",
                codecpv_division="45",
                procedure="Appel d'offres ouvert",
                acheteur_siret="12345678900011",
                titulaire_siret="98765432100022",
                titulaire_id_type="SIRET",
                dureemois=12,
                datenotification=date(2024, 1, 1),
                datepublicationdonnees=None,
                montant=500000.0,
                formeprix=None,
                offresrecues=3,
                marcheinnovant=False,
            ),
            MarchePublic(
                marche_id_source="2",
                nature="Marché",
                objet="Fourniture de matériel informatique",
                objet_recherche="fourniture de materiel informatique",
                codecpv="30200000-1",
                codecpv_division="30",
                procedure="Procédure adaptée",
                acheteur_siret="12345678900011",
                titulaire_siret="FR59000017896",
                titulaire_id_type="TVA",
                dureemois=6,
                datenotification=date(2024, 6, 1),
                datepublicationdonnees=None,
                montant=25000.0,
                formeprix=None,
                offresrecues=None,
                marcheinnovant=True,
            ),
        ]
    )
    await db_session.commit()


async def test_lister_marches_vide_retourne_200(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/marches")

    assert response.status_code == 200
    body = response.json()
    assert body == {"items": [], "total": 0, "page": 1, "page_size": 20, "total_pages": 0}


async def test_lister_marches_retourne_la_page_demandee(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_marches(db_session)

    response = await async_client.get("/api/v1/marches", params={"page": 1, "page_size": 1})

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["total_pages"] == 2
    assert len(body["items"]) == 1
    # Tri par datenotification decroissant: le marche du 2024-06-01 en premier.
    assert body["items"][0]["marche_id_source"] == "2"


async def test_lister_marches_filtre_par_recherche(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_marches(db_session)

    response = await async_client.get("/api/v1/marches", params={"q": "renovation"})

    body = response.json()
    assert body["total"] == 1
    assert body["items"][0]["marche_id_source"] == "1"


async def test_lister_marches_page_size_hors_bornes_retourne_422(
    async_client: AsyncClient,
) -> None:
    response = await async_client.get("/api/v1/marches", params={"page_size": 500})

    assert response.status_code == 422


async def test_obtenir_repartition_cpv(async_client: AsyncClient, db_session: AsyncSession) -> None:
    await _seed_marches(db_session)

    response = await async_client.get("/api/v1/marches/repartition-cpv")

    assert response.status_code == 200
    body = response.json()
    divisions = {item["cpv_division"]: item for item in body}
    assert divisions["45"]["label"] == "Travaux de construction"
    assert divisions["45"]["montant_total"] == 500000.0
    assert divisions["45"]["nombre"] == 1


async def test_obtenir_bornes(async_client: AsyncClient, db_session: AsyncSession) -> None:
    await _seed_marches(db_session)

    response = await async_client.get("/api/v1/marches/bornes")

    assert response.status_code == 200
    body = response.json()
    assert body["date_min"] == "2024-01-01"
    assert body["date_max"] == "2024-06-01"
    assert body["montant_min"] == 25000.0
    assert body["montant_max"] == 500000.0
