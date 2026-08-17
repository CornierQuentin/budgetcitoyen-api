"""Tests du router /api/v1/budget."""

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.annee_budget import AnneeBudget


async def _seed_annees(db_session: AsyncSession) -> None:
    db_session.add_all(
        [
            AnneeBudget(
                annee=2023,
                depenses_nettes=1000.0,
                recettes_nettes=900.0,
                deficit=-100.0,
                dette_pib=None,
                source_url="https://example.test/2023",
            ),
            AnneeBudget(
                annee=2024,
                depenses_nettes=1100.0,
                recettes_nettes=950.0,
                deficit=-150.0,
                dette_pib=None,
                source_url="https://example.test/2024",
            ),
            AnneeBudget(
                annee=2025,
                depenses_nettes=1200.0,
                recettes_nettes=1000.0,
                deficit=-200.0,
                dette_pib=None,
                source_url="https://example.test/2025",
            ),
        ]
    )
    await db_session.commit()


async def test_liste_vide_retourne_200(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/budget/annees")

    assert response.status_code == 200
    assert response.json() == []


async def test_ressource_inexistante_retourne_404_rfc7807(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/budget/2099")

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 404


async def test_obtenir_annee_existante_retourne_200(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_annees(db_session)

    response = await async_client.get("/api/v1/budget/2024")

    assert response.status_code == 200
    body = response.json()
    assert body["annee"] == 2024
    assert body["deficit"] == -150.0


async def test_historique_sans_filtre_retourne_toutes_les_annees(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_annees(db_session)

    response = await async_client.get("/api/v1/budget/historique")

    assert response.status_code == 200
    body = response.json()
    assert [item["annee"] for item in body] == [2023, 2024, 2025]


async def test_historique_filtre_par_plage_de_a(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_annees(db_session)

    response = await async_client.get("/api/v1/budget/historique", params={"de": 2024, "a": 2024})

    assert response.status_code == 200
    body = response.json()
    assert [item["annee"] for item in body] == [2024]


async def test_historique_liste_vide_retourne_200(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/budget/historique")

    assert response.status_code == 200
    assert response.json() == []
