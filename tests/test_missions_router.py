"""Tests du router /api/v1/missions."""

from httpx import AsyncClient


async def test_liste_vide_retourne_200(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/missions")

    assert response.status_code == 200
    assert response.json() == []


async def test_ressource_inexistante_retourne_404_rfc7807(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/missions/mission-inexistante")

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 404
