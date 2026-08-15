"""Tests de l'endpoint de sante."""

from httpx import AsyncClient


async def test_health_retourne_200_et_status_ok(async_client: AsyncClient) -> None:
    response = await async_client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
