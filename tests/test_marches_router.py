"""Tests du router /api/v1/marches (module V2, non implemente)."""

from httpx import AsyncClient


async def test_lister_marches_retourne_501_non_implemente(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/marches")

    assert response.status_code == 501
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 501
