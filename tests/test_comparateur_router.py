"""Tests du router /api/v1/comparateur.

Ce router ne renvoie pas de liste (il compare deux annees precises), les tests
adaptent donc le pattern standard: un cas de parametres manquants (422) et un
cas d'annee inexistante (404 RFC7807).
"""

from httpx import AsyncClient


async def test_parametres_manquants_retourne_422(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/comparateur")

    assert response.status_code == 422


async def test_ressource_inexistante_retourne_404_rfc7807(async_client: AsyncClient) -> None:
    response = await async_client.get(
        "/api/v1/comparateur", params={"annee_a": 2099, "annee_b": 2098}
    )

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 404
