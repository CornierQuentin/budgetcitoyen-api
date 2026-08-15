"""Tests du router /api/v1/budget-perso.

Le service sous-jacent n'est pas implemente (cf. TODO methodologie TVA dans
budget_perso_service.py): on verifie ici le contrat d'erreur plutot qu'un
resultat de calcul.
"""

from httpx import AsyncClient


async def test_parametre_manquant_retourne_422(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/budget-perso")

    assert response.status_code == 422


async def test_calcul_non_implemente_retourne_500_rfc7807(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/budget-perso", params={"revenu_net": 30000})

    assert response.status_code == 500
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 500
