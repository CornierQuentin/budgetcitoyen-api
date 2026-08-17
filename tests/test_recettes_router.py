"""Tests du router /api/v1/recettes."""

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.recette import Recette, TypeRecette


async def _seed_recettes(db_session: AsyncSession) -> None:
    db_session.add_all(
        [
            Recette(annee=2023, type=TypeRecette.IR, montant_brut=90.0, montant_net=85.0),
            Recette(annee=2023, type=TypeRecette.TVA, montant_brut=200.0, montant_net=195.0),
            Recette(annee=2024, type=TypeRecette.IR, montant_brut=95.0, montant_net=90.0),
            Recette(annee=2024, type=TypeRecette.TVA, montant_brut=210.0, montant_net=205.0),
        ]
    )
    await db_session.commit()


async def test_liste_vide_retourne_200(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/recettes/historique")

    assert response.status_code == 200
    assert response.json() == []


async def test_ressource_inexistante_retourne_404_rfc7807(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/recettes/2099")

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 404


async def test_obtenir_recettes_existantes_retourne_200(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_recettes(db_session)

    response = await async_client.get("/api/v1/recettes/2023")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    types = {item["type"] for item in body}
    assert types == {"IR", "TVA"}


async def test_historique_filtre_par_plage_d_annees(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_recettes(db_session)

    response = await async_client.get("/api/v1/recettes/historique", params={"de": 2024, "a": 2024})

    assert response.status_code == 200
    body = response.json()
    assert {item["annee"] for item in body} == {2024}


async def test_historique_filtre_par_type(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_recettes(db_session)

    response = await async_client.get("/api/v1/recettes/historique", params={"type": "IR"})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert all(item["type"] == "IR" for item in body)
