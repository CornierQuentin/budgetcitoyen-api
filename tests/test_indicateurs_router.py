"""Tests du router /api/v1/indicateurs."""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.indicateur_macro import IndicateurMacro


async def test_indicateur_inexistant_retourne_404_rfc7807(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/indicateurs/2099")

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 404


async def test_indicateur_existant_retourne_200(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    db_session.add(
        IndicateurMacro(
            annee=2024,
            pib_courant=2919900000000.0,
            population=68436616,
            source_pib_url="https://example.org/pib-2024.xlsx",
            source_population_url="https://example.org/population.xlsx",
        )
    )
    await db_session.commit()

    response = await async_client.get("/api/v1/indicateurs/2024")

    assert response.status_code == 200
    body = response.json()
    assert body["annee"] == 2024
    assert body["pib_courant"] == pytest.approx(2919900000000.0)
    assert body["population"] == 68436616


async def test_indicateur_avec_seulement_la_population_garde_pib_null(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    """Une annee sans PIB source (ex: trou 2023-2025 non comble) garde
    `pib_courant` a None plutot que de bloquer la validation Pydantic."""
    db_session.add(
        IndicateurMacro(
            annee=2019,
            pib_courant=None,
            population=64700000,
            source_pib_url=None,
            source_population_url="https://example.org/population.xlsx",
        )
    )
    await db_session.commit()

    response = await async_client.get("/api/v1/indicateurs/2019")

    assert response.status_code == 200
    body = response.json()
    assert body["pib_courant"] is None
    assert body["population"] == 64700000


async def test_historique_filtre_par_plage_d_annees(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    db_session.add_all(
        [
            IndicateurMacro(annee=2020, pib_courant=2317832000000.0, population=67441850),
            IndicateurMacro(annee=2021, pib_courant=2502118000000.0, population=67697091),
            IndicateurMacro(annee=2022, pib_courant=2639092000000.0, population=68060207),
        ]
    )
    await db_session.commit()

    response = await async_client.get("/api/v1/indicateurs/historique?de=2021&a=2022")

    assert response.status_code == 200
    body = response.json()
    assert [item["annee"] for item in body] == [2021, 2022]
