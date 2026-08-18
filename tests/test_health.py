"""Tests de l'endpoint de sante."""

from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.ingestion_log import IngestionLog
from api.routers import health


async def test_health_retourne_200_et_status_ok(async_client: AsyncClient) -> None:
    response = await async_client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["derniere_ingestion"] is None


async def test_health_reflete_la_derniere_ingestion_enregistree(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    plus_ancienne = datetime(2026, 1, 1, tzinfo=UTC)
    plus_recente = datetime(2026, 6, 15, 10, 30, tzinfo=UTC)
    db_session.add_all(
        [IngestionLog(termine_a=plus_ancienne), IngestionLog(termine_a=plus_recente)]
    )
    await db_session.commit()

    response = await async_client.get("/health")

    assert response.json()["derniere_ingestion"] == plus_recente.isoformat()


def test_get_version_retombe_sur_defaut_si_package_non_installe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repli documente: si les metadonnees du package sont absentes (ex: environnement
    de dev sans installation `pip install -e .`), `_get_version` ne doit pas lever."""

    def _version_absente(_name: str) -> str:
        raise PackageNotFoundError

    monkeypatch.setattr(health, "version", _version_absente)

    assert health._get_version() == "0.1.0"
