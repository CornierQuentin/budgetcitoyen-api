"""Tests de l'endpoint de sante."""

from importlib.metadata import PackageNotFoundError

import pytest
from httpx import AsyncClient

from api.routers import health


async def test_health_retourne_200_et_status_ok(async_client: AsyncClient) -> None:
    response = await async_client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["derniere_ingestion"] is None


def test_get_version_retombe_sur_defaut_si_package_non_installe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repli documente: si les metadonnees du package sont absentes (ex: environnement
    de dev sans installation `pip install -e .`), `_get_version` ne doit pas lever."""

    def _version_absente(_name: str) -> str:
        raise PackageNotFoundError

    monkeypatch.setattr(health, "version", _version_absente)

    assert health._get_version() == "0.1.0"
