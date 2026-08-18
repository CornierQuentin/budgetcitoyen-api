"""Tests des en-tetes de securite HTTP (cahier des charges, section 9)."""

from httpx import AsyncClient

_CSP_VALUE = "default-src 'none'; frame-ancestors 'none'"


async def test_reponse_api_porte_les_en_tetes_de_securite(async_client: AsyncClient) -> None:
    response = await async_client.get("/health")

    assert response.headers["Strict-Transport-Security"] == "max-age=63072000; includeSubDomains"
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Content-Security-Policy"] == _CSP_VALUE


async def test_erreur_404_porte_aussi_les_en_tetes_de_securite(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/route-inexistante")

    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["Content-Security-Policy"] == _CSP_VALUE


async def test_documentation_swagger_est_exemptee_de_la_csp(async_client: AsyncClient) -> None:
    """La CSP stricte casserait Swagger UI (JS/CSS charges depuis un CDN externe)."""
    response = await async_client.get("/api/docs")

    assert "Content-Security-Policy" not in response.headers
    # Les autres en-tetes restent appliques : rien ne justifie de les
    # retirer aussi sur cette route.
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["Strict-Transport-Security"] == "max-age=63072000; includeSubDomains"


async def test_documentation_redoc_est_exemptee_de_la_csp(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/redoc")

    assert "Content-Security-Policy" not in response.headers
