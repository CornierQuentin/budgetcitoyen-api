"""Tests du format d'erreur RFC 7807 (application/problem+json)."""

from httpx import AsyncClient
from starlette.requests import Request

from api.core.errors import (
    ProblemDetailException,
    http_exception_handler,
    unhandled_exception_handler,
)
from api.schemas.error import ProblemDetail


def _fake_request(path: str = "/api/v1/inconnu") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
        "query_string": b"",
        "scheme": "http",
        "server": ("test", 80),
    }
    return Request(scope)


def test_problem_detail_exception_to_dict_omet_instance_par_defaut() -> None:
    exc = ProblemDetailException(title="Titre", status=404, detail="Detail")

    payload = exc.to_dict()

    assert payload == {
        "type": "about:blank",
        "title": "Titre",
        "status": 404,
        "detail": "Detail",
    }
    assert "instance" not in payload


def test_problem_detail_exception_to_dict_inclut_instance_si_fournie() -> None:
    exc = ProblemDetailException(
        title="Titre", status=404, detail="Detail", instance="/api/v1/missions/x"
    )

    payload = exc.to_dict()

    assert payload["instance"] == "/api/v1/missions/x"


def test_problem_detail_schema_valeurs_par_defaut() -> None:
    problem = ProblemDetail(title="Titre", status=400, detail="Detail")

    assert problem.type == "about:blank"
    assert problem.instance is None


async def test_route_inexistante_retourne_404_rfc7807_via_http_exception_handler(
    async_client: AsyncClient,
) -> None:
    """Une route qui ne matche aucun router passe par le handler HTTPException
    standard de Starlette (pas ProblemDetailException), doit rester RFC7807."""
    response = await async_client.get("/api/v1/route-totalement-inconnue")

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 404
    assert body["type"] == "about:blank"
    assert body["instance"] is not None


async def test_http_exception_handler_directement() -> None:
    from starlette.exceptions import HTTPException

    request = _fake_request()
    exc = HTTPException(status_code=404, detail="Not Found")

    response = await http_exception_handler(request, exc)

    assert response.status_code == 404
    assert response.media_type == "application/problem+json"


async def test_unhandled_exception_handler_retourne_500_rfc7807() -> None:
    """Toute exception non geree doit se traduire par une 500 au format RFC7807,
    sans jamais exposer le detail de l'exception interne (message generique)."""
    request = _fake_request()
    exc = RuntimeError("boom - detail interne sensible")

    response = await unhandled_exception_handler(request, exc)

    assert response.status_code == 500
    assert response.media_type == "application/problem+json"
    body = response.body.decode()
    assert "boom" not in body
    assert "Une erreur inattendue est survenue." in body
