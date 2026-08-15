"""Gestion des erreurs au format RFC 7807 (application/problem+json)."""

from __future__ import annotations

import logging

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)


class ProblemDetailException(Exception):
    """Exception applicative representant un probleme au sens RFC 7807."""

    def __init__(
        self,
        *,
        type: str = "about:blank",
        title: str,
        status: int,
        detail: str,
        instance: str | None = None,
    ) -> None:
        self.type = type
        self.title = title
        self.status = status
        self.detail = detail
        self.instance = instance
        super().__init__(detail)

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "type": self.type,
            "title": self.title,
            "status": self.status,
            "detail": self.detail,
        }
        if self.instance is not None:
            payload["instance"] = self.instance
        return payload


async def problem_detail_exception_handler(
    request: Request, exc: ProblemDetailException
) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status,
        content=exc.to_dict(),
        media_type="application/problem+json",
    )


async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
    payload = {
        "type": "about:blank",
        "title": detail,
        "status": exc.status_code,
        "detail": detail,
        "instance": str(request.url),
    }
    return JSONResponse(
        status_code=exc.status_code,
        content=payload,
        media_type="application/problem+json",
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Erreur non geree lors du traitement de la requete", exc_info=exc)
    payload = {
        "type": "about:blank",
        "title": "Erreur interne du serveur",
        "status": 500,
        "detail": "Une erreur inattendue est survenue.",
        "instance": str(request.url),
    }
    return JSONResponse(
        status_code=500,
        content=payload,
        media_type="application/problem+json",
    )
