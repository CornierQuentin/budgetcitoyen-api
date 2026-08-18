"""En-tetes de securite HTTP (cahier des charges, section 9).

HSTS / X-Frame-Options / X-Content-Type-Options s'appliquent a toutes les
reponses. La CSP stricte ("default-src 'none'", adaptee a une API qui ne
sert que du JSON) est elle exemptee sur /api/docs et /api/redoc : ces deux
routes servent une vraie page HTML (Swagger UI / Redoc) qui charge son
JS/CSS depuis un CDN externe (comportement par defaut de FastAPI) - une
CSP stricte y casserait la documentation interactive sans gain de securite
correspondant (ces pages ne traitent aucune donnee utilisateur sensible).
"""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

_DOCS_PATHS = ("/api/docs", "/api/redoc")

_HSTS_VALUE = "max-age=63072000; includeSubDomains"
_CSP_VALUE = "default-src 'none'; frame-ancestors 'none'"


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        response.headers["Strict-Transport-Security"] = _HSTS_VALUE
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        if not request.url.path.startswith(_DOCS_PATHS):
            response.headers["Content-Security-Policy"] = _CSP_VALUE
        return response
