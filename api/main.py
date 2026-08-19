"""Point d'entree de l'application FastAPI BudgetCitoyen."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException

from api.core.config import get_settings
from api.core.errors import (
    ProblemDetailException,
    http_exception_handler,
    problem_detail_exception_handler,
    unhandled_exception_handler,
)
from api.core.rate_limit import limiter
from api.core.security_headers import SecurityHeadersMiddleware
from api.routers import (
    budget,
    budget_perso,
    comparateur,
    depenses_fiscales,
    health,
    indicateurs,
    marches,
    missions,
    recettes,
)


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="BudgetCitoyen API",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
    )

    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]
    app.add_middleware(SlowAPIMiddleware)
    app.add_middleware(SecurityHeadersMiddleware)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.add_exception_handler(
        ProblemDetailException, problem_detail_exception_handler  # type: ignore[arg-type]
    )
    app.add_exception_handler(
        StarletteHTTPException, http_exception_handler  # type: ignore[arg-type]
    )
    app.add_exception_handler(Exception, unhandled_exception_handler)

    app.include_router(health.router)
    app.include_router(budget.router, prefix="/api/v1")
    app.include_router(recettes.router, prefix="/api/v1")
    app.include_router(missions.router, prefix="/api/v1")
    app.include_router(comparateur.router, prefix="/api/v1")
    app.include_router(budget_perso.router, prefix="/api/v1")
    app.include_router(marches.router, prefix="/api/v1")
    app.include_router(indicateurs.router, prefix="/api/v1")
    app.include_router(depenses_fiscales.router, prefix="/api/v1")

    return app


app = create_app()
