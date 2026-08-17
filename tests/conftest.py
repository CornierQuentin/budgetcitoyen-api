"""Fixtures pytest partagees: client HTTP async et base de donnees de test."""

from collections.abc import AsyncGenerator

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from api.core.config import get_settings
from api.db.deps import get_db
from api.main import app
from api.models import Base


@pytest_asyncio.fixture
async def db_engine() -> AsyncGenerator[AsyncEngine, None]:
    """Cree les tables sur la base Postgres reelle definie par DATABASE_URL, puis nettoie.

    NullPool: pytest-asyncio cree un event loop distinct par test; un pool par
    defaut garderait des connexions asyncpg attachees a un loop deja ferme.
    """
    settings = get_settings()
    db_name = settings.database_url.rsplit("/", 1)[-1]
    if "test" not in db_name.lower():
        raise RuntimeError(
            f"DATABASE_URL pointe vers '{db_name}', qui ne contient pas 'test'. "
            "Ce fixture fait un DROP de toutes les tables en teardown : lancer les "
            "tests contre la base de dev (docker-compose, 'budgetcitoyen') detruirait "
            "les vraies donnees ingerees. Utilise une base dediee, ex. "
            "'budgetcitoyen_test' (meme convention que la CI, voir .github/workflows/ci.yml)."
        )
    engine = create_async_engine(settings.database_url, poolclass=NullPool)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine: AsyncEngine) -> AsyncGenerator[AsyncSession, None]:
    """Session SQLAlchemy sur la meme base de test que `async_client` (meme engine).

    Permet de seeder des donnees (Mission/Programme/Action/Depense/Recette/...)
    avant d'appeler l'API via `async_client`: les deux fixtures partagent le
    meme `db_engine` (donc la meme base Postgres de test), seul le
    `async_sessionmaker` est recree ici. Les tests doivent `commit()`
    explicitement apres insertion.
    """
    session_maker = async_sessionmaker(db_engine, expire_on_commit=False)
    async with session_maker() as session:
        yield session


@pytest_asyncio.fixture
async def async_client(db_engine: AsyncEngine) -> AsyncGenerator[AsyncClient, None]:
    """Client HTTP asynchrone branche sur l'application, avec la DB de test injectee."""
    session_maker = async_sessionmaker(db_engine, expire_on_commit=False)

    async def _get_test_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_maker() as session:
            yield session

    app.dependency_overrides[get_db] = _get_test_db

    # raise_app_exceptions=False: une exception non geree doit se traduire par
    # la reponse 500 RFC7807 (cf. unhandled_exception_handler), pas par une
    # propagation de l'exception dans le test (comportement par defaut de
    # Starlette ServerErrorMiddleware, qui re-leve apres avoir genere la reponse).
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    app.dependency_overrides.clear()
