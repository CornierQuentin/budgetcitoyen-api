"""Test de la dependance FastAPI `get_db` (hors des overrides de test habituels)."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from api.db.deps import get_db


async def test_get_db_fournit_une_session_utilisable(db_engine: AsyncEngine) -> None:
    """`db_engine` garantit deja qu'on cible bien une base de test (voir conftest.py).
    Ce test exerce directement le generateur `get_db` (habituellement court-circuite
    par `app.dependency_overrides` dans les autres tests) pour verifier qu'il fournit
    une session fonctionnelle et la ferme correctement en fin de generateur."""
    generator = get_db()
    session = await anext(generator)
    try:
        result = await session.execute(text("SELECT 1"))
        assert result.scalar_one() == 1
    finally:
        await generator.aclose()
