"""Dependances FastAPI liees a la base de donnees."""

from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api.db.session import async_session_maker


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Fournit une session de base de donnees pour la duree d'une requete."""
    async with async_session_maker() as session:
        yield session


# Singleton de dependance (evite l'appel repete de Depends() dans les defauts
# d'arguments des routers, cf. regle ruff B008).
DbSession = Annotated[AsyncSession, Depends(get_db)]
