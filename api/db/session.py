"""Configuration du moteur SQLAlchemy async et de la fabrique de sessions."""

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from api.core.config import get_settings

settings = get_settings()

engine: AsyncEngine = create_async_engine(settings.database_url)

async_session_maker = async_sessionmaker(engine, expire_on_commit=False)
