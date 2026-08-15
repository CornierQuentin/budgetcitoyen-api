"""Configuration Alembic (pattern async officiel), URL injectee depuis Settings."""

import asyncio
from logging.config import fileConfig

from sqlalchemy import Connection, pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context
from api.core.config import get_settings
from api.models import Base

# Objet de configuration Alembic, fournit l'acces aux valeurs du fichier .ini utilise.
config = context.config

# Injection de l'URL de connexion depuis Settings (jamais de credentials en clair dans alembic.ini).
settings = get_settings()
config.set_main_option("sqlalchemy.url", settings.database_url)

# Interpretation du fichier de config pour le logging Python.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Metadata cible pour le support 'autogenerate'.
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Execute les migrations en mode 'offline' (genere du SQL sans connexion)."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Cree un moteur async et l'associe a un contexte de migration."""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Execute les migrations en mode 'online' (via une connexion async)."""
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
