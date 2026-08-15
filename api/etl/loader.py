"""Chargement (upsert) des donnees normalisees en base (Phase 1)."""

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession


async def upsert_missions(db: AsyncSession, missions: list[dict[str, Any]]) -> None:
    """Insere ou met a jour les missions en base a partir de donnees normalisees."""
    raise NotImplementedError("Ingestion reelle - Phase 1")


async def upsert_depenses(db: AsyncSession, depenses: list[dict[str, Any]]) -> None:
    """Insere ou met a jour les depenses en base a partir de donnees normalisees."""
    raise NotImplementedError("Ingestion reelle - Phase 1")


async def upsert_recettes(db: AsyncSession, recettes: list[dict[str, Any]]) -> None:
    """Insere ou met a jour les recettes en base a partir de donnees normalisees."""
    raise NotImplementedError("Ingestion reelle - Phase 1")
