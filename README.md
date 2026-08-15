# BudgetCitoyen API

Backend API REST du projet open source [BudgetCitoyen.fr](https://budgetcitoyen.fr), qui rend le
budget de l'État français explorable par toutes et tous.

## Stack technique

- Python 3.12
- FastAPI
- SQLAlchemy 2 (async) + asyncpg
- Alembic (migrations)
- Pydantic v2 / pydantic-settings
- PostgreSQL 16
- Redis (cache, via `redis.asyncio`)
- Pandas (traitement des jeux de données ETL)
- SlowAPI (rate limiting)
- Uvicorn
- Ruff, Black, mypy (strict)
- Pytest, pytest-asyncio, httpx

## Démarrage local

Ce dépôt fait partie d'un workspace plus large. Pour démarrer l'environnement complet
(API + PostgreSQL + Redis), depuis la racine du workspace parent :

```bash
docker compose up
```

L'API est alors disponible sur `http://localhost:8000`.

- Documentation interactive (Swagger UI) : `http://localhost:8000/api/docs`
- Documentation ReDoc : `http://localhost:8000/api/redoc`

## Licence

Ce projet est distribué sous licence [AGPL-3.0-or-later](./LICENSE).
