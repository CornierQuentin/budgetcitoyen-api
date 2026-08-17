"""Tests du decorateur de cache Redis (`api.cache.decorators.cached`).

Utilise l'instance Redis reelle du service `redis` (docker-compose), le meme
que celui vise par `REDIS_URL` en environnement de test/dev: comme ce
decorateur n'est pas encore branche sur un endpoint applicatif, les cles
qu'il pose sont prefixees par le nom qualifie de la fonction de test
elle-meme (voir l'implementation) et explicitement nettoyees apres chaque
test pour ne rien laisser trainer.
"""

from collections.abc import AsyncGenerator

import pytest_asyncio

from api.cache.decorators import cached
from api.cache.redis_client import redis_client


@pytest_asyncio.fixture(autouse=True)
async def _nettoyage_cles_cache() -> AsyncGenerator[None, None]:
    yield
    async for key in redis_client.scan_iter(match="tests.test_cache_decorators.*"):
        await redis_client.delete(key)
    # `redis_client` est un singleton module-level dont le pool de connexions
    # est lie a l'event loop courant; pytest-asyncio cree un event loop par
    # test, donc sans fermeture explicite ici, la connexion tentera d'etre
    # reutilisee sur le prochain event loop (deja ferme) et echouera avec
    # "Event loop is closed" sur le test suivant.
    await redis_client.aclose()


async def test_cached_met_en_cache_le_resultat_et_evite_un_second_appel() -> None:
    appels = []

    @cached(ttl=60)
    async def fonction_couteuse(x: int) -> int:
        appels.append(x)
        return x * 2

    premier = await fonction_couteuse(21)
    second = await fonction_couteuse(21)

    assert premier == 42
    assert second == 42
    # Le second appel doit avoir servi le cache, pas re-execute la fonction.
    assert appels == [21]


async def test_cached_distingue_les_appels_par_arguments() -> None:
    @cached(ttl=60)
    async def double(x: int) -> int:
        return x * 2

    assert await double(1) == 2
    assert await double(2) == 4


async def test_cached_pose_bien_le_ttl_fourni_sur_la_cle() -> None:
    @cached(ttl=60)
    async def fonction(x: int) -> int:
        return x

    await fonction(7)

    cles = [key async for key in redis_client.scan_iter(match="tests.test_cache_decorators.*")]
    assert len(cles) == 1
    ttl_restant = await redis_client.ttl(cles[0])
    assert 0 < ttl_restant <= 60
