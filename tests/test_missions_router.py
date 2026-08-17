"""Tests du router /api/v1/missions."""

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.action import Action
from api.models.depense import Depense
from api.models.mission import Mission
from api.models.programme import Programme

ANNEE_REFERENCE = 2024


async def _seed_mission_justice(db_session: AsyncSession) -> None:
    """Seede une mission Justice avec deux programmes et deux actions par programme.

    Montants choisis pour verifier facilement l'agregation:
    - Programme JA-P1: actions a 100 et 50 -> 150
    - Programme JA-P2: action a 30 -> 30
    - Mission Justice: 150 + 30 = 180
    """
    mission = Mission(
        slug="justice",
        nom_normalise="justice",
        nom_officiel="Justice",
        annee=ANNEE_REFERENCE,
        code_mission="JA",
    )
    db_session.add(mission)
    await db_session.flush()

    programme_1 = Programme(
        mission_id=mission.id, code="JA-P1", nom="Programme Justice 1", annee=ANNEE_REFERENCE
    )
    programme_2 = Programme(
        mission_id=mission.id, code="JA-P2", nom="Programme Justice 2", annee=ANNEE_REFERENCE
    )
    db_session.add_all([programme_1, programme_2])
    await db_session.flush()

    action_1a = Action(
        programme_id=programme_1.id, code="JA-A1", nom="Action Justice 1a", annee=ANNEE_REFERENCE
    )
    action_1b = Action(
        programme_id=programme_1.id, code="JA-A2", nom="Action Justice 1b", annee=ANNEE_REFERENCE
    )
    action_2a = Action(
        programme_id=programme_2.id, code="JA-A3", nom="Action Justice 2a", annee=ANNEE_REFERENCE
    )
    db_session.add_all([action_1a, action_1b, action_2a])
    await db_session.flush()

    db_session.add_all(
        [
            Depense(action_id=action_1a.id, ae=100.0, cp=100.0, annee=ANNEE_REFERENCE),
            Depense(action_id=action_1b.id, ae=50.0, cp=50.0, annee=ANNEE_REFERENCE),
            Depense(action_id=action_2a.id, ae=30.0, cp=30.0, annee=ANNEE_REFERENCE),
        ]
    )
    await db_session.commit()


async def test_liste_vide_retourne_200(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/missions")

    assert response.status_code == 200
    assert response.json() == []


async def test_ressource_inexistante_retourne_404_rfc7807(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/missions/mission-inexistante")

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 404


async def test_liste_missions_inclut_montant_total(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_mission_justice(db_session)

    response = await async_client.get("/api/v1/missions", params={"annee": ANNEE_REFERENCE})

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["slug"] == "justice"
    assert body[0]["montant_total"] == 180.0


async def test_obtenir_mission_inclut_montant_total(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_mission_justice(db_session)

    response = await async_client.get("/api/v1/missions/justice", params={"annee": ANNEE_REFERENCE})

    assert response.status_code == 200
    body = response.json()
    assert body["montant_total"] == 180.0


async def test_detail_mission_retourne_decomposition_programmes_actions(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_mission_justice(db_session)

    response = await async_client.get(
        "/api/v1/missions/justice/detail", params={"annee": ANNEE_REFERENCE}
    )

    assert response.status_code == 200
    body = response.json()

    assert body["slug"] == "justice"
    assert body["annee"] == ANNEE_REFERENCE
    assert body["montant_total"] == 180.0
    assert len(body["programmes"]) == 2

    programme_1 = next(p for p in body["programmes"] if p["code"] == "JA-P1")
    assert programme_1["montant_total"] == 150.0
    assert len(programme_1["actions"]) == 2
    codes_actions_p1 = {a["code"] for a in programme_1["actions"]}
    assert codes_actions_p1 == {"JA-A1", "JA-A2"}
    for action in programme_1["actions"]:
        assert action["ae"] > 0
        assert action["cp"] > 0

    programme_2 = next(p for p in body["programmes"] if p["code"] == "JA-P2")
    assert programme_2["montant_total"] == 30.0
    assert len(programme_2["actions"]) == 1


async def test_detail_mission_sans_annee_utilise_la_derniere_disponible(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_mission_justice(db_session)

    response = await async_client.get("/api/v1/missions/justice/detail")

    assert response.status_code == 200
    body = response.json()
    assert body["annee"] == ANNEE_REFERENCE
    assert body["montant_total"] == 180.0


async def test_detail_mission_inexistante_retourne_404_rfc7807(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/missions/mission-inexistante/detail")

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 404
