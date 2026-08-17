"""Tests du router /api/v1/comparateur.

Ce router ne renvoie pas de liste (il compare deux annees precises), les tests
adaptent donc le pattern standard: un cas de parametres manquants (422) et un
cas d'annee inexistante (404 RFC7807).
"""

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.action import Action
from api.models.annee_budget import AnneeBudget
from api.models.depense import Depense
from api.models.mission import Mission
from api.models.programme import Programme
from api.models.recette import Recette, TypeRecette


async def test_parametres_manquants_retourne_422(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/comparateur")

    assert response.status_code == 422


async def test_ressource_inexistante_retourne_404_rfc7807(async_client: AsyncClient) -> None:
    response = await async_client.get(
        "/api/v1/comparateur", params={"annee_a": 2099, "annee_b": 2098}
    )

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 404


async def test_happy_path_retourne_deltas_missions_et_recettes(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    db_session.add_all(
        [
            AnneeBudget(
                annee=2024,
                depenses_nettes=300.0,
                recettes_nettes=250.0,
                deficit=-50.0,
                dette_pib=None,
                source_url="https://example.test/2024",
            ),
            AnneeBudget(
                annee=2025,
                depenses_nettes=320.0,
                recettes_nettes=260.0,
                deficit=-60.0,
                dette_pib=None,
                source_url="https://example.test/2025",
            ),
        ]
    )

    mission_2024 = Mission(
        slug="justice",
        nom_normalise="justice",
        nom_officiel="Justice",
        annee=2024,
        code_mission="JA",
    )
    mission_2025 = Mission(
        slug="justice",
        nom_normalise="justice",
        nom_officiel="Justice",
        annee=2025,
        code_mission="JA",
    )
    db_session.add_all([mission_2024, mission_2025])
    await db_session.flush()

    programme_2024 = Programme(
        mission_id=mission_2024.id, code="JA-P1", nom="Programme", annee=2024
    )
    programme_2025 = Programme(
        mission_id=mission_2025.id, code="JA-P1", nom="Programme", annee=2025
    )
    db_session.add_all([programme_2024, programme_2025])
    await db_session.flush()

    action_2024 = Action(programme_id=programme_2024.id, code="JA-A1", nom="Action", annee=2024)
    action_2025 = Action(programme_id=programme_2025.id, code="JA-A1", nom="Action", annee=2025)
    db_session.add_all([action_2024, action_2025])
    await db_session.flush()

    db_session.add_all(
        [
            Depense(action_id=action_2024.id, ae=100.0, cp=100.0, annee=2024),
            Depense(action_id=action_2025.id, ae=120.0, cp=120.0, annee=2025),
            Recette(annee=2024, type=TypeRecette.IR, montant_brut=90.0, montant_net=85.0),
            Recette(annee=2024, type=TypeRecette.TVA, montant_brut=180.0, montant_net=175.0),
            Recette(annee=2024, type=TypeRecette.IS, montant_brut=60.0, montant_net=58.0),
            Recette(annee=2024, type=TypeRecette.TICPE, montant_brut=30.0, montant_net=29.0),
            Recette(annee=2024, type=TypeRecette.AUTRES, montant_brut=10.0, montant_net=9.0),
            Recette(annee=2025, type=TypeRecette.IR, montant_brut=95.0, montant_net=90.0),
            Recette(annee=2025, type=TypeRecette.TVA, montant_brut=185.0, montant_net=180.0),
            Recette(annee=2025, type=TypeRecette.IS, montant_brut=62.0, montant_net=60.0),
            Recette(annee=2025, type=TypeRecette.TICPE, montant_brut=31.0, montant_net=30.0),
            Recette(annee=2025, type=TypeRecette.AUTRES, montant_brut=11.0, montant_net=10.0),
        ]
    )
    await db_session.commit()

    response = await async_client.get(
        "/api/v1/comparateur", params={"annee_a": 2024, "annee_b": 2025}
    )

    assert response.status_code == 200
    body = response.json()

    assert body["ecart_depenses"] == 20.0
    assert body["ecart_recettes"] == 10.0

    missions = body["missions"]
    assert len(missions) == 1
    assert missions[0]["slug"] == "justice"
    assert missions[0]["montant_a"] == 100.0
    assert missions[0]["montant_b"] == 120.0
    assert missions[0]["delta_absolu"] == 20.0

    recettes = body["recettes"]
    types_recettes = {r["type"] for r in recettes}
    assert types_recettes == {"IR", "TVA", "IS", "TICPE", "AUTRES"}
    ir = next(r for r in recettes if r["type"] == "IR")
    assert ir["montant_a"] == 85.0
    assert ir["montant_b"] == 90.0
    assert ir["delta_absolu"] == 5.0
