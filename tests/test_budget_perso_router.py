"""Tests du router /api/v1/budget-perso (Module 5: Budget personnalise)."""

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.action import Action
from api.models.annee_budget import AnneeBudget
from api.models.depense import Depense
from api.models.mission import Mission
from api.models.programme import Programme

ANNEE_REFERENCE = 2025


async def _seed_annee_avec_deux_missions(db_session: AsyncSession) -> None:
    """Seede une annee budgetaire avec deux missions (600/1000 et 400/1000).

    Les depenses par mission (Depense.cp) sont volontairement choisies de
    sorte que leur somme (1000) soit egale a AnneeBudget.depenses_nettes: cela
    permet de verifier que la ventilation par mission somme exactement au
    total (aucun ecart de perimetre entre les deux sources dans ce test).
    """
    db_session.add(
        AnneeBudget(
            annee=ANNEE_REFERENCE,
            depenses_nettes=1000.0,
            recettes_nettes=900.0,
            deficit=-100.0,
            dette_pib=None,
            source_url="https://example.test/annee",
        )
    )

    mission_justice = Mission(
        slug="justice",
        nom_normalise="justice",
        nom_officiel="Justice",
        annee=ANNEE_REFERENCE,
        code_mission="JA",
    )
    mission_education = Mission(
        slug="education",
        nom_normalise="education",
        nom_officiel="Enseignement scolaire",
        annee=ANNEE_REFERENCE,
        code_mission="EN",
    )
    db_session.add_all([mission_justice, mission_education])
    await db_session.flush()

    programme_justice = Programme(
        mission_id=mission_justice.id, code="JA-P1", nom="Programme Justice", annee=ANNEE_REFERENCE
    )
    programme_education = Programme(
        mission_id=mission_education.id,
        code="EN-P1",
        nom="Programme Education",
        annee=ANNEE_REFERENCE,
    )
    db_session.add_all([programme_justice, programme_education])
    await db_session.flush()

    action_justice = Action(
        programme_id=programme_justice.id, code="JA-A1", nom="Action Justice", annee=ANNEE_REFERENCE
    )
    action_education = Action(
        programme_id=programme_education.id,
        code="EN-A1",
        nom="Action Education",
        annee=ANNEE_REFERENCE,
    )
    db_session.add_all([action_justice, action_education])
    await db_session.flush()

    db_session.add_all(
        [
            Depense(action_id=action_justice.id, ae=600.0, cp=600.0, annee=ANNEE_REFERENCE),
            Depense(action_id=action_education.id, ae=400.0, cp=400.0, annee=ANNEE_REFERENCE),
        ]
    )
    await db_session.commit()


async def test_aucune_annee_budgetaire_disponible_retourne_404_rfc7807(
    async_client: AsyncClient,
) -> None:
    """Aucune donnee `annee_budget` en base: pas d'annee de reference pour la ventilation."""
    response = await async_client.get("/api/v1/budget-perso", params={"revenu_net": 2000})

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 404


async def test_depenses_nettes_nulles_donne_repartition_vide(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    """Si `depenses_nettes` de l'annee de reference est 0, la ventilation par mission
    (qui proratise sur ce total) reste une liste vide plutot qu'une division par zero."""
    db_session.add(
        AnneeBudget(
            annee=ANNEE_REFERENCE,
            depenses_nettes=0.0,
            recettes_nettes=0.0,
            deficit=0.0,
            dette_pib=None,
            source_url="https://example.test/annee",
        )
    )
    await db_session.commit()

    response = await async_client.get("/api/v1/budget-perso", params={"revenu_net": 2000})

    assert response.status_code == 200
    body = response.json()
    assert body["repartition"] == []
    assert body["contribution_totale_estimee"] > 0


async def test_parametre_manquant_retourne_422(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/budget-perso")

    assert response.status_code == 422


async def test_revenu_negatif_retourne_422(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/budget-perso", params={"revenu_net": -100})

    assert response.status_code == 422


async def test_revenu_nul_donne_ir_et_tva_nuls(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_annee_avec_deux_missions(db_session)

    response = await async_client.get("/api/v1/budget-perso", params={"revenu_net": 0})

    assert response.status_code == 200
    body = response.json()
    assert body["ir_estime"] == 0.0
    assert body["tva_estimee"] == 0.0
    assert body["contribution_totale_estimee"] == 0.0
    assert sum(item["montant"] for item in body["repartition"]) == pytest.approx(0.0)


async def test_revenu_2500_produit_des_montants_plausibles_et_une_repartition_coherente(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_annee_avec_deux_missions(db_session)

    response = await async_client.get("/api/v1/budget-perso", params={"revenu_net": 2500})

    assert response.status_code == 200
    body = response.json()

    assert body["revenu_net_mensuel"] == 2500.0
    assert body["annee_reference"] == ANNEE_REFERENCE

    # IR attendu: revenu net annuel 30 000, abattement 10% -> imposable
    # 27 000; bareme 2026 (1 part): 11 600 @ 0% puis 15 400 @ 11% = 1 694.
    assert body["ir_estime"] == pytest.approx(1694.0)
    # Plausible pour ce niveau de revenu (ordre de grandeur: quelques
    # centaines a ~2 000 euros), certainement pas nul ni aberrant (ex: > revenu).
    assert 0 < body["ir_estime"] < 3000

    # TVA attendue: consommation 30 000 * (1 - 0.179) = 24 630, * 9.7% = 2 389.11.
    assert body["tva_estimee"] == pytest.approx(2389.11, abs=0.5)
    assert body["tva_estimee"] > 0

    assert body["contribution_totale_estimee"] == pytest.approx(
        body["ir_estime"] + body["tva_estimee"]
    )

    repartition = body["repartition"]
    assert len(repartition) == 2
    slugs = {item["mission_slug"] for item in repartition}
    assert slugs == {"justice", "education"}

    somme_repartition = sum(item["montant"] for item in repartition)
    assert somme_repartition == pytest.approx(body["contribution_totale_estimee"], abs=0.01)

    justice = next(item for item in repartition if item["mission_slug"] == "justice")
    assert justice["montant"] == pytest.approx(body["contribution_totale_estimee"] * 0.6, abs=0.01)

    # Methodologie affichee explicitement (CDC 1.3: transparence/verifiabilite).
    methodologie = body["methodologie"]
    assert len(methodologie["hypotheses"]) > 0
    assert len(methodologie["limites"]) > 0
    assert len(methodologie["sources"]) > 0
    for source in methodologie["sources"]:
        assert source["url"].startswith("https://")
