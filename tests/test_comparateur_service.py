"""Tests de la logique metier du comparateur (agregation par mission et par recette)."""

from sqlalchemy.ext.asyncio import AsyncSession

from api.models.action import Action
from api.models.annee_budget import AnneeBudget
from api.models.depense import Depense
from api.models.mission import Mission
from api.models.programme import Programme
from api.services.comparateur_service import comparer_annees
from api.services.mission_service import totaux_depenses_par_mission


async def _seed_mission_depense(
    db: AsyncSession,
    *,
    slug: str,
    nom_officiel: str,
    annee: int,
    cp_montants: list[float],
) -> None:
    """Insere une mission avec un programme/une action par montant CP fourni."""
    mission = Mission(
        slug=slug,
        nom_normalise=slug,
        nom_officiel=nom_officiel,
        annee=annee,
        code_mission=None,
    )
    db.add(mission)
    await db.flush()

    for i, cp in enumerate(cp_montants):
        programme = Programme(
            mission_id=mission.id, code=f"{slug}-P{i}", nom=f"Programme {i}", annee=annee
        )
        db.add(programme)
        await db.flush()

        action = Action(
            programme_id=programme.id, code=f"{slug}-A{i}", nom=f"Action {i}", annee=annee
        )
        db.add(action)
        await db.flush()

        db.add(Depense(action_id=action.id, ae=cp, cp=cp, annee=annee))

    await db.flush()


async def _seed_annee_budget(db: AsyncSession, annee: int) -> None:
    db.add(
        AnneeBudget(
            annee=annee,
            depenses_nettes=0.0,
            recettes_nettes=0.0,
            deficit=0.0,
            dette_pib=None,
            source_url="https://example.test/source",
        )
    )


async def test_totaux_depenses_par_mission_agrege_les_actions(db_session: AsyncSession) -> None:
    await _seed_mission_depense(
        db_session, slug="justice", nom_officiel="Justice", annee=2024, cp_montants=[100.0, 50.0]
    )
    await db_session.commit()

    totaux = await totaux_depenses_par_mission(db_session, 2024)

    assert totaux["justice"] == ("Justice", 150.0)


async def test_comparer_annees_missions_triees_par_delta_absolu_decroissant(
    db_session: AsyncSession,
) -> None:
    await _seed_annee_budget(db_session, 2024)
    await _seed_annee_budget(db_session, 2025)
    # Justice: 100 -> 200 (delta +100, plus gros delta absolu positif)
    await _seed_mission_depense(
        db_session, slug="justice", nom_officiel="Justice", annee=2024, cp_montants=[100.0]
    )
    await _seed_mission_depense(
        db_session, slug="justice", nom_officiel="Justice", annee=2025, cp_montants=[200.0]
    )
    # Defense: 500 -> 490 (delta -10)
    await _seed_mission_depense(
        db_session, slug="defense", nom_officiel="Defense", annee=2024, cp_montants=[500.0]
    )
    await _seed_mission_depense(
        db_session, slug="defense", nom_officiel="Defense", annee=2025, cp_montants=[490.0]
    )
    await db_session.commit()

    reponse = await comparer_annees(db_session, 2024, 2025)

    slugs_tries = [m.slug for m in reponse.missions]
    assert slugs_tries == ["justice", "defense"]


async def test_comparer_annees_delta_relatif_none_si_montant_a_nul(
    db_session: AsyncSession,
) -> None:
    await _seed_annee_budget(db_session, 2024)
    await _seed_annee_budget(db_session, 2025)
    # Mission nouvelle en 2025, absente en 2024: montant_a == 0.
    await _seed_mission_depense(
        db_session,
        slug="nouvelle-mission",
        nom_officiel="Nouvelle Mission",
        annee=2025,
        cp_montants=[80.0],
    )
    await db_session.commit()

    reponse = await comparer_annees(db_session, 2024, 2025)

    item = next(m for m in reponse.missions if m.slug == "nouvelle-mission")
    assert item.montant_a == 0.0
    assert item.montant_b == 80.0
    assert item.delta_absolu == 80.0
    assert item.delta_relatif_pct is None


async def test_comparer_annees_mission_absente_annee_b_montant_zero(
    db_session: AsyncSession,
) -> None:
    await _seed_annee_budget(db_session, 2024)
    await _seed_annee_budget(db_session, 2025)
    # Mission presente uniquement en 2024 (ex: supprimee/fusionnee en 2025).
    await _seed_mission_depense(
        db_session,
        slug="mission-disparue",
        nom_officiel="Mission disparue",
        annee=2024,
        cp_montants=[60.0],
    )
    await db_session.commit()

    reponse = await comparer_annees(db_session, 2024, 2025)

    item = next(m for m in reponse.missions if m.slug == "mission-disparue")
    assert item.montant_a == 60.0
    assert item.montant_b == 0.0
    assert item.delta_absolu == -60.0
    assert item.delta_relatif_pct == -100.0
