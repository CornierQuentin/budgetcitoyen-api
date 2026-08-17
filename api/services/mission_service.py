"""Logique metier liee aux missions, programmes et actions budgetaires."""

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.core.errors import ProblemDetailException
from api.models.action import Action
from api.models.depense import Depense
from api.models.mission import Mission
from api.models.programme import Programme
from api.schemas.mission import ActionDetailItem, MissionDetailResponse, ProgrammeDetailItem


async def lister_missions(db: AsyncSession, annee: int | None) -> list[tuple[Mission, float]]:
    """Retourne les missions (filtrees eventuellement par annee) avec leur montant total CP.

    Pour eviter un N+1 (une requete de total par mission), les totaux sont
    calcules une seule fois par annee distincte presente dans le resultat
    (via `totaux_depenses_par_mission`), puis associes aux missions en memoire.
    """
    stmt = select(Mission).order_by(Mission.nom_officiel)
    if annee is not None:
        stmt = stmt.where(Mission.annee == annee)
    result = await db.execute(stmt)
    missions = list(result.scalars().all())

    totaux_par_annee: dict[int, dict[str, tuple[str, float]]] = {}
    for a in {m.annee for m in missions}:
        totaux_par_annee[a] = await totaux_depenses_par_mission(db, a)

    return [
        (
            mission,
            totaux_par_annee[mission.annee].get(mission.slug, (mission.nom_officiel, 0.0))[1],
        )
        for mission in missions
    ]


async def obtenir_mission(db: AsyncSession, slug: str, annee: int | None) -> tuple[Mission, float]:
    """Retourne une mission par slug (et annee optionnelle) avec son montant total CP.

    Leve une 404 RFC7807 si la mission est absente.
    """
    stmt = select(Mission).where(Mission.slug == slug)
    if annee is not None:
        stmt = stmt.where(Mission.annee == annee)
    stmt = stmt.order_by(Mission.annee.desc())
    result = await db.execute(stmt)
    mission = result.scalars().first()
    if mission is None:
        raise ProblemDetailException(
            title="Mission introuvable",
            status=404,
            detail=f"Aucune mission trouvee pour le slug '{slug}'.",
        )

    stmt_total = (
        select(func.coalesce(func.sum(Depense.cp), 0.0))
        .select_from(Programme)
        .join(Action, Action.programme_id == Programme.id)
        .join(Depense, Depense.action_id == Action.id)
        .where(Programme.mission_id == mission.id)
    )
    result_total = await db.execute(stmt_total)
    montant_total = float(result_total.scalar_one())

    return mission, montant_total


async def obtenir_mission_detail(
    db: AsyncSession, slug: str, annee: int | None
) -> MissionDetailResponse:
    """Retourne une mission avec sa decomposition programmes -> actions et leurs montants.

    Si `annee` est None, utilise la derniere annee disponible pour ce slug
    (comportement coherent avec `obtenir_mission`, ou `annee` est egalement
    optionnel). Leve une 404 RFC7807 si la mission n'existe pas du tout (aucune
    annee trouvee pour ce slug, ou aucune mission pour le couple slug/annee
    demande). La decomposition est recuperee en une seule requete jointe
    (Programme -> Action -> Depense), les totaux etant ensuite agreges en
    Python plutot que par des allers-retours DB supplementaires.
    """
    if annee is None:
        stmt_derniere_annee = select(func.max(Mission.annee)).where(Mission.slug == slug)
        result_derniere_annee = await db.execute(stmt_derniere_annee)
        annee = result_derniere_annee.scalar_one_or_none()

    if annee is None:
        raise ProblemDetailException(
            title="Mission introuvable",
            status=404,
            detail=f"Aucune mission trouvee pour le slug '{slug}'.",
        )

    stmt_mission = select(Mission).where(Mission.slug == slug, Mission.annee == annee)
    result_mission = await db.execute(stmt_mission)
    mission = result_mission.scalars().first()
    if mission is None:
        raise ProblemDetailException(
            title="Mission introuvable",
            status=404,
            detail=f"Aucune mission trouvee pour le slug '{slug}' et l'annee {annee}.",
        )

    stmt_decomposition = (
        select(
            Programme.id,
            Programme.code,
            Programme.nom,
            Action.id,
            Action.code,
            Action.nom,
            func.coalesce(func.sum(Depense.ae), 0.0),
            func.coalesce(func.sum(Depense.cp), 0.0),
        )
        .select_from(Programme)
        .join(Action, Action.programme_id == Programme.id)
        .outerjoin(Depense, Depense.action_id == Action.id)
        .where(Programme.mission_id == mission.id)
        .group_by(Programme.id, Programme.code, Programme.nom, Action.id, Action.code, Action.nom)
        .order_by(Programme.code, Action.code)
    )
    result_decomposition = await db.execute(stmt_decomposition)

    programmes_actions: dict[int, tuple[str, str, list[ActionDetailItem]]] = {}
    ordre_programmes: list[int] = []
    montant_total_mission = 0.0

    for prog_id, prog_code, prog_nom, act_id, act_code, act_nom, ae, cp in result_decomposition:
        ae_f, cp_f = float(ae), float(cp)
        montant_total_mission += cp_f

        if prog_id not in programmes_actions:
            programmes_actions[prog_id] = (prog_code, prog_nom, [])
            ordre_programmes.append(prog_id)

        programmes_actions[prog_id][2].append(
            ActionDetailItem(id=act_id, code=act_code, nom=act_nom, ae=ae_f, cp=cp_f)
        )

    programmes = [
        ProgrammeDetailItem(
            id=prog_id,
            code=programmes_actions[prog_id][0],
            nom=programmes_actions[prog_id][1],
            montant_total=sum(a.cp for a in programmes_actions[prog_id][2]),
            actions=programmes_actions[prog_id][2],
        )
        for prog_id in ordre_programmes
    ]

    return MissionDetailResponse(
        id=mission.id,
        slug=mission.slug,
        nom_officiel=mission.nom_officiel,
        annee=mission.annee,
        montant_total=montant_total_mission,
        programmes=programmes,
    )


async def historique_mission(
    db: AsyncSession, slug: str, de: int | None, a: int | None
) -> list[Mission]:
    """Retourne l'historique d'une mission (par slug) sur une plage d'annees."""
    stmt = select(Mission).where(Mission.slug == slug).order_by(Mission.annee)
    if de is not None:
        stmt = stmt.where(Mission.annee >= de)
    if a is not None:
        stmt = stmt.where(Mission.annee <= a)
    result = await db.execute(stmt)
    return list(result.scalars().all())


async def obtenir_programme(db: AsyncSession, programme_id: int, annee: int | None) -> Programme:
    """Retourne un programme par id (et annee optionnelle), ou 404 si absent."""
    stmt = select(Programme).where(Programme.id == programme_id)
    if annee is not None:
        stmt = stmt.where(Programme.annee == annee)
    result = await db.execute(stmt)
    programme = result.scalar_one_or_none()
    if programme is None:
        raise ProblemDetailException(
            title="Programme introuvable",
            status=404,
            detail=f"Aucun programme trouve pour l'id {programme_id}.",
        )
    return programme


async def totaux_depenses_par_mission(db: AsyncSession, annee: int) -> dict[str, tuple[str, float]]:
    """Retourne le total des credits de paiement (CP) par mission pour une annee.

    Cle = slug de la mission (stable dans le temps, cf. `api.etl.normalize`),
    valeur = (nom_officiel, montant total CP). Fonction partagee: reutilisee
    par le comparateur d'annees et, a terme, par le module de budget
    personnalise (repartition de la contribution individuelle par mission).
    """
    stmt = (
        select(
            Mission.slug,
            Mission.nom_officiel,
            func.coalesce(func.sum(Depense.cp), 0.0),
        )
        .join(Programme, Programme.mission_id == Mission.id)
        .join(Action, Action.programme_id == Programme.id)
        .join(Depense, Depense.action_id == Action.id)
        .where(Mission.annee == annee)
        .group_by(Mission.slug, Mission.nom_officiel)
    )
    result = await db.execute(stmt)
    return {slug: (nom, float(total)) for slug, nom, total in result.all()}
