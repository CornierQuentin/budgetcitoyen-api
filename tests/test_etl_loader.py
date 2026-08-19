"""Tests du module de chargement (upsert) ETL en base (`api.etl.loader`).

Contrairement a `test_etl_normalize.py` (fonctions pures, sans DB), ces
fonctions ecrivent reellement en base: on reutilise `db_session` (meme
convention que les tests de routers), sur la base de test isolee garantie
par `conftest.py`.
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.etl import loader
from api.etl.normalize import (
    DepenseAggregat,
    DepenseFiscaleRecord,
    MissionAliasRow,
    MissionYearRow,
    RecetteAggregat,
)
from api.models.action import Action
from api.models.annee_budget import AnneeBudget
from api.models.depense import Depense
from api.models.depense_fiscale import DepenseFiscale, StatutMontant
from api.models.indicateur_macro import IndicateurMacro
from api.models.ingestion_log import IngestionLog
from api.models.mission import Mission
from api.models.mission_alias import MissionAlias
from api.models.programme import Programme
from api.models.recette import Recette, TypeRecette

# ---------------------------------------------------------------------------
# upsert_missions
# ---------------------------------------------------------------------------


async def test_upsert_missions_avec_liste_vide_ne_fait_rien(db_session: AsyncSession) -> None:
    mapping = await loader.upsert_missions(db_session, [])

    assert mapping == {}


async def test_upsert_missions_insere_et_retourne_le_mapping(db_session: AsyncSession) -> None:
    rows = [
        MissionYearRow(
            slug="justice",
            nom_normalise="justice",
            nom_officiel="Justice",
            annee=2024,
            code_mission="JA",
        ),
        MissionYearRow(
            slug="education",
            nom_normalise="education",
            nom_officiel="Enseignement scolaire",
            annee=2024,
            code_mission="EN",
        ),
    ]

    mapping = await loader.upsert_missions(db_session, rows)
    await db_session.commit()

    assert set(mapping.keys()) == {("justice", 2024), ("education", 2024)}

    result = await db_session.execute(select(Mission).where(Mission.slug == "justice"))
    mission = result.scalar_one()
    assert mission.id == mapping[("justice", 2024)]
    assert mission.nom_officiel == "Justice"
    assert mission.code_mission == "JA"


async def test_upsert_missions_est_idempotent_sur_slug_annee(db_session: AsyncSession) -> None:
    row_initial = MissionYearRow(
        slug="justice",
        nom_normalise="justice",
        nom_officiel="Justice",
        annee=2024,
        code_mission="JA",
    )
    mapping_1 = await loader.upsert_missions(db_session, [row_initial])
    await db_session.commit()

    row_mis_a_jour = MissionYearRow(
        slug="justice",
        nom_normalise="justice (maj)",
        nom_officiel="Ministere de la Justice",
        annee=2024,
        code_mission="JA",
    )
    mapping_2 = await loader.upsert_missions(db_session, [row_mis_a_jour])
    await db_session.commit()

    # Meme id (update, pas un doublon insere).
    assert mapping_1[("justice", 2024)] == mapping_2[("justice", 2024)]

    result = await db_session.execute(select(Mission).where(Mission.slug == "justice"))
    missions = result.scalars().all()
    assert len(missions) == 1
    assert missions[0].nom_officiel == "Ministere de la Justice"


# ---------------------------------------------------------------------------
# upsert_mission_aliases
# ---------------------------------------------------------------------------


async def test_upsert_mission_aliases_sans_annees_ni_rows_ne_fait_rien(
    db_session: AsyncSession,
) -> None:
    await loader.upsert_mission_aliases(db_session, [], [])
    # Ne doit pas lever; rien a verifier de plus (pas de mission en base).


async def test_upsert_mission_aliases_reconstruit_les_alias_fournis(
    db_session: AsyncSession,
) -> None:
    mapping = await loader.upsert_missions(
        db_session,
        [
            MissionYearRow(
                slug="justice",
                nom_normalise="justice",
                nom_officiel="Justice",
                annee=2024,
                code_mission="JA",
            )
        ],
    )
    await db_session.commit()
    mission_id = mapping[("justice", 2024)]

    alias = MissionAliasRow(
        nom_csv="Justice (ancien libelle)",
        slug="justice",
        annee_cible=2024,
        annee_debut=2018,
        annee_fin=2023,
    )
    await loader.upsert_mission_aliases(db_session, [(alias, mission_id)], [2024])
    await db_session.commit()

    result = await db_session.execute(
        select(MissionAlias).where(MissionAlias.mission_id == mission_id)
    )
    aliases = result.scalars().all()
    assert len(aliases) == 1
    assert aliases[0].nom_csv == "Justice (ancien libelle)"
    assert aliases[0].annee_debut == 2018
    assert aliases[0].annee_fin == 2023


async def test_upsert_mission_aliases_scope_la_suppression_aux_annees_fournies(
    db_session: AsyncSession,
) -> None:
    """Un run partiel (ex: --annees 2024) ne doit PAS effacer les alias des
    missions d'autres annees (bug reel documente dans le docstring de la fonction)."""
    mapping = await loader.upsert_missions(
        db_session,
        [
            MissionYearRow(
                slug="justice",
                nom_normalise="justice",
                nom_officiel="Justice",
                annee=2024,
                code_mission="JA",
            ),
            MissionYearRow(
                slug="education",
                nom_normalise="education",
                nom_officiel="Education",
                annee=2018,
                code_mission="EN",
            ),
        ],
    )
    await db_session.commit()
    mission_justice_id = mapping[("justice", 2024)]
    mission_education_id = mapping[("education", 2018)]

    alias_justice = MissionAliasRow(
        nom_csv="Justice",
        slug="justice",
        annee_cible=2024,
        annee_debut=2024,
        annee_fin=2024,
    )
    alias_education = MissionAliasRow(
        nom_csv="Education",
        slug="education",
        annee_cible=2018,
        annee_debut=2018,
        annee_fin=2018,
    )
    await loader.upsert_mission_aliases(
        db_session,
        [(alias_justice, mission_justice_id), (alias_education, mission_education_id)],
        [2024, 2018],
    )
    await db_session.commit()

    # Nouveau run partiel, ne couvrant QUE 2024, avec aucun nouvel alias a inserer
    # (simule un run --depenses-only --annees 2024 qui ne genere aucun alias).
    await loader.upsert_mission_aliases(db_session, [], [2024])
    await db_session.commit()

    result = await db_session.execute(select(MissionAlias))
    aliases_restants = result.scalars().all()

    # L'alias de la mission 2024 a ete supprime (scope du run), celui de 2018 conserve.
    assert len(aliases_restants) == 1
    assert aliases_restants[0].mission_id == mission_education_id


# ---------------------------------------------------------------------------
# upsert_depenses
# ---------------------------------------------------------------------------


def _aggregat(
    mission_code: str = "JA",
    mission_libelle: str = "Justice",
    programme_code: str = "JA-P1",
    programme_libelle: str = "Programme Justice",
    action_code: str = "JA-A1",
    action_libelle: str = "Action Justice",
    ae: float = 100.0,
    cp: float = 100.0,
    annee: int = 2024,
) -> DepenseAggregat:
    return DepenseAggregat(
        annee=annee,
        mission_code=mission_code,
        mission_libelle=mission_libelle,
        programme_code=programme_code,
        programme_libelle=programme_libelle,
        action_code=action_code,
        action_libelle=action_libelle,
        ae=ae,
        cp=cp,
    )


async def test_upsert_depenses_avec_aggregats_vides_supprime_et_retourne_zero(
    db_session: AsyncSession,
) -> None:
    nb = await loader.upsert_depenses(db_session, 2024, [])

    assert nb == 0


async def test_upsert_depenses_insere_programmes_actions_depenses(db_session: AsyncSession) -> None:
    mapping = await loader.upsert_missions(
        db_session,
        [
            MissionYearRow(
                slug="justice",
                nom_normalise="justice",
                nom_officiel="Justice",
                annee=2024,
                code_mission="JA",
            )
        ],
    )
    await db_session.commit()
    mission_id = mapping[("justice", 2024)]

    aggregats = [
        (_aggregat(action_code="JA-A1", ae=100.0, cp=90.0), mission_id),
        (_aggregat(action_code="JA-A2", ae=50.0, cp=40.0), mission_id),
    ]

    nb = await loader.upsert_depenses(db_session, 2024, aggregats)
    await db_session.commit()

    assert nb == 2

    result_programmes = await db_session.execute(
        select(Programme).where(Programme.mission_id == mission_id)
    )
    programmes = result_programmes.scalars().all()
    assert len(programmes) == 1  # un seul programme (JA-P1), partage par les 2 actions
    assert programmes[0].code == "JA-P1"

    result_actions = await db_session.execute(
        select(Action).where(Action.programme_id == programmes[0].id)
    )
    actions = {a.code: a for a in result_actions.scalars().all()}
    assert set(actions.keys()) == {"JA-A1", "JA-A2"}

    result_depenses = await db_session.execute(select(Depense).where(Depense.annee == 2024))
    depenses = result_depenses.scalars().all()
    assert len(depenses) == 2
    assert {float(d.cp) for d in depenses} == {90.0, 40.0}


async def test_upsert_depenses_recharge_completement_l_annee(db_session: AsyncSession) -> None:
    """Un second appel pour la meme annee doit d'abord vider les donnees existantes
    (pas d'accumulation ni de doublon), meme si la nouvelle passe a moins de lignes."""
    mapping = await loader.upsert_missions(
        db_session,
        [
            MissionYearRow(
                slug="justice",
                nom_normalise="justice",
                nom_officiel="Justice",
                annee=2024,
                code_mission="JA",
            )
        ],
    )
    await db_session.commit()
    mission_id = mapping[("justice", 2024)]

    await loader.upsert_depenses(
        db_session,
        2024,
        [
            (_aggregat(action_code="JA-A1", cp=90.0), mission_id),
            (_aggregat(action_code="JA-A2", cp=40.0), mission_id),
        ],
    )
    await db_session.commit()

    nb_second_run = await loader.upsert_depenses(
        db_session, 2024, [(_aggregat(action_code="JA-A1", cp=999.0), mission_id)]
    )
    await db_session.commit()

    assert nb_second_run == 1

    result = await db_session.execute(select(Depense).where(Depense.annee == 2024))
    depenses = result.scalars().all()
    assert len(depenses) == 1
    assert float(depenses[0].cp) == 999.0


async def test_upsert_depenses_tronque_les_libelles_trop_longs(db_session: AsyncSession) -> None:
    mapping = await loader.upsert_missions(
        db_session,
        [
            MissionYearRow(
                slug="justice",
                nom_normalise="justice",
                nom_officiel="Justice",
                annee=2024,
                code_mission="JA",
            )
        ],
    )
    await db_session.commit()
    mission_id = mapping[("justice", 2024)]

    libelle_trop_long = "x" * 300
    await loader.upsert_depenses(
        db_session,
        2024,
        [(_aggregat(programme_libelle=libelle_trop_long), mission_id)],
    )
    await db_session.commit()

    result = await db_session.execute(select(Programme).where(Programme.mission_id == mission_id))
    programme = result.scalar_one()
    assert len(programme.nom) == 255


# ---------------------------------------------------------------------------
# upsert_recettes
# ---------------------------------------------------------------------------


async def test_upsert_recettes_avec_liste_vide_ne_fait_rien(db_session: AsyncSession) -> None:
    await loader.upsert_recettes(db_session, [])
    result = await db_session.execute(select(Recette))
    assert result.scalars().all() == []


async def test_upsert_recettes_insere_puis_met_a_jour(db_session: AsyncSession) -> None:
    aggregat = RecetteAggregat(
        annee=2024, type=TypeRecette.IR, montant_brut=100.0, montant_net=90.0
    )
    await loader.upsert_recettes(db_session, [aggregat])
    await db_session.commit()

    result = await db_session.execute(select(Recette).where(Recette.annee == 2024))
    recette = result.scalar_one()
    assert float(recette.montant_net) == 90.0

    aggregat_maj = RecetteAggregat(
        annee=2024, type=TypeRecette.IR, montant_brut=110.0, montant_net=105.0
    )
    await loader.upsert_recettes(db_session, [aggregat_maj])
    await db_session.commit()
    # `upsert_recettes` ecrit en SQL "brut" (hors ORM unit-of-work): sans ce
    # refresh explicite, la ligne deja chargee dans l'identity map de CETTE
    # session resterait a sa valeur pre-update (la vraie passe ETL, elle,
    # utilise une session fraiche par execution - cf. `api.etl.run` - donc
    # ne rencontre jamais ce cas).
    db_session.expire_all()

    result = await db_session.execute(select(Recette))
    recettes = result.scalars().all()
    assert len(recettes) == 1  # pas de doublon: meme (annee, type)
    assert float(recettes[0].montant_net) == 105.0


# ---------------------------------------------------------------------------
# get_remboursements_degrevements_cp
# ---------------------------------------------------------------------------


async def test_get_remboursements_degrevements_cp_sans_donnees_retourne_zero(
    db_session: AsyncSession,
) -> None:
    total = await loader.get_remboursements_degrevements_cp(db_session, 2024)

    assert total == 0.0


async def test_get_remboursements_degrevements_cp_somme_la_mission_rd(
    db_session: AsyncSession,
) -> None:
    mapping = await loader.upsert_missions(
        db_session,
        [
            MissionYearRow(
                slug="remboursements-et-degrevements",
                nom_normalise="remboursements et degrevements",
                nom_officiel="Remboursements et degrevements",
                annee=2024,
                code_mission="RD",
            ),
            MissionYearRow(
                slug="justice",
                nom_normalise="justice",
                nom_officiel="Justice",
                annee=2024,
                code_mission="JA",
            ),
        ],
    )
    await db_session.commit()

    await loader.upsert_depenses(
        db_session,
        2024,
        [
            (
                _aggregat(
                    mission_code="RD",
                    mission_libelle="Remboursements et degrevements",
                    programme_code="RD-P1",
                    action_code="RD-A1",
                    cp=1000.0,
                ),
                mapping[("remboursements-et-degrevements", 2024)],
            ),
            (
                _aggregat(
                    mission_code="JA",
                    mission_libelle="Justice",
                    programme_code="JA-P1",
                    action_code="JA-A1",
                    cp=500.0,
                ),
                mapping[("justice", 2024)],
            ),
        ],
    )
    await db_session.commit()

    total = await loader.get_remboursements_degrevements_cp(db_session, 2024)

    assert total == 1000.0


# ---------------------------------------------------------------------------
# get_remboursements_degrevements_impots_etat_cp
# ---------------------------------------------------------------------------


async def test_get_remboursements_degrevements_impots_etat_cp_ignore_les_impots_locaux(
    db_session: AsyncSession,
) -> None:
    """LFI 2026 (Etat A): "Remboursements et degrevements" a 2 programmes
    ("...d'impots d'Etat" et "...d'impots locaux") - seul le premier doit
    etre regrossi (les remboursements d'impots locaux ne sont jamais
    netes d'une recette d'Etat, cf. docstring de la fonction). Aucun code
    disponible (mission ET programme) sur cette source: recherche par slug
    de mission + libelle de programme.
    """
    mapping = await loader.upsert_missions(
        db_session,
        [
            MissionYearRow(
                slug="remboursements-et-degrevements",
                nom_normalise="remboursements et degrevements",
                nom_officiel="Remboursements et degrevements",
                annee=2026,
                code_mission=None,
            )
        ],
    )
    await db_session.commit()

    mission_id = mapping[("remboursements-et-degrevements", 2026)]
    await loader.upsert_depenses(
        db_session,
        2026,
        [
            (
                _aggregat(
                    mission_code="",
                    mission_libelle="Remboursements et degrevements",
                    programme_code="hash-etat",
                    programme_libelle="Remboursements et dégrèvements d'impôts d'Etat",
                    action_code="hash-etat",
                    cp=141174362742.0,
                    annee=2026,
                ),
                mission_id,
            ),
            (
                _aggregat(
                    mission_code="",
                    mission_libelle="Remboursements et degrevements",
                    programme_code="hash-locaux",
                    programme_libelle="Remboursements et dégrèvements d'impôts locaux",
                    action_code="hash-locaux",
                    cp=4426000000.0,
                    annee=2026,
                ),
                mission_id,
            ),
        ],
    )
    await db_session.commit()

    total = await loader.get_remboursements_degrevements_impots_etat_cp(db_session, 2026)

    assert total == 141174362742.0


async def test_get_remboursements_degrevements_impots_etat_cp_sans_donnees_retourne_zero(
    db_session: AsyncSession,
) -> None:
    total = await loader.get_remboursements_degrevements_impots_etat_cp(db_session, 2026)

    assert total == 0.0


# ---------------------------------------------------------------------------
# recalculer_annee_budget
# ---------------------------------------------------------------------------


async def test_recalculer_annee_budget_sans_depenses_ni_recettes_retourne_none(
    db_session: AsyncSession,
) -> None:
    result = await loader.recalculer_annee_budget(db_session, 2024, "https://example.test")

    assert result is None

    result_db = await db_session.execute(select(AnneeBudget))
    assert result_db.scalars().all() == []


async def test_recalculer_annee_budget_avec_seulement_des_depenses_retourne_none(
    db_session: AsyncSession,
) -> None:
    mapping = await loader.upsert_missions(
        db_session,
        [
            MissionYearRow(
                slug="justice",
                nom_normalise="justice",
                nom_officiel="Justice",
                annee=2024,
                code_mission="JA",
            )
        ],
    )
    await db_session.commit()
    await loader.upsert_depenses(
        db_session, 2024, [(_aggregat(cp=100.0), mapping[("justice", 2024)])]
    )
    await db_session.commit()

    result = await loader.recalculer_annee_budget(db_session, 2024, "https://example.test")

    assert result is None


async def test_recalculer_annee_budget_calcule_le_deficit_avec_psr(
    db_session: AsyncSession,
) -> None:
    mapping = await loader.upsert_missions(
        db_session,
        [
            MissionYearRow(
                slug="justice",
                nom_normalise="justice",
                nom_officiel="Justice",
                annee=2024,
                code_mission="JA",
            )
        ],
    )
    await db_session.commit()
    await loader.upsert_depenses(
        db_session, 2024, [(_aggregat(cp=1000.0), mapping[("justice", 2024)])]
    )
    await loader.upsert_recettes(
        db_session,
        [RecetteAggregat(annee=2024, type=TypeRecette.IR, montant_brut=900.0, montant_net=900.0)],
    )
    await db_session.commit()

    annee_budget = await loader.recalculer_annee_budget(
        db_session, 2024, "https://example.test", prelevements_sur_recettes=50.0
    )
    await db_session.commit()

    assert annee_budget is not None
    assert annee_budget.annee == 2024
    assert float(annee_budget.depenses_nettes) == 1000.0
    # recettes_nettes = 900 (brutes) - 50 (PSR) = 850
    assert float(annee_budget.recettes_nettes) == 850.0
    # deficit = depenses - recettes = 1000 - 850 = 150
    assert float(annee_budget.deficit) == 150.0
    assert annee_budget.dette_pib is None


async def test_recalculer_annee_budget_est_idempotent_et_preserve_dette_pib(
    db_session: AsyncSession,
) -> None:
    mapping = await loader.upsert_missions(
        db_session,
        [
            MissionYearRow(
                slug="justice",
                nom_normalise="justice",
                nom_officiel="Justice",
                annee=2024,
                code_mission="JA",
            )
        ],
    )
    await db_session.commit()
    await loader.upsert_depenses(
        db_session, 2024, [(_aggregat(cp=1000.0), mapping[("justice", 2024)])]
    )
    await loader.upsert_recettes(
        db_session,
        [RecetteAggregat(annee=2024, type=TypeRecette.IR, montant_brut=900.0, montant_net=900.0)],
    )
    await db_session.commit()

    await loader.recalculer_annee_budget(db_session, 2024, "https://example.test")
    await db_session.commit()

    # Un backfill manuel renseigne dette_pib (hors perimetre de ce recalcul).
    result = await db_session.execute(select(AnneeBudget).where(AnneeBudget.annee == 2024))
    annee_budget = result.scalar_one()
    annee_budget.dette_pib = 111.1
    await db_session.commit()
    db_session.expire_all()

    # Un second recalcul (ex: relance --recettes-only) ne doit pas ecraser dette_pib.
    annee_budget_recalculee = await loader.recalculer_annee_budget(
        db_session, 2024, "https://example.test/v2"
    )
    await db_session.commit()

    assert annee_budget_recalculee is not None
    assert float(annee_budget_recalculee.dette_pib) == 111.1
    assert annee_budget_recalculee.source_url == "https://example.test/v2"

    result_final = await db_session.execute(select(AnneeBudget))
    assert len(result_final.scalars().all()) == 1  # pas de doublon


# ---------------------------------------------------------------------------
# upsert_indicateurs_macro
# ---------------------------------------------------------------------------


async def test_upsert_indicateurs_macro_sans_donnees_ne_fait_rien(db_session: AsyncSession) -> None:
    await loader.upsert_indicateurs_macro(db_session, {}, {}, {}, "https://example.test")

    result = await db_session.execute(select(IndicateurMacro))
    assert result.scalars().all() == []


async def test_upsert_indicateurs_macro_annee_sans_pib_garde_pib_null(
    db_session: AsyncSession,
) -> None:
    await loader.upsert_indicateurs_macro(
        db_session,
        pib={},
        population={2019: 64_700_000},
        source_pib_url={},
        source_population_url="https://example.test/population",
    )
    await db_session.commit()

    result = await db_session.execute(select(IndicateurMacro).where(IndicateurMacro.annee == 2019))
    indicateur = result.scalar_one()
    assert indicateur.pib_courant is None
    assert indicateur.source_pib_url is None
    assert indicateur.population == 64_700_000
    assert indicateur.source_population_url == "https://example.test/population"


async def test_upsert_indicateurs_macro_insere_puis_met_a_jour(db_session: AsyncSession) -> None:
    await loader.upsert_indicateurs_macro(
        db_session,
        pib={2024: 2_919_900_000_000.0},
        population={2024: 68_436_616},
        source_pib_url={2024: "https://example.test/pib-2024"},
        source_population_url="https://example.test/population",
    )
    await db_session.commit()

    await loader.upsert_indicateurs_macro(
        db_session,
        pib={2024: 3_000_000_000_000.0},
        population={2024: 68_500_000},
        source_pib_url={2024: "https://example.test/pib-2024-v2"},
        source_population_url="https://example.test/population-v2",
    )
    await db_session.commit()

    result = await db_session.execute(select(IndicateurMacro))
    indicateurs = result.scalars().all()
    assert len(indicateurs) == 1  # pas de doublon (upsert sur `annee`)
    assert float(indicateurs[0].pib_courant) == 3_000_000_000_000.0
    assert indicateurs[0].population == 68_500_000
    assert indicateurs[0].source_pib_url == "https://example.test/pib-2024-v2"


# ---------------------------------------------------------------------------
# enregistrer_ingestion_terminee
# ---------------------------------------------------------------------------


async def test_enregistrer_ingestion_terminee_ajoute_une_ligne(
    db_session: AsyncSession,
) -> None:
    avant = (await db_session.execute(select(IngestionLog))).scalars().all()
    assert avant == []

    await loader.enregistrer_ingestion_terminee(db_session)
    await db_session.commit()

    lignes = (await db_session.execute(select(IngestionLog))).scalars().all()
    assert len(lignes) == 1
    assert lignes[0].termine_a is not None


async def test_enregistrer_ingestion_terminee_appelee_plusieurs_fois_ajoute_plusieurs_lignes(
    db_session: AsyncSession,
) -> None:
    await loader.enregistrer_ingestion_terminee(db_session)
    await loader.enregistrer_ingestion_terminee(db_session)
    await db_session.commit()

    lignes = (await db_session.execute(select(IngestionLog))).scalars().all()
    assert len(lignes) == 2


def _depense_fiscale_record(numero: str, **overrides: object) -> DepenseFiscaleRecord:
    defaults: dict = {
        "annee": 2021,
        "numero": numero,
        "categorie": "Impôt sur le revenu",
        "sous_categorie": "Sous-categorie",
        "sous_sous_categorie": None,
        "libelle": "Libelle de la mesure",
        "beneficiaire": "Menages",
        "montant_millions": 10.0,
        "statut_montant": StatutMontant.CHIFFRE,
        "methode_chiffrage": "Simulation",
    }
    defaults.update(overrides)
    return DepenseFiscaleRecord(**defaults)


async def test_upsert_depenses_fiscales_sans_donnees_ne_fait_rien(
    db_session: AsyncSession,
) -> None:
    n = await loader.upsert_depenses_fiscales(db_session, 2021, [])

    assert n == 0
    assert (await db_session.execute(select(DepenseFiscale))).scalars().all() == []


async def test_upsert_depenses_fiscales_insere_les_statuts_non_chiffrables(
    db_session: AsyncSession,
) -> None:
    records = [
        _depense_fiscale_record("1", statut_montant=StatutMontant.CHIFFRE, montant_millions=42.0),
        _depense_fiscale_record("2", statut_montant=StatutMontant.EPSILON, montant_millions=None),
        _depense_fiscale_record(
            "3", statut_montant=StatutMontant.NON_CALCULABLE, montant_millions=None
        ),
        _depense_fiscale_record(
            "4", statut_montant=StatutMontant.AUCUN_EFFET, montant_millions=None
        ),
    ]

    n = await loader.upsert_depenses_fiscales(db_session, 2021, records)
    await db_session.commit()

    assert n == 4
    result = await db_session.execute(select(DepenseFiscale).order_by(DepenseFiscale.numero))
    lignes = result.scalars().all()
    assert [ligne.statut_montant for ligne in lignes] == [
        StatutMontant.CHIFFRE,
        StatutMontant.EPSILON,
        StatutMontant.NON_CALCULABLE,
        StatutMontant.AUCUN_EFFET,
    ]
    # Les montants non chiffrables restent None, jamais 0.
    assert [ligne.montant_millions for ligne in lignes[1:]] == [None, None, None]


async def test_upsert_depenses_fiscales_remplace_l_annee_sans_toucher_les_autres(
    db_session: AsyncSession,
) -> None:
    await loader.upsert_depenses_fiscales(db_session, 2021, [_depense_fiscale_record("1")])
    await loader.upsert_depenses_fiscales(db_session, 2022, [_depense_fiscale_record("1")])
    await db_session.commit()

    await loader.upsert_depenses_fiscales(
        db_session, 2021, [_depense_fiscale_record("1"), _depense_fiscale_record("2")]
    )
    await db_session.commit()

    result = await db_session.execute(select(DepenseFiscale))
    lignes = result.scalars().all()
    assert sorted((ligne.annee, ligne.numero) for ligne in lignes) == [
        (2021, "1"),
        (2021, "2"),
        (2022, "1"),
    ]
