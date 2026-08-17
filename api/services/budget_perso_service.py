"""Logique metier de la simulation de budget personnel (contribution individuelle).

Module 5 du CDC ("Budget personnalise"): estime la contribution individuelle
d'un contribuable au budget general de l'Etat (impot sur le revenu + TVA
estimes), ventilee par mission au prorata des depenses reelles de la derniere
annee budgetaire disponible en base.

Neutralite politique (CDC section 1.3): methode arithmetique publique et
sourcee uniquement (cf. `api.services.bareme_ir`), aucune connotation
politique, limites methodologiques affichees explicitement a l'utilisateur
(champ `methodologie` de la reponse). Ce n'est PAS un calculateur d'impot
officiel: c'est un outil pedagogique assume comme tel.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas.budget_perso import (
    BudgetPersoResponse,
    MethodologieInfo,
    RepartitionItem,
    SourceCitee,
)
from api.services.bareme_ir import (
    ABATTEMENT_FORFAITAIRE,
    TAUX_EPARGNE_MOYEN_MENAGES,
    TAUX_TVA_MOYEN_PONDERE,
    calculer_ir,
)
from api.services.budget_service import obtenir_derniere_annee_disponible
from api.services.mission_service import totaux_depenses_par_mission


def _construire_methodologie() -> MethodologieInfo:
    """Construit le bloc methodologie affiche a l'utilisateur (CDC 1.3)."""
    return MethodologieInfo(
        hypotheses=[
            "1 part de quotient familial (personne seule), sans enfant a charge.",
            "Aucune reduction ni credit d'impot pris en compte.",
            "Abattement forfaitaire de 10 % applique au revenu net annuel "
            "pour obtenir le revenu imposable (plafond et plancher legaux de "
            "cet abattement ignores).",
            "Consommation annuelle estimee comme le revenu net annuel diminue "
            "de l'epargne moyenne des menages francais (dernier taux "
            "trimestriel publie par l'INSEE).",
            "Taux de TVA applique a cette consommation estimee: taux moyen "
            "pondere national (tous taux legaux confondus), pas la structure "
            "de consommation reelle de l'utilisateur.",
        ],
        limites=[
            "Cet outil n'est PAS un calculateur d'impot officiel: il fournit "
            "une estimation pedagogique et simplifiee, pas une simulation "
            "fiscale personnalisee. Pour un calcul d'impot exact, utiliser le "
            "simulateur officiel sur impots.gouv.fr.",
            "L'hypothese d'1 part fiscale / personne seule ne reflete pas la "
            "situation familiale reelle de l'utilisateur (quotient familial, "
            "parts supplementaires, situation de couple, etc.).",
            "Le taux de TVA applique est un taux moyen pondere national "
            "(dernier chiffre publie), pas la consommation reelle de "
            "l'utilisateur: la structure de consommation par taux (20 % / "
            "10 % / 5,5 % / 2,1 %) propre a chaque foyer n'est pas connue ici.",
            "Les cotisations sociales sont hors perimetre: elles financent la "
            "Securite sociale, un perimetre distinct du budget general de "
            "l'Etat modelise par cet outil (aucune donnee Securite sociale "
            "n'est ingeree en base). Les ventiler par mission de l'Etat "
            "serait methodologiquement incoherent, elles ne sont donc pas "
            "incluses dans ce calcul.",
            "La ventilation par mission est une proratisation (montant "
            "estime multiplie par la part de la mission dans les depenses "
            "nettes totales de la derniere annee disponible), pas un "
            "flechage reel des sommes versees. Le budget general de l'Etat "
            "est regi par le principe de non-affectation des recettes "
            "(art. 6 de la LOLF): aucune recette n'est legalement affectee a "
            "une depense precise. La proratisation est donc la "
            "representation la plus fidele possible de la realite "
            "budgetaire, pas une approximation gratuite.",
        ],
        sources=[
            SourceCitee(
                nom=(
                    "Bareme de l'impot sur le revenu 2026 (revenus 2025), "
                    "1 part - service-public.fr"
                ),
                url="https://www.service-public.gouv.fr/particuliers/vosdroits/F1419",
            ),
            SourceCitee(
                nom=(
                    "Abattement forfaitaire de 10 % pour frais professionnels "
                    "- service-public.fr"
                ),
                url="https://www.service-public.gouv.fr/particuliers/vosdroits/F1989",
            ),
            SourceCitee(
                nom=(
                    "Taux d'epargne des menages, 4e trimestre 2025 - INSEE "
                    "Informations rapides n°51"
                ),
                url="https://www.insee.fr/fr/statistiques/8885657",
            ),
            SourceCitee(
                nom=(
                    "Taux de TVA moyen pondere effectif (9,7 %) - Conseil des "
                    "prelevements obligatoires, note n°6, sept. 2023, repris "
                    "par FIPECO"
                ),
                url="https://www.fipeco.fr/fiche/La-taxe-sur-la-valeur-ajout%C3%A9e",
            ),
        ],
    )


async def calculer_budget_perso(revenu_net: float, db: AsyncSession) -> BudgetPersoResponse:
    """Estime la contribution individuelle au budget de l'Etat a partir du revenu net mensuel."""
    revenu_net_annuel = revenu_net * 12

    revenu_imposable = revenu_net_annuel * (1 - ABATTEMENT_FORFAITAIRE)
    ir_estime = calculer_ir(revenu_imposable)

    consommation_estimee = revenu_net_annuel * (1 - TAUX_EPARGNE_MOYEN_MENAGES)
    tva_estimee = consommation_estimee * TAUX_TVA_MOYEN_PONDERE

    contribution_totale_estimee = ir_estime + tva_estimee

    annee_budget = await obtenir_derniere_annee_disponible(db)
    totaux_missions = await totaux_depenses_par_mission(db, annee_budget.annee)
    depenses_nettes_totales = float(annee_budget.depenses_nettes)

    repartition: list[RepartitionItem] = []
    if depenses_nettes_totales > 0:
        for slug, (nom, depense_mission) in totaux_missions.items():
            part = depense_mission / depenses_nettes_totales
            repartition.append(
                RepartitionItem(
                    mission_slug=slug,
                    mission_nom=nom,
                    montant=contribution_totale_estimee * part,
                )
            )
        repartition.sort(key=lambda item: item.montant, reverse=True)

    return BudgetPersoResponse(
        revenu_net_mensuel=revenu_net,
        annee_reference=annee_budget.annee,
        ir_estime=ir_estime,
        tva_estimee=tva_estimee,
        contribution_totale_estimee=contribution_totale_estimee,
        repartition=repartition,
        methodologie=_construire_methodologie(),
    )
