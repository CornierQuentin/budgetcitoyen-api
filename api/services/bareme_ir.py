"""Constantes fiscales sourcees pour le module Budget personnalise (Module 5).

Neutralite politique (CDC section 1.3): chaque constante ci-dessous est une
valeur publique, sourcee (URL officielle ou institutionnelle) et datee. Cet
outil est un simulateur pedagogique, pas un calculateur d'impot officiel.

IMPORTANT: ces valeurs doivent etre revues chaque annee (barreme fiscal,
taux d'epargne, taux de TVA moyen) au meme titre qu'un dataset ETL — la
tracabilite (source + date de reference) est essentielle pour permettre
cette mise a jour.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class TrancheIR:
    """Une tranche du bareme progressif de l'impot sur le revenu.

    `plafond=None` signifie "et plus" (derniere tranche, taux marginal
    maximal, aucun plafond superieur).
    """

    plafond: float | None
    taux: float


# ---------------------------------------------------------------------------
# Bareme de l'impot sur le revenu, pour 1 part de quotient familial (personne
# seule, sans enfant a charge, aucune reduction/credit d'impot).
#
# Bareme 2026, applicable aux revenus 2025 (dernier bareme publie au moment de
# cette ecriture, 17/08/2026).
# Source: service-public.fr, "Quel est le bareme de l'impot sur le revenu ?"
# https://www.service-public.gouv.fr/particuliers/vosdroits/F1419
# (page verifiee le 15/04/2026: "le bareme de 2026 (applicable aux revenus de
# 2025)").
# Seuils corrobores independamment par la recherche du 17/08/2026 (memes
# valeurs: 11 600 / 29 579 / 84 577 / 181 917 euros).
# ---------------------------------------------------------------------------
BAREME_IR: list[TrancheIR] = [
    TrancheIR(plafond=11_600.0, taux=0.0),
    TrancheIR(plafond=29_579.0, taux=0.11),
    TrancheIR(plafond=84_577.0, taux=0.30),
    TrancheIR(plafond=181_917.0, taux=0.41),
    TrancheIR(plafond=None, taux=0.45),
]

# Abattement forfaitaire pour frais professionnels, applique au revenu net
# annuel pour obtenir le revenu imposable (regime "frais reels" non modelise).
# LIMITE documentee: le plafond et le plancher legaux de cet abattement (cf.
# art. 83 3° du Code general des impots) sont ignores ici par simplification.
# Source: https://www.service-public.gouv.fr/particuliers/vosdroits/F1989
ABATTEMENT_FORFAITAIRE = 0.10

# ---------------------------------------------------------------------------
# Taux d'epargne des menages francais (part du revenu disponible brut, RDB,
# non consommee). Dernier chiffre trimestriel publie par l'INSEE au moment de
# cette ecriture: 4e trimestre 2025 (publication du 27/02/2026).
# Source: INSEE, Informations rapides n°51, "Au quatrieme trimestre 2025, le
# PIB augmente legerement (+0,2 %) et le taux d'epargne des menages recule de
# nouveau (17,9 % apres 18,3 %)"
# https://www.insee.fr/fr/statistiques/8885657
# ---------------------------------------------------------------------------
TAUX_EPARGNE_MOYEN_MENAGES = 0.179

# ---------------------------------------------------------------------------
# Taux de TVA moyen pondere effectif (recettes nettes de TVA rapportees aux
# emplois taxables), tous taux legaux confondus (20 % / 10 % / 5,5 % / 2,1 %).
#
# Valeur retenue: 9,7 %.
# Source primaire: Conseil des prelevements obligatoires (CPO), note n°6,
# septembre 2023, "La TVA est-elle un impot juste ?" ("son taux effectif
# moyen ... est de 9,7 % en 2019")
# https://www.ccomptes.fr/sites/default/files/2023-10/20230928-TVA-est-elle-impot-juste.pdf
# Repris et confirme par FIPECO, fiche "La taxe sur la valeur ajoutee":
# https://www.fipeco.fr/fiche/La-taxe-sur-la-valeur-ajout%C3%A9e
# Corrobore independamment par une estimation de la Commission europeenne
# pour 2021 (9,7 % egalement), citee par la Fondation IFRAP:
# https://www.ifrap.org/budget-et-fiscalite/tva-3-ans-de-stagnation-des-recettes-et-derreurs-de-previsions
#
# LIMITES documentees: (1) c'est le chiffre le plus recent publie tel quel au
# moment de cette recherche (annees de reference 2019/2021, pas 2025 — aucune
# publication plus recente d'un taux effectif moyen pondere n'a ete trouvee);
# (2) la methode sous-jacente rapporte les recettes de TVA aux "emplois
# taxables" (au sens comptabilite nationale), un perimetre legerement plus
# large que la seule consommation finale des menages. A defaut de donnee plus
# recente publiee telle quelle, retenu comme la meilleure estimation
# institutionnelle disponible pour cet outil pedagogique.
# ---------------------------------------------------------------------------
TAUX_TVA_MOYEN_PONDERE = 0.097


def calculer_ir(revenu_imposable: float) -> float:
    """Calcule l'impot sur le revenu selon le bareme progressif par tranches.

    Applique le principe standard du bareme progressif francais: chaque
    tranche de revenu imposable est taxee au taux qui lui correspond (seul le
    "surplus" au-dessus d'un seuil est taxe au taux marginal superieur).
    """
    if revenu_imposable <= 0:
        return 0.0

    impot = 0.0
    seuil_bas = 0.0
    for tranche in BAREME_IR:
        if revenu_imposable <= seuil_bas:
            break
        plafond = tranche.plafond if tranche.plafond is not None else revenu_imposable
        montant_tranche = min(revenu_imposable, plafond) - seuil_bas
        if montant_tranche > 0:
            impot += montant_tranche * tranche.taux
        seuil_bas = plafond
        if tranche.plafond is None:
            break
    return impot
