"""Modele DepenseFiscale (niches fiscales, annexe "Voies et moyens" Tome II du PLF)."""

import enum

from sqlalchemy import Enum, Numeric
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class StatutMontant(enum.StrEnum):
    """Statut du montant chiffre d'une mesure fiscale.

    La source (Tome II "Voies et moyens") encode certains montants avec des
    valeurs non numeriques plutot qu'un chiffre: `" - "` (aucun effet
    budgetaire chiffrable pour cette mesure), `"ε"` (epsilon, effet non nul
    mais inferieur a 0,5 M EUR, non calcule precisement), `"nc"` (non
    calculable, methode de chiffrage non disponible). Ces 3 cas ne doivent
    JAMAIS etre traites comme 0 (fausserait silencieusement les totaux et
    classements) - `montant_millions` reste `None` pour ces 3 statuts,
    `CHIFFRE` etant le seul avec une valeur exploitable.
    """

    CHIFFRE = "chiffre"
    EPSILON = "epsilon"
    NON_CALCULABLE = "non_calculable"
    AUCUN_EFFET = "aucun_effet"


class DepenseFiscale(Base):
    """Une mesure de depense fiscale (niche fiscale) pour une annee de PLF.

    Domaine independant de missions/depenses/recettes/annee_budget: les
    niches fiscales sont des allegements deja integres dans les recettes
    fiscales nettes (recette.montant_net) - ne jamais les re-deduire du
    deficit calcule, ce module est purement informatif/visualisation.

    `categorie` est en realite le type d'impot concerne par la mesure (ex:
    "Impot sur le revenu", "Taxe sur la valeur ajoutee", "Impots locaux") -
    la source ("Voies et moyens" Tome II) n'expose aucune colonne "impot
    concerne" distincte de cette categorie de tete, verifie a l'inspection
    reelle du fichier source (9 valeurs de categorie, toutes des types
    d'impot).
    """

    __tablename__ = "depense_fiscale"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    annee: Mapped[int] = mapped_column(index=True)
    numero: Mapped[str] = mapped_column(index=True)
    categorie: Mapped[str]
    sous_categorie: Mapped[str]
    sous_sous_categorie: Mapped[str | None]
    libelle: Mapped[str]
    beneficiaire: Mapped[str]
    montant_millions: Mapped[float | None] = mapped_column(Numeric(12, 1))
    statut_montant: Mapped[StatutMontant] = mapped_column(
        Enum(
            StatutMontant,
            name="statut_montant_depense_fiscale",
            # Sans values_callable, SQLAlchemy serialise un Enum Python par
            # son .name (ex: "EPSILON"), pas sa .value ("epsilon" - le type
            # PostgreSQL cree par la migration 0005 attend les valeurs en
            # minuscules): piege reel, jamais rencontre sur TypeRecette
            # (dont name == value pour chaque membre, ex. IR="IR").
            values_callable=lambda enum_cls: [e.value for e in enum_cls],
        )
    )
    methode_chiffrage: Mapped[str | None]
