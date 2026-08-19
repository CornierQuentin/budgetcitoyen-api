"""Modele MarchePublic (Donnees Essentielles de la Commande Publique - DECP).

Source: dataset OpenDataSoft `decp-2022-marches-valides` (data.economie.gouv.fr,
cf. api.etl.sources) - "2022" designe la version du format legal (arrete du
22/12/2022), PAS une restriction d'annee: les donnees couvrent en realite
2010-06-02 a aujourd'hui, mises a jour quotidiennement. 17 colonnes retenues
sur les 54 de la source (modifications/sous-traitance/geolocalisation
notamment exclues, cf. plan du chantier) - table rechargee integralement a
chaque run ETL (pas d'upsert par annee comme le reste du projet), voir
`api.etl.loader.upsert_marches`.
"""

from datetime import date

from sqlalchemy import Boolean, Date, Index, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from api.models.base import Base


class MarchePublic(Base):
    """Un marche public notifie (DECP).

    Pas de nom d'entreprise/acheteur exploitable dans cette source: seuls des
    identifiants (`acheteur_siret`, `titulaire_siret`) sont disponibles -
    verifie reellement, pas une limitation de l'ETL. Le frontend doit lier
    vers l'annuaire public (annuaire-entreprises.data.gouv.fr) plutot que
    d'afficher un nom.
    """

    __tablename__ = "marche_public"
    __table_args__ = (
        Index(
            "ix_marche_public_objet_recherche_trgm",
            "objet_recherche",
            postgresql_using="gin",
            postgresql_ops={"objet_recherche": "gin_trgm_ops"},
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Reference du marche cote source ("id" dans le dataset). PAS une cle
    # unique fiable: sur 689 062 lignes reelles, seulement 465 736 valeurs
    # distinctes (des acheteurs saisissent des bouts d'annee comme "2024" a
    # la place d'une vraie reference) - gardee uniquement pour tracabilite/
    # debug vers la source, jamais utilisee pour un upsert ou une contrainte
    # d'unicite.
    marche_id_source: Mapped[str] = mapped_column(String(64), index=True)

    nature: Mapped[str | None]
    objet: Mapped[str] = mapped_column(Text)
    # `objet` normalise (NFD, sans diacritiques, minuscules - meme logique
    # que normaliserPourRecherche() cote frontend) pour la recherche texte
    # serveur (index trigram ci-dessus) - jamais affiche tel quel.
    objet_recherche: Mapped[str] = mapped_column(Text)

    # 14 caracteres attendus ("XXXXXXXX-Y") mais certaines lignes reelles
    # portent le meme artefact "INX " que decrit ci-dessous, poussant la
    # longueur reelle observee a 14 caracteres au maximum - String(16) garde
    # une marge plutot que de coller exactement a l'observation actuelle
    # (donnee source vivante, sans garantie de stabilite).
    codecpv: Mapped[str] = mapped_column(String(16))
    # 2 premiers caracteres bruts de `codecpv`. Sur ~0,5% des lignes reelles,
    # `codecpv` est prefixe par un artefact de saisie "INX " (ex. "INX
    # 11200000-2") - cette extraction naive leur donne alors la "division"
    # "IN", traitee comme un bucket "Non classe" explicite plutot qu'une
    # vraie division CPV, cf. api.core.cpv.CPV_DIVISION_LABELS.
    codecpv_division: Mapped[str] = mapped_column(String(2), index=True)

    procedure: Mapped[str | None]

    acheteur_siret: Mapped[str] = mapped_column(String(20), index=True)
    # Piege reel confirme par l'execution reelle de l'ETL: PAS toujours un
    # SIRET a 14 chiffres meme quand `titulaire_id_type == "SIRET"` - des
    # valeurs vues en pratique atteignent 41 caracteres (SIRET truffes
    # d'espaces, ou memes prefixes "INX " que sur `codecpv`, ex. "INX
    # 19800AUTINALEXANDRE") - String(14) tronquait silencieusement (erreur
    # DB stricte en realite, StringDataRightTruncationError) avant correction.
    titulaire_siret: Mapped[str] = mapped_column(String(64))
    # SIRET/TVA/IREP/HORS-UE/RIDET/TAHITI observes reellement dans la source
    # (`titulaire_typeidentifiant_1`) - le frontend ne doit lier vers
    # l'annuaire public que lorsque ce champ vaut "SIRET".
    titulaire_id_type: Mapped[str | None]

    dureemois: Mapped[int | None]
    datenotification: Mapped[date] = mapped_column(Date, index=True)
    datepublicationdonnees: Mapped[date | None] = mapped_column(Date)
    montant: Mapped[float] = mapped_column(Numeric(15, 2), index=True)

    formeprix: Mapped[str | None]
    offresrecues: Mapped[int | None]
    # Source: chaine libre "oui"/"non" (`marcheinnovant`), convertie a l'ETL.
    marcheinnovant: Mapped[bool | None] = mapped_column(Boolean)
