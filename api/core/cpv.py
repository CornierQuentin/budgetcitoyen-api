"""Nomenclature CPV (Common Procurement Vocabulary, reglement UE 213/2008,
annexe I) - libelles des 46 "divisions" (2 premiers chiffres du code CPV)
reellement presentes dans le dataset DECP source, verifiees par inspection
directe du fichier reel (cf. api.etl.sources, dataset decp-2022-marches-valides).

Ne PAS etendre ce mapping avec des divisions inventees: si une division
absente de ce dict apparait un jour dans une future mise a jour de la source
(donnee live), `api.services.marche_service.repartition_cpv` retombe sur le
code brut plutot que d'inventer un libelle.
"""

CPV_DIVISION_LABELS: dict[str, str] = {
    "03": "Produits agricoles, d'elevage, de peche, de sylviculture",
    "09": "Produits petroliers, combustibles, electricite, energie",
    "14": "Produits d'exploitation miniere, metaux de base",
    "15": "Produits alimentaires, boissons, tabac",
    "16": "Machines agricoles",
    "18": "Vetements, articles chaussants, bagages",
    "19": "Cuir, textile, matieres plastiques, caoutchouc",
    "22": "Imprimes et produits connexes",
    "24": "Produits chimiques",
    "30": "Machines de bureau et de calcul, equipements informatiques",
    "31": "Machines et equipements electriques, eclairage",
    "32": "Equipements de radio, television, telecommunication",
    "33": "Appareils medicaux, produits pharmaceutiques",
    "34": "Equipements de transport et produits auxiliaires",
    "35": "Equipements de securite, incendie, police, defense",
    "37": "Instruments de musique, articles de sport, jeux, jouets",
    "38": "Equipements de laboratoire, d'optique et de precision",
    "39": "Mobilier, articles d'ameublement, electromenager",
    "41": "Ouvrages collectes et purifies (eau)",
    "42": "Machines industrielles",
    "43": "Machines pour l'extraction miniere et la construction",
    "44": "Structures et materiaux de construction",
    "45": "Travaux de construction",
    "48": "Logiciels et systemes d'information",
    "50": "Services de reparation et d'entretien",
    "51": "Services d'installation",
    "55": "Services d'hotellerie, de restauration, de vente au detail",
    "60": "Services de transport",
    "63": "Services annexes des transports, agences de voyage",
    "64": "Services postaux et de telecommunications",
    "65": "Services collectifs (eau, energie)",
    "66": "Services financiers et d'assurance",
    "70": "Services immobiliers",
    "71": "Services d'architecture, d'ingenierie, d'inspection",
    "72": "Services de technologies de l'information",
    "73": "Services de recherche et developpement",
    "75": "Services d'administration publique, de defense",
    "76": "Services relatifs a l'industrie petroliere et gaziere",
    "77": "Services agricoles, sylvicoles, horticoles",
    "79": "Services aux entreprises (droit, conseil, recrutement...)",
    "80": "Services d'enseignement et de formation",
    "85": "Services de sante et services sociaux",
    "90": "Services d'evacuation des eaux usees et de dechets",
    "92": "Services recreatifs, culturels et sportifs",
    "98": "Autres services communautaires, sociaux et personnels",
    # Artefact de saisie observe sur ~0,5% des lignes reelles (codecpv
    # prefixe "INX ", ex. "INX 11200000-2") - PAS une vraie division CPV,
    # cf. api.models.marche_public.codecpv_division.
    "IN": "Non classe (donnee source anormale)",
}
