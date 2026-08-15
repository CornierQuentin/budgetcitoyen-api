"""Normalisation des libelles bruts issus des sources de donnees (Phase 1)."""


def normalize_mission_name(nom_csv: str, annee: int) -> str:
    """Normalise le nom d'une mission tel qu'il apparait dans les CSV source.

    Doit gerer les changements d'intitules d'une annee sur l'autre via la table
    MissionAlias. Non implemente: la logique de correspondance reste a definir.
    """
    raise NotImplementedError("Normalisation des noms de mission - Phase 1")
