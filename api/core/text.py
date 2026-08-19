"""Utilitaires texte partages entre l'ETL et la couche services."""

import unicodedata


def normaliser_pour_recherche(texte: str) -> str:
    """Normalise une chaine pour une recherche insensible a la casse et aux
    accents (ex: "defense" doit trouver "Défense") - meme logique que
    normaliserPourRecherche() cote frontend (Comparateur.tsx/
    DepensesFiscales.tsx). Utilisee des deux cotes d'une meme comparaison:
    a l'ETL pour peupler `marche_public.objet_recherche`, et cote service
    pour normaliser le terme de recherche saisi par l'utilisateur avant de
    le comparer a cette colonne.
    """
    normalized = unicodedata.normalize("NFD", texte)
    return "".join(c for c in normalized if not unicodedata.combining(c)).lower()
