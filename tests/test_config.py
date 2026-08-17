"""Tests du parsing de configuration (`api.core.config`)."""

from api.core.config import Settings


def test_parse_cors_origins_depuis_une_chaine_csv() -> None:
    settings = Settings(cors_origins="http://a.test, http://b.test,,http://c.test")

    assert settings.cors_origins == ["http://a.test", "http://b.test", "http://c.test"]


def test_parse_cors_origins_laisse_une_liste_deja_construite_inchangee() -> None:
    settings = Settings(cors_origins=["http://a.test", "http://b.test"])

    assert settings.cors_origins == ["http://a.test", "http://b.test"]
