"""Tests unitaires du calcul de l'impot sur le revenu par tranches (bareme progressif)."""

import pytest

from api.services.bareme_ir import calculer_ir


def test_revenu_imposable_nul_ou_negatif_ne_produit_aucun_impot() -> None:
    assert calculer_ir(0.0) == 0.0
    assert calculer_ir(-100.0) == 0.0


def test_revenu_imposable_dans_la_tranche_a_taux_zero() -> None:
    assert calculer_ir(10_000.0) == 0.0
    assert calculer_ir(11_600.0) == 0.0


def test_revenu_imposable_a_cheval_sur_deux_tranches() -> None:
    # 11 600 premiers euros a 0%, puis (20 000 - 11 600) = 8 400 euros a 11%.
    assert calculer_ir(20_000.0) == pytest.approx(924.0)


def test_revenu_imposable_traversant_trois_tranches() -> None:
    # 11 600 @ 0% + (29 579-11 600) @ 11% + (30 000-29 579) @ 30%.
    attendu = (29_579.0 - 11_600.0) * 0.11 + (30_000.0 - 29_579.0) * 0.30
    assert calculer_ir(30_000.0) == pytest.approx(attendu)


def test_revenu_imposable_tres_eleve_atteint_la_derniere_tranche() -> None:
    # Verifie que le taux marginal maximal (45%) s'applique bien au-dela du
    # dernier seuil fini du bareme (181 917 euros).
    revenu = 200_000.0
    attendu = (
        (29_579.0 - 11_600.0) * 0.11
        + (84_577.0 - 29_579.0) * 0.30
        + (181_917.0 - 84_577.0) * 0.41
        + (revenu - 181_917.0) * 0.45
    )
    assert calculer_ir(revenu) == pytest.approx(attendu)


def test_impot_croissant_avec_le_revenu() -> None:
    """Verifie la monotonie du bareme (aucune tranche mal ordonnee/aberrante)."""
    revenus = [0.0, 5_000.0, 15_000.0, 40_000.0, 100_000.0, 300_000.0]
    impots = [calculer_ir(r) for r in revenus]
    assert impots == sorted(impots)
