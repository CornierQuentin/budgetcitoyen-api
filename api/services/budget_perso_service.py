"""Logique metier de la simulation de budget personnel (contribution individuelle)."""

from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas.budget_perso import BudgetPersoResponse


async def calculer_budget_perso(revenu_net: float, db: AsyncSession) -> BudgetPersoResponse:
    """Estime la contribution individuelle au budget de l'Etat a partir du revenu net."""
    # TODO: methode de calcul TVA estimee non definie par le CDC - a specifier avant implementation
    raise NotImplementedError(
        "Le calcul du budget personnel necessite une methodologie validee (cf. TODO ci-dessus)."
    )
