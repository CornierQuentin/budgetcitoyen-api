"""Tests du router /api/v1/depenses-fiscales."""

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from api.models.depense_fiscale import DepenseFiscale, StatutMontant


async def _seed_depenses_fiscales(db_session: AsyncSession) -> None:
    db_session.add_all(
        [
            DepenseFiscale(
                annee=2021,
                numero="1",
                categorie="Impôt sur le revenu",
                sous_categorie="Sous-categorie",
                sous_sous_categorie=None,
                libelle="Mesure A",
                beneficiaire="Menages",
                montant_millions=100.0,
                statut_montant=StatutMontant.CHIFFRE,
                methode_chiffrage="Simulation",
            ),
            DepenseFiscale(
                annee=2021,
                numero="2",
                categorie="Taxe sur la valeur ajoutée",
                sous_categorie="Sous-categorie",
                sous_sous_categorie=None,
                libelle="Mesure B",
                beneficiaire="Entreprises",
                montant_millions=None,
                statut_montant=StatutMontant.NON_CALCULABLE,
                methode_chiffrage=None,
            ),
        ]
    )
    await db_session.commit()


async def test_ressource_inexistante_retourne_404_rfc7807(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/depenses-fiscales/2099")

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    body = response.json()
    assert body["status"] == 404


async def test_obtenir_depenses_fiscales_existantes_retourne_200(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_depenses_fiscales(db_session)

    response = await async_client.get("/api/v1/depenses-fiscales/2021")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    numeros = {item["numero"] for item in body}
    assert numeros == {"1", "2"}
    non_chiffree = next(item for item in body if item["numero"] == "2")
    assert non_chiffree["statut_montant"] == "non_calculable"
    assert non_chiffree["montant_millions"] is None


async def test_lister_annees_vide_retourne_200(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/depenses-fiscales/annees")

    assert response.status_code == 200
    assert response.json() == []


async def test_lister_annees_retourne_les_annees_disponibles(
    async_client: AsyncClient, db_session: AsyncSession
) -> None:
    await _seed_depenses_fiscales(db_session)

    response = await async_client.get("/api/v1/depenses-fiscales/annees")

    assert response.status_code == 200
    assert response.json() == [2021]
