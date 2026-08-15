# Contribuer à BudgetCitoyen API

Merci de contribuer au projet ! Voici les conventions à respecter.

## Conventional Commits

Les messages de commit suivent la convention [Conventional Commits](https://www.conventionalcommits.org/fr/),
en français :

```
type(scope): description courte en français
```

Types autorisés :

- `feat` : nouvelle fonctionnalité
- `fix` : correction de bug
- `docs` : documentation uniquement
- `chore` : maintenance, dépendances, configuration
- `refactor` : changement de code sans changement de comportement
- `test` : ajout ou modification de tests
- `perf` : amélioration de performance

Exemple : `feat(budget): ajoute l'endpoint historique par annee`

## Workflow git

- `main` est la branche protégée, reflète la production.
- `develop` est la branche d'intégration.
- Les branches de travail sont créées depuis `develop` et préfixées par `feat/`, `fix/` ou `docs/`
  (ex : `feat/comparateur-annees`).
- Les pull requests sont fusionnées dans `develop` en **squash merge**.

## Avant d'ouvrir une pull request

Vérifiez que les commandes suivantes passent toutes sans erreur :

```bash
ruff check .
black --check .
mypy api
pytest
```
