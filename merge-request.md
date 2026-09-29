## Résumé

Industrialisation de la pipeline Real-Time Sales (API FastAPI → Kafka → Spark Structured Streaming → PostgreSQL) : trois niveaux de tests automatisés, pipeline Jenkins complète, analyse SonarQube et Quality Gate bloquant.

Le rapport détaillé est dans `docs/report.md`.

## Stratégie de tests

- **Unitaires** (21 tests) : règles métier du modèle `Order` et endpoints de l'API, avec Kafka remplacé par un mock. Aucune dépendance externe, exécution en ~1 s.
- **Intégration** (3 tests) : API → Kafka, et Kafka → Spark → PostgreSQL, sur les vrais services Docker.
- **E2E** (1 test) : commande envoyée à l'API puis retrouvée dans `processed_orders` avec les valeurs attendues.
- Attente par polling avec timeout configurable (`PIPELINE_TIMEOUT_SECONDS`), sans `sleep` arbitraire.

## Résultats Jenkins

Pipeline verte de bout en bout (~1 min 15) : Checkout → Install → Unit Tests → Integration Tests → Build → E2E Tests → SonarQube → Quality Gate.
Régression volontaire testée : la pipeline s'arrête dès le stage Unit Tests.

## Résultats SonarQube

- Bugs : 0
- Vulnérabilités : 0 (1 corrigée)
- Code smells : 0 (9 corrigés)
- Duplications : 0,0 %
- Quality Gate : Passed

## Couverture

100 % sur `app/main.py` (71 % au départ).

## Difficultés rencontrées

- Spark démarrait avant la création du topic Kafka et s'arrêtait (`UnknownTopicOrPartitionException`).
- Image Spark ARM exécutée en émulation sur poste x86 : micro-batchs très lents, tests en timeout.
- Jenkins lisait la branche `dev` alors que le travail avait été fait sur `main`.
- Couverture initiale de 71 %, sous le seuil du Quality Gate.
- Job Spark non inclus dans l'analyse SonarQube.

## Corrections apportées

- Service `kafka-init` et dépendance `service_completed_successfully` : ordre de démarrage garanti.
- Image officielle multi-architecture `apache/spark:3.5.7` et délai de test configurable.
- Fusion de `main` dans `dev` (conflits `docker-compose.yml` et `docker/spark/Dockerfile` résolus).
- Ajout de `tests/unit/test_api.py` : couverture à 100 %.
- Analyse SonarQube étendue à `spark/`, avec exclusion de couverture justifiée.
- Checkpoint Spark hors de `/tmp`, réponse 404 documentée, assertions de tests précises.
