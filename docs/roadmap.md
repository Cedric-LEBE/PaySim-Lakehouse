# Feuille de route

[Retour au README](../README.md)

## Disponible

| Domaine | Fonctionnalités |
| --- | --- |
| Ingestion | Kafka KRaft local, topic partitionné, producer CSV à débit limité, consumer avec confirmation manuelle |
| Bronze | Structured Streaming, conservation brute, checkpoint durable et reprise des offsets |
| Silver | CDF incrémental, MERGE, reprise, Decimal, quarantaine et conflits d'identité inter-versions |
| Gold | Agrégats incrémentaux CDF, contributions signées, reprise sans double comptage et rapprochement CSV |
| Modèle dimensionnel | Faits incrémentaux, trois dimensions conservées, deux rôles compte, clés stables, reprise et manifeste de versions |
| Orchestration locale | Lanceur complet avec vérification CSV, arrêt sur erreur, nettoyage Kafka, verrou et journaux chronométrés |
| Exploration | Requêtes Spark SQL sur un instantané cohérent de l'étoile |
| Validation | Tests unitaires et scénario incrémental synthétique en GitHub Actions ; tests locaux approfondis de reprise, qualité, précision et jointures |

Le périmètre validé de bout en bout comprend les préfixes de 100, 10 000 et 250 000 transactions. Silver, Gold et le modèle dimensionnel sont incrémentaux après initialisation complète. Le dépôt fournit une plateforme locale de préparation et d'analyse des données ; la prédiction de fraude est une évolution distincte.

## Prochaines étapes locales

- Élargir progressivement l'échantillon pour couvrir plusieurs heures et mesurer RAM, disque et temps de traitement.
- Tester une interruption pendant une écriture Bronze et une perte d'offsets liée à la rétention Kafka.
- Mesurer et optimiser les scans de validation et les MERGE avant le dataset complet.
- Étendre la CI aux scénarios d'interruption et à Kafka lorsque le temps et les ressources d'exécution le permettent.
- Ajouter une restitution BI du modèle dimensionnel.
- Évaluer une approche de détection de fraude avec séparation entraînement/évaluation et prévention des fuites de labels.

## Déploiement et industrialisation

| Étape | Travail prévu avant déploiement |
| --- | --- |
| VPS | Dimensionnement à partir de mesures, budget, persistance, exploitation et accès Kafka sécurisé |
| AWS | Stockage S3, rôles IAM à privilèges minimaux, budget et cycle de vie des ressources |
| Databricks | Vérification des capacités de l'environnement retenu, connectivité Kafka, tables et gouvernance Unity Catalog |
| Livraison | Databricks Asset Bundles, tests, secrets externes et CI/CD |

Ces intégrations ne sont pas implémentées dans la version actuelle. Aucun provisionnement ni coût cloud n'est nécessaire pour le parcours local. Le choix d'une offre VPS ou Databricks sera vérifié au moment du déploiement.
