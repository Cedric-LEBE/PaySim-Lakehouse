# Feuille de route

[Retour au README](../README.md)

## Périmètre

Le projet couvre le Data Engineering et l'infrastructure. Le machine learning ne fait pas partie des critères de livraison.

## Réalisé

- Kafka KRaft, producer CSV à débit limité, Bronze Structured Streaming et checkpoints.
- Silver, Gold et étoile incrémentaux, contrôles qualité, déduplication, reprise et rapprochement.
- Traitement validé des **6 362 620 transactions** sur EC2, dont **8 213 fraudes étiquetées**.
- Export vérifié, publication S3 et import Delta Databricks via un volume externe Unity Catalog en lecture seule.
- Quatre pages de dashboard en anglais ; refonte et lisibilité confirmées visuellement.
- CI GitHub Actions et déploiement DAB ; 42 tests unitaires et intégration Spark/Delta.

## Travail restant, dans l'ordre

1. **Infrastructure reproductible** : [inventaire AWS et adoption Terraform](../infrastructure/aws/README.md). Inventaire reçu, bucket d'état créé (6 ressources), adoption de 12 ressources réalisée sans modification AWS, plans finaux sans différence. Validation Terraform sans identifiants AWS ajoutée à la CI. La généralisation en modules réutilisables et la supervision de dérive restent des évolutions possibles.
2. **Performances** : exploiter les rapports réels par étape, mesurer les scans et rapprochements cumulatifs, optimiser sans réduire les garanties de qualité et de reprise. La durée proche de 24 heures est un constat utilisateur à préciser à partir des rapports, pas un benchmark instrumenté.
3. **Orchestration et supervision** : compléter le lanceur et ses journaux par un parcours EC2 → export S3 → import Databricks, avec détection des échecs, reprise contrôlée et prévention des exécutions concurrentes.
4. **Exploitation** : vérifier sauvegarde/restauration, documenter les arrêts, coûts mesurés, rétention et démantèlement. Un export de l'étoile n'est pas à lui seul une sauvegarde des tables et checkpoints de reprise.
5. **Portfolio** : harmoniser le README anglais et les documents avec l'architecture réelle, ajouter captures, résultats et procédure de démonstration reproductible.

La livraison sera considérée complète après validation de ces cinq points. Aucun VPS supplémentaire ni extension prédictive n'est nécessaire.
