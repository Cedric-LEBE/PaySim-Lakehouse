# Livraison du projet

[Retour au README](../README.md)

Le périmètre est **Data Engineering et infrastructure**. Le machine learning et un VPS supplémentaire ne sont pas requis.

## Validations réalisées

- Kafka KRaft, rejeu CSV borné, Bronze Structured Streaming et checkpoints.
- Silver, Gold et étoile incrémentaux : qualité, quarantaine, déduplication, reprise et rapprochement.
- Les **6 362 620 transactions** traitées sur EC2 et importées dans Databricks depuis S3 ; quatre pages analytiques en anglais.
- Adoption Terraform de 12 ressources existantes sans modification, bootstrap de 6 ressources pour l’état distant, plans sans différence et validation CI sans identifiants AWS.
- Référence de performance issue de 128 rapports : **26,72 heures cumulées**, sans promesse de gain. Benchmark isolé de 10 000 lignes ; optimisations supplémentaires reportées.
- Supervision locale des métadonnées et publication S3 → job Databricks. Test réel de 10 000 faits puis relance réutilisant le même run réussi le 16 novembre 2026.
- Snapshot EBS restauré sur un volume distinct : empreinte CSV, cohérence curseur/manifeste et lecture des quatre tables étoile à leurs versions sauvegardées, avec effectifs conformes.
- Documentation des résultats, limites, restauration et captures portfolio.

Voir le [registre de validation](validation-record.md) pour distinguer les contrôles réellement exécutés des garanties non testées.

## Derniers éléments de livraison

1. Confirmer la suppression des deux volumes de restauration temporaires ; conserver le snapshot et le volume original. Arrêter EC2 hors utilisation.
2. Les huit captures reçues sont intégrées au [portfolio](portfolio.md), avec légendes et identifiants personnels masqués. Vérifier la CI du commit final avant publication.
3. Relever les coûts AWS facturés sur la période : aucune facture finale ni mesure de RPO/RTO n’a été fournie.
4. Relire et publier les documents et captures après les contrôles du dépôt.

## Évolutions facultatives

Optimisations mesurées, modules Terraform réutilisables, supervision continue et alertes automatiques, sauvegardes planifiées et test de redémarrage complet sur une nouvelle instance. Ces évolutions ne sont pas présentées comme déjà réalisées.
