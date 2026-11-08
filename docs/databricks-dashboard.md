# Premier dashboard Databricks : activité bancaire

[Retour au README](../README.md)

Le premier dashboard utilise un instantané cohérent de l'étoile locale. Il présente l'activité et les labels de fraude PaySim ; il ne constitue pas un modèle prédictif. Le transfert initial est manuel, sans accès direct au Kafka local ni création de ressources AWS payantes. Les capacités et quotas de Free Edition restent applicables.

## 1. Exporter l'étoile publiée

```bash
bash scripts/export-star.sh
```

Le script lit les quatre versions du manifeste étoile, écrit une seule partie Parquet par table, relit chaque fichier pour comparer les données à Delta et publie `manifest.json` en dernier. Les montants restent décimaux et les timestamps sont exportés en microsecondes. Les empreintes SHA256, schémas, effectifs, totaux et versions sont enregistrés. Un dossier existant est refusé ; aucun export précédent n'est remplacé.

Le résultat se trouve sous `data/exports/<identifiant>/` :

```text
dim_transaction_type.parquet
dim_simulation_time.parquet
dim_account.parquet
fact_transaction.parquet
manifest.json
```

Ces fichiers sont ignorés par Git. Ne pas utiliser un export incomplet ou sans manifeste valide. L'export est destiné aux volumes locaux validés : la sortie Parquet unique par table concentre son écriture sur une tâche ; un export partitionné devra être prévu pour des volumes beaucoup plus importants.

## 2. Charger les fichiers dans Databricks Free Edition

Alternative sans téléversement manuel : [import depuis S3 via un volume externe](s3-import.md). La procédure ci-dessous décrit le transfert initial vers un volume managé.

Dans **Catalog**, choisir un catalogue accessible en écriture et créer un schéma de dépôt, par exemple `paysim_uploads`, puis un volume managé, par exemple `exports`. Dans ce volume, créer un dossier portant l'identifiant de l'export et y téléverser les **cinq fichiers** du même dossier local. Garder les noms inchangés.

Le chemin obtenu ressemble à `/Volumes/<catalogue>/paysim_uploads/exports/<identifiant>` ; les éléments entre chevrons doivent être remplacés par les valeurs réelles. Ne pas envoyer les fichiers au dépôt GitHub. Voir [les fichiers dans les volumes Unity Catalog](https://docs.databricks.com/aws/en/volumes/volume-files).

## 3. Importer les tables et préparer la vue

Déployer le notebook [01_import_star.py](../databricks/notebooks/01_import_star.py) et son module voisin [_import_support.py](../databricks/notebooks/_import_support.py) dans le même dossier Workspace (ou utiliser DAB). Ouvrir le notebook Python et sélectionner le calcul serverless disponible. Renseigner les widgets :

- `export_path` : chemin complet du dossier sous `/Volumes/` contenant les cinq fichiers.
- `catalog` : catalogue cible accessible en écriture, proposé par défaut à partir du catalogue courant.

Exécuter le notebook. Il contrôle les empreintes, les effectifs, les clés, les quatre références et les totaux, puis crée un **nouveau schéma par export**, quatre tables Delta managées et une vue `transactions_dashboard` avec les deux rôles des comptes. Il imprime le nom complet de la vue, la date UTC d'export et la version Silver.

Le schéma est créé s'il n'existe pas. Les tables manquantes sont créées par SQL ; les tables existantes sont comparées à l'export et ne sont pas écrasées. Une divergence arrête l'import. Une reprise après interruption est possible en relançant le notebook. La vue est recréée après contrôle des quatre tables ; attendre le message `PASS` pour considérer l'import comme terminé.

Le notebook a été exécuté avec succès dans Databricks, manuellement puis via le job DAB, sur l'export de 250 000 faits. Le bundle [analytique complet](databricks-bundle.md) étend le premier dashboard manuel avec quatre pages et seize analyses.

## 4. Créer le dashboard

Dans **Dashboards**, créer un dashboard nommé **PaySim — Activité bancaire**, puis ajouter trois datasets SQL dans l'onglet **Data**. Pour chaque fichier ci-dessous, remplacer `{{STAR_VIEW}}` par le nom complet de la vue imprimé par le notebook. Ce jeton est un emplacement à remplacer, pas un paramètre de filtre du dashboard. Les datasets doivent contenir uniquement les requêtes SELECT, sans `USE`, `CREATE` ni autre instruction de préparation.

| Dataset | SQL | Visualisation proposée |
| --- | --- | --- |
| Synthèse | [01_activity_summary.sql](../sql/dashboards/01_activity_summary.sql) | Compteurs : transactions, montant total, montant moyen, fraudes, taux de fraude et montant frauduleux |
| Par type | [02_activity_by_type.sql](../sql/dashboards/02_activity_by_type.sql) | Barres : nombre de transactions par type ; tableau complémentaire des montants et taux |
| Par heure simulée | [03_activity_by_hour.sql](../sql/dashboards/03_activity_by_hour.sql) | Courbe : nombre de transactions par `simulation_step`, puis courbe des montants séparée |

Placer les compteurs en haut, les barres et la courbe en dessous. Ne pas mélanger montants et volumes sur une même échelle. Afficher le taux de fraude avec quatre décimales : le taux de référence est 0,0652 %, que deux décimales arrondiraient à 0,07 %.

Ajouter un bloc texte : **Données synthétiques PaySim — instantané exporté le [date du manifeste] — heures de simulation, sans dates réelles ni devise attribuée. Les fraudes correspondent aux labels fournis par le dataset.**

Sur les 250 000 premières lignes actuellement validées, les compteurs attendus sont : 250 000 transactions, montant total 44 796 585 140,23, montant moyen 179 186,340561, 163 fraudes, taux 0,0652 % et montant frauduleux 128 120 119,75. Les volumes par type et par heure doivent se réconcilier avec le total. Les dimensions historiques peuvent contenir des membres sans fait courant ; les graphiques partent de la vue des faits.

Vérifier d'abord l'aperçu et les résultats SQL avant publication. Les modalités de partage seront choisies selon celles disponibles dans le workspace ; aucune publication publique n'est effectuée par les scripts.

## Suite prévue

- Dashboard d'analyse de fraude et exploration des rôles des comptes.
- Tables de suivi dédiées, puis dashboard de qualité et de fraîcheur du pipeline.
- Transfert automatisé et déploiement distant après validation de cette première restitution.

Références : [dashboards dans Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition), [création d'un dashboard](https://docs.databricks.com/aws/en/dashboards/tutorials/create-dashboard), [datasets SQL](https://docs.databricks.com/aws/en/dashboards/manage/data-modeling/datasets).
