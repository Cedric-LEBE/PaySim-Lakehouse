# Importer un export S3 vérifié dans Databricks

Le pipeline EC2 écrit ses tables et checkpoints sur EBS. Il ne doit pas être modifié pendant une exécution. Après validation d'un instantané, l'export portable contient quatre fichiers Parquet et `manifest.json`. Le job Databricks importe ces fichiers dans des tables Delta managées ; il ne consomme pas Kafka et ne traite pas le CSV brut.

## Accès et prérequis

La lecture du fichier de connectivité depuis Spark a été validée dans le workspace Free Edition pour cette configuration :

- Bucket : `paysim-lakehouse-968077439279-eu-north-1-an`, région `eu-north-1`.
- External location : `paysim_exports`, URL `s3://paysim-lakehouse-968077439279-eu-north-1-an/exports/`.
- Storage credential : `paysim_exports_read`, rôle dédié `PaySimDatabricksReadRole`.
- Lecture seule côté IAM et Unity Catalog, file events désactivés (`enable_file_events=false` et `effective_enable_file_events=false`).

Le volume externe `workspace.paysim_uploads.s3_exports` a ensuite été créé et un premier import réel de deux transactions synthétiques a réussi dans un schéma de test distinct. Cela ne valide pas encore l'import des 6 362 620 lignes ni son coût dans les quotas Free Edition. Le rôle EC2 reste distinct et peut publier dans les préfixes du projet ; Databricks peut seulement lire les exports. Aucun identifiant AWS statique n'est nécessaire.

## Créer une fois le volume externe

L'import calcule les SHA-256 en lisant les fichiers par blocs de 1 Mio. Un **volume externe** permet à Python et Spark de lire les mêmes objets S3 via `/Volumes/...`, sans collecter les fichiers binaires dans le driver et sans copie préalable vers un volume managé.

Dans un notebook Databricks, après vérification qu'aucun volume ou table n'occupe déjà ce chemin :

```sql
CREATE EXTERNAL VOLUME workspace.paysim_uploads.s3_exports
LOCATION 's3://paysim-lakehouse-968077439279-eu-north-1-an/exports/';
```

Ce DDL doit être exécuté une seule fois par un utilisateur autorisé (`USE CATALOG`, `USE SCHEMA`, `CREATE VOLUME` et `CREATE EXTERNAL VOLUME` sur l'external location). Un nom déjà existant provoque une erreur : examiner `DESCRIBE VOLUME` plutôt que de remplacer un volume ou masquer une mauvaise association avec `IF NOT EXISTS`. Le job d'import ne crée pas ce volume.

Vérifier sa configuration :

```sql
DESCRIBE VOLUME workspace.paysim_uploads.s3_exports;
```

Puis vérifier l'accès Python (cellule Python) :

```python
from pathlib import Path
print(Path('/Volumes/workspace/paysim_uploads/s3_exports/_connectivity/check.txt').read_text())
```

Le texte attendu est `PaySim S3 connectivity check`. Le notebook d'import synthétique a déjà lu les fichiers via ce volume en Python et Spark ; la réussite de `spark.read.text('s3://...')` seule ne remplacerait pas ce contrôle FUSE/Python. L'identité d'exécution du job doit aussi avoir `READ VOLUME`, `USE SCHEMA` et `USE CATALOG`.

## Publier un instantané S3

Attendre la fin du pipeline et la validation de son curseur. Ne pas lancer un export sur les tables en cours de modification. Depuis EC2, créer un nouvel export avec `bash scripts/export-star.sh`. Le script imprime son dossier de sortie.

Utiliser un identifiant d'export neuf et conserver exactement les cinq noms. Copier d'abord les quatre Parquet dans `exports/<export_id>/`, vérifier leurs transferts, puis téléverser **manifest.json en dernier**. Ne pas réutiliser ni écraser un dossier déjà publié ; la lecture et les relances supposent un instantané immuable. L'absence de droit `DeleteObject` n'interdit pas l'écrasement par une identité ayant `PutObject`.

Les fichiers de connectivité sous `exports/_connectivity/` ne font pas partie d'un export et ne doivent pas être utilisés comme source d'import.

## Paramètres du notebook et du bundle

Déployer ensemble `databricks/notebooks/01_import_star.py` et son module voisin `_import_support.py`. Le déploiement DAB les synchronise tous les deux. Un import manuel du seul notebook ne suffit plus.

| Paramètre | Exemple / comportement |
| --- | --- |
| `export_path` | `s3://paysim-lakehouse-968077439279-eu-north-1-an/exports/<export_id>/` |
| `s3_volume` | `workspace.paysim_uploads.s3_exports` ; obligatoire avec une URL S3 |
| `catalog` | `workspace`, catalogue des tables cibles |
| `target_schema` | Dans le notebook : vide pour dériver `paysim_<export_id>`, ou nom explicite neuf |
| Variable bundle `schema` | Alimente `target_schema` et le schéma du dashboard ; choisir explicitement le nouveau schéma pour un nouvel export |

Le notebook exécute `DESCRIBE VOLUME` et vérifie que le volume est externe, que son bucket correspond et que l'URL d'export est bien sous sa racine (limite de segment, pas une simple comparaison de chaîne). Les chemins ambigus, traversées et URL avec paramètres sont refusés. Les noms `s3_volume` sont limités à trois identifiants simples `catalog.schema.volume`.

Les anciens chemins `/Volumes/catalog/schema/volume/export` restent acceptés sans `s3_volume`. Les valeurs par défaut du bundle continuent de viser l'ancien export de 250 000 lignes : ce changement de code ne bascule pas le dashboard ni le job vers un nouvel instantané.

Pour GitHub Actions, préparer les variables **du dépôt** suivantes seulement lorsque l'export et son schéma cible sont connus :

- `DATABRICKS_EXPORT_PATH` : URL S3 du nouvel export complet.
- `DATABRICKS_S3_VOLUME` : `workspace.paysim_uploads.s3_exports`.
- `DATABRICKS_SCHEMA` : nom du nouveau schéma dérivé de l'identifiant d'export ou choisi explicitement.

La CI teste le code ; le workflow manuel DAB déploie ses définitions, sans lancer l'import. Exécuter ensuite le job avec les paramètres correspondants. Ne pas servir le nouveau dashboard avant le succès de l'import et le rapprochement de ses indicateurs.

## Validation avant publication des tables

Le notebook vérifie :

1. Manifeste borné à 1 Mio, format, identifiant, quatre noms de fichiers exacts et métadonnées de taille/empreinte/effectif.
2. Taille et SHA-256 de **tous** les fichiers avant toute écriture cible ; lecture Python par blocs de 1 Mio.
3. Schéma explicite Spark, montants décimaux, effectifs, clés primaires et quatre références aux dimensions.
4. Totaux des montants, fraudes et montants frauduleux.
5. Tables déjà présentes : schémas et données strictement identiques ; aucune table existante n'est écrasée.
6. Vue dashboard créée après les quatre tables et effectif final conforme au manifeste.

Les tables Delta sont des transactions distinctes. Une interruption peut laisser un schéma partiellement importé ; relancer sur le même export immuable complète les tables manquantes et valide les tables existantes. Il n'y a ni bascule atomique de toutes les tables ni garantie d'authenticité du manifeste par sa seule empreinte : protéger les écritures S3 par IAM.

## Tests

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
BANKING_SPARK_APP=tests/integration/import_star_check.py bash scripts/bronze.sh
```

Les tests unitaires couvrent les associations S3/volume, les refus de chemins hors périmètre, les métadonnées de manifeste et la détection de corruption. L'intégration exécute le vrai notebook avec Spark/Delta sur deux transactions synthétiques, contrôle l'absence d'écriture après corruption ou totaux invalides, les totaux attendus, la relance sans écrasement et le refus d'une table divergente. Les widgets, métadonnées du volume et chemins FUSE sont simulés : ce test n'établit pas une connexion AWS/Databricks. Il est aussi exécuté en CI, sans secrets cloud.

Références : [volumes Unity Catalog](https://docs.databricks.com/aws/en/volumes/), [DESCRIBE VOLUME](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-volume), [modules Python dans Workspace](https://docs.databricks.com/aws/en/files/workspace-modules).
