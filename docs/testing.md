# Tests et reproductibilité

[Retour au README](../README.md)

Exécuter les commandes depuis la racine du dépôt, après installation des dépendances. Les tests d'intégration utilisent des ressources locales et ne provisionnent aucun service cloud.

## Tests unitaires

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
```

Les trente-neuf tests couvrent également l’ordre des étapes du lanceur, le blocage après échec, le nettoyage Kafka et le refus d’un CSV incorrect avant publication. Ils couvrent la précision et l'identité des événements producer, la validation consumer et l'absence de confirmation d'offset lorsque le message, sa clé ou sa sortie sont en échec. Kafka, Spark et le CSV Kaggle ne sont pas requis.

Les contrôles de déploiement ajoutent sept tests : configuration absente, mauvaise branche/événement, commit exact, attente de CI, jobs ignorés et refus de revenir à un ancien succès après un échec plus récent. Ils ne nécessitent aucun accès Databricks.

Les neuf contrôles ajoutés pour la reprise couvrent les plages CSV et identités absolues, l'initialisation vérifiée du curseur, les lots successifs et relances sans publication, l'échec d'initialisation, le rejeu d'une plage en attente même après publication des tables, le refus d'un CSV/manifeste changé ou d'un checkpoint manquant, le plafond estimé de publication et l'ordre drainage → envoi → vérification cumulée. Les étapes externes sont simulées dans ces tests unitaires.

## Reprise CSV : intégration isolée

```bash
BANKING_SPARK_APP=tests/integration/replay_resume_check.py bash scripts/bronze.sh
```

Ce scénario utilise un CSV synthétique de quatre lignes et les véritables plages du producteur, curseur, builders Spark/Delta Silver/Gold/étoile. Il remplace l'adaptateur shell/Kafka par des écritures Bronze utilisant `bronze_frame`, sous un dossier unique de `data/integration/`. Il initialise depuis deux faits vérifiés, interrompt après livraison de la ligne 3, rejoue cette ligne, poursuit à 4 et vérifie cinq lignes Bronze pour quatre faits uniques, les montants, la quarantaine vide et une relance sans publication. Il ne teste pas le broker Kafka ni une coupure système pendant une écriture de fichier. Le test Kafka Bronze reste distinct. Ce scénario est également exécuté en CI, après le parcours incrémental existant.

## Import d’un export S3 / volume

```bash
BANKING_SPARK_APP=tests/integration/import_star_check.py bash scripts/bronze.sh
```

Le vrai notebook est exécuté avec Spark/Delta sur un export synthétique de deux transactions. Les widgets et la résolution du stockage S3/volume sont simulés ; aucune connexion cloud n'est établie. Il vérifie les empreintes et les totaux avant écriture, la conservation des Decimal, les clés et références, la reprise sans écrasement et le refus d'une table divergente. Le scénario est exécuté en CI. Huit tests unitaires supplémentaires couvrent la résolution sécurisée des chemins, les métadonnées et empreintes. Voir [le guide S3](s3-import.md) pour les contrôles à réaliser dans le workspace avant un import distant complet.

## Tests Kafka

Démarrer le broker et préparer le topic avant les tests :

```bash
bash scripts/kafka.sh start
bash scripts/kafka.sh topic
bash scripts/kafka.sh smoke
bash scripts/kafka.sh persistence
```

| Test | Commande | Prérequis supplémentaires | Vérifications |
| --- | --- | --- | --- |
| Consumer | `PYTHONPATH=src .venv/bin/python tests/integration/consumer_check.py` | Transactions PaySim déjà publiées sur le topic principal | Reprise des offsets 0 puis 1 avec le même groupe, absence de rejeu, inspection de transactions réelles |
| Bronze | `BANKING_SPARK_APP=tests/integration/bronze_check.py bash scripts/bronze.sh` | Dépendances Spark installées | JSON malformé conservé, Decimal, reprise 2→2→3, offsets uniques, rejeu métier conservé et changement de source refusé |

Les tests créent puis suppriment des topics temporaires. Le test consumer inspecte aussi jusqu'à 200 événements du topic principal et attend au moins un événement dont le dataset ID commence par `paysim-sha256-`. La rétention Kafka doit encore permettre sa lecture. Des groupes de lecture de test peuvent subsister jusqu'à leur expiration côté Kafka.

Après les tests qui utilisent le broker :

```bash
bash scripts/kafka.sh stop
```

## Tests Spark sans Kafka

Ces tests s'exécutent successivement ; Kafka peut rester arrêté.

| Test | Prérequis | Périmètre |
| --- | --- | --- |
| `dashboard_analytics_check.py` | Étoile publiée avec exactement les 250 000 premières lignes | Seize analyses, totaux réconciliés, paramètres combinés, concentration et cas vide ; test local |
| `export_star_check.py` | Fixture synthétique du dépôt | Export Parquet relu, empreintes, montants décimaux, refus d’écrasement et trois requêtes dashboard ; test local |
| `pipeline_check.py` | Fixture synthétique du dépôt uniquement | Bronze → Silver → Gold → étoile, 2→3→2 faits, relance sans écriture, rejeu métier, conflit retiré, parité avec le batch et quatre références ; exécuté en CI |
| `silver_incremental_check.py` | Fixture fournie dans le dépôt | CDF, relance sans écriture, conflits entre commits, parité batch, interruption entre MERGE et avant curseur, arrivées pendant reprise, lecteurs bloqués et rejet des suppressions Bronze |
| `silver_check.py` | Fixture fournie dans le dépôt | Typage, notation scientifique exacte, rejet des sous-centimes et débordements, précision sans arrondi, quarantaine, conflit d'identité, déduplication et reconstructions 1→1→2 |
| `gold_incremental_check.py` | Fixtures créées dans le test | Parité de tous les indicateurs avec le batch complet, précision des centimes, insertions, corrections, retraits, groupes disparus, cas vide, relance sans écriture, interruption entre MERGE et arrivées pendant reprise |
| `gold_check.py` | CSV vérifié et Silver contenant exactement les 100 premières transactions PaySim | Rapprochement des agrégats avec le CSV, relance stable, cas vide/sans fraude, distinction flag/label, rejet des doublons |
| `star_incremental_check.py` | Fixtures créées dans le test | Parité des faits avec le batch, inclusion des dimensions courantes, conservation des dimensions historiques et clés, corrections, retraits, vide, relance sans écriture et ancien manifeste lisible pendant reprise |
| `star_check.py` | Silver contenant exactement les 100 premières transactions PaySim | Grain, reconstitution par jointures, totaux SQL, clés stables, deux rôles du compte, références et interruption de publication |

```bash
BANKING_SPARK_APP=tests/integration/export_star_check.py bash scripts/bronze.sh
BANKING_SPARK_APP=tests/integration/pipeline_check.py bash scripts/bronze.sh
BANKING_SPARK_APP=tests/integration/silver_check.py bash scripts/bronze.sh
BANKING_SPARK_APP=tests/integration/silver_incremental_check.py bash scripts/bronze.sh
BANKING_SPARK_APP=tests/integration/gold_incremental_check.py bash scripts/bronze.sh
BANKING_SPARK_APP=tests/integration/star_incremental_check.py bash scripts/bronze.sh
BANKING_SPARK_APP=tests/integration/gold_check.py bash scripts/bronze.sh
BANKING_SPARK_APP=tests/integration/star_check.py bash scripts/bronze.sh
```

Les données et tables de test sont isolées sous `data/integration/`. Les événements supplémentaires créés par ces tests ne sont pas ajoutés aux tables principales. Ces résultats locaux s'accumulent d'une exécution à l'autre et sont ignorés par Git.

## Reproduire l'échantillon de référence

Suivre le [démarrage du README](../README.fr.md#démarrage-local) sur des destinations neuves, avec le CSV contrôlé et les 100 premières lignes publiées une seule fois. Il produit 100 événements Bronze, 100 transactions Silver et zéro ligne en quarantaine.

Le bilan attendu est de 2 transactions étiquetées frauduleuses, 362,00 de montant frauduleux et 11 644 940,90 de montant total. L'étoile contient 100 faits, 4 types observés, 1 heure relative et 189 comptes distincts.

La publication de fixtures ou les rejeux changent les comptes Bronze. Silver conserve un seul événement admissible par event_id et met les autres datasets en quarantaine. Les tests Gold et étoile vérifient un échantillon précis ; les adapter avant d'élargir volontairement le périmètre Silver.

## Rejeu de 10 000 lignes

Après vérification du CSV, publier les 10 000 premières lignes avec le même dataset ID que l'échantillon initial :

```bash
bash scripts/kafka.sh start
bash scripts/kafka.sh topic
PYTHONPATH=src .venv/bin/python -m banking.producer \
  --csv data/raw/PS_20174392719_1491204439457_log.csv \
  --dataset-id paysim-sha256-16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b \
  --max-messages 10000 --rate 100
bash scripts/bronze.sh --max-offsets 1000
bash scripts/kafka.sh stop
bash scripts/silver.sh
bash scripts/gold.sh
bash scripts/star.sh
BANKING_SPARK_APP=tests/integration/replay_check.py bash scripts/bronze.sh --max-messages 10000
```

Exécuter les étapes successivement et s'arrêter en cas d'échec. Conserver la table Bronze et son checkpoint existants. Le producer repart du début ; Silver doit conserver exactement 10 000 identités distinctes. Les comptes Bronze, doublons et quarantaine dépendent des publications antérieures.

Le contrôle compare chaque champ métier Silver au CSV, rapproche les agrégats globaux/par type/par étape et vérifie les identités et totaux des faits ainsi que les effectifs des dimensions publiées. Les membres attendus du CSV doivent être présents ; des membres historiques supplémentaires sont autorisés dans les dimensions incrémentales. Il lit les tables sans les modifier. Son paramètre `--max-messages` permet de choisir un autre préfixe du CSV. Le CSV est lu ligne par ligne et les transactions Silver sont transférées vers Python une partition à la fois après répartition par plage (128 partitions demandées) et tri par ligne source. Les petits agrégats par type/étape restent en Python ; les contrôles de comptes distincts sont exécutés dans Spark. Toutes les transactions ne sont plus collectées simultanément.

Résultats attendus : 10 000 transactions, montant total 1 035 466 899,49, 68 fraudes (0,68 %) et montant frauduleux 36 275 527,43. L'étoile contient 10 000 faits, 5 types, 7 heures relatives et 16 397 comptes. Les tests `gold_check.py` et `star_check.py` ci-dessus restent dédiés au préfixe de 100 lignes ; utiliser `replay_check.py` pour ce périmètre élargi.

## Palier de 250 000 lignes

Le lanceur a été validé sur les 250 000 premières lignes, après les rejeux précédents :

```bash
bash scripts/pipeline.sh --max-messages 250000 --rate 1000 --max-offsets 5000
```

| Contrôle | Résultat observé |
| --- | ---: |
| Livraisons Kafka confirmées | 250 000 |
| Événements Bronze, historique compris | 280 106 |
| Transactions Silver et faits uniques | 250 000 |
| Doublons retirés dans Silver | 30 100 |
| Quarantaine, anciens événements synthétiques | 6 |
| Montant total | 44 796 585 140,23 |
| Transactions frauduleuses | 163 (0,0652 %) |
| Montant frauduleux | 128 120 119,75 |
| Types / heures relatives / comptes | 5 / 14 / 365 533 |

La comparaison Silver champ par champ au CSV, les agrégats Gold et les contrôles de l'étoile ont réussi. Les comptes Bronze/doublons/quarantaine dépendent de l'historique ; ils ne sont pas les résultats attendus d'un environnement neuf.

L'exécution locale a duré 542,12 s (environ 9 min 02 s), dont 316,72 s de publication, 75,44 s pour Bronze, 38,49 s pour Silver, 20,07 s pour Gold, 35,31 s pour l'étoile et 38,27 s de contrôle final. Ces temps ne constituent pas une garantie de performance.

La somme des RSS des processus hôtes du pipeline, échantillonnée toutes les 5 s, a atteint environ 771 Mio. Elle exclut la machine virtuelle Docker, peut compter des pages partagées plusieurs fois et ne constitue pas le pic mémoire total de la machine. Kafka conserve sa limite Compose de 1 Gio ; Spark conserve son heap de 768 Mio et ses deux threads. Les dossiers Bronze/Silver/Gold, historique Delta compris, occupaient environ 276 Mio après exécution ; ce chiffre exclut Kafka, le CSV, les dépendances et les caches Spark.

Les journaux, `report.json` et les mesures ponctuelles `resources.json` de cette validation sont conservés localement sous `data/runs/`, hors Git. Le lanceur produit les durées ; la mesure RSS a été réalisée par un observateur externe pour cette validation et n'est pas intégrée au lanceur.

## Relances et interruptions

- Relancer Bronze avec la même table et le même checkpoint vérifie la reprise des offsets déjà traités.
- Relancer Silver et Gold sans changement d'entrée conserve les lignes métier et les indicateurs, tout en créant de nouvelles versions Delta.
- Le test étoile simule une interruption avant publication du manifeste et vérifie que les versions précédemment publiées restent lisibles.

L'interruption pendant une écriture Bronze, la perte de rétention Kafka et un benchmark du dataset complet restent à tester. Tous ces tests peuvent s'exécuter localement ; la CI exécute les tests unitaires, `pipeline_check.py`, `replay_resume_check.py` et `import_star_check.py`. Les scénarios détaillés d'interruption et les tests Kafka restent locaux.

## GitHub Actions

Le workflow [CI](../.github/workflows/ci.yml) se déclenche à chaque push, à chaque ouverture ou mise à jour d'une pull request, et manuellement via `workflow_dispatch`. Les deux jobs utilisent Ubuntu 24.04 et Python 3.13, avec lecture seule du dépôt et actions figées par SHA.

1. **Unit tests (Python 3.13)** installe `requirements.txt`, vérifie les dépendances et exécute les trente-neuf tests unitaires sans Spark ni Kafka. Ce job est limité à cinq minutes.
2. **Incremental pipeline (Spark / Delta)** attend la réussite du premier job, configure Java 17 Temurin, crée `.venv` et installe `requirements-streaming.txt`. Il est limité à quinze minutes et exécute :

```bash
BANKING_SPARK_APP=tests/integration/pipeline_check.py bash scripts/bronze.sh
```

Une seconde étape exécute `replay_resume_check.py` pour couvrir le curseur CSV et sa reprise après livraison partielle. Une troisième exécute `import_star_check.py` pour tester le notebook d’import sur un export synthétique isolé.

Le scénario `pipeline_check.py` utilise les deux lignes synthétiques du dépôt et deux événements supplémentaires construits par le test. Il crée les lignes Bronze via le même `bronze_frame` que l'ingestion, sans connecter de broker. Il vérifie les totaux attendus à chaque phase (2→3→2 faits), la parité avec des reconstructions complètes Gold/étoile, les quatre références aux dimensions et la conservation des dimensions historiques. Une relance inchangée doit laisser les versions de toutes les tables et le manifeste identiques. Un rejeu métier doit être dédupliqué ; un conflit doit retirer la transaction de Silver, des agrégats Gold et des faits, et placer ses deux occurrences en quarantaine.

Aucun service Docker, dataset Kaggle ou secret cloud n'est utilisé. Spark et les dépendances Maven sont téléchargés si nécessaire ; le lanceur conserve ses limites locales de deux threads et 768 Mio de heap. Les seules dépendances mises en cache par le workflow sont celles de pip ; le cache Ivy reste propre à l'exécution GitHub. Les sorties isolées sous `data/integration/` restent hors Git. Les journaux de l'étape sont consultables dans Actions.

La concurrence annule une exécution ancienne de la même référence lorsqu'une nouvelle arrive. Aucun déploiement n'est effectué. Après push, ouvrir **Actions → CI** et vérifier que les deux jobs sont verts. Un succès de ce scénario valide le parcours Delta synthétique ; il ne valide ni la connectivité Kafka, ni les interruptions couvertes par les autres tests locaux, ni le rejeu du dataset complet.

Références : [construire et tester Python avec GitHub Actions](https://docs.github.com/en/actions/tutorials/build-and-test-code/python), [configuration de Java avec setup-java](https://github.com/actions/setup-java).


## Présentation du dashboard complet

Les quatre contrôles supplémentaires du générateur vérifient les chevauchements et largeurs de grille, les formats pourcentage sans mise à l'échelle des données ni conversion des rangs en pourcentages, les compteurs entiers et références obsolètes, et le maintien des soldes après séparation du tableau de détail. La suite compte désormais 39 tests unitaires. Les 17 SQL des datasets sont inchangés. Les KPI du nouvel instantané Databricks ont été rapprochés avec le manifeste S3 (6 362 620 faits et 8 213 fraudes) ; le contrôle local historique `dashboard_analytics_check.py` reste volontairement dédié au préfixe de 250 000 lignes. Le rendu visuel final doit être vérifié dans le client Databricks.
