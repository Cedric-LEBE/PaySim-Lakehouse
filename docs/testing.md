# Tests et reproductibilité

[Retour au README](../README.md)

Exécuter les commandes depuis la racine du dépôt, après installation des dépendances. Les tests d'intégration utilisent des ressources locales et ne provisionnent aucun service cloud.

## Tests unitaires

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
```

Ces tests couvrent la précision et l'identité des événements producer, la validation consumer et l'absence de confirmation d'offset lorsque le message, sa clé ou sa sortie sont en échec. Kafka, Spark et le CSV Kaggle ne sont pas requis.

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
| `silver_check.py` | Fixture fournie dans le dépôt | Typage, notation scientifique exacte, rejet des sous-centimes et débordements, précision sans arrondi, quarantaine, conflit d'identité, déduplication et reconstructions 1→1→2 |
| `gold_check.py` | CSV vérifié et Silver contenant exactement les 100 premières transactions PaySim | Rapprochement des agrégats avec le CSV, relance stable, cas vide/sans fraude, distinction flag/label, rejet des doublons |
| `star_check.py` | Silver contenant exactement les 100 premières transactions PaySim | Grain, reconstitution par jointures, totaux SQL, clés stables, deux rôles du compte, références et interruption de publication |

```bash
BANKING_SPARK_APP=tests/integration/silver_check.py bash scripts/bronze.sh
BANKING_SPARK_APP=tests/integration/gold_check.py bash scripts/bronze.sh
BANKING_SPARK_APP=tests/integration/star_check.py bash scripts/bronze.sh
```

Les données et tables de test sont isolées sous `data/integration/`. Les événements supplémentaires créés par ces tests ne sont pas ajoutés aux tables principales. Ces résultats locaux s'accumulent d'une exécution à l'autre et sont ignorés par Git.

## Reproduire l'échantillon de référence

Suivre le [démarrage du README](../README.md#démarrage-local) sur des destinations neuves, avec le CSV contrôlé et les 100 premières lignes publiées une seule fois. Il produit 100 événements Bronze, 100 transactions Silver et zéro ligne en quarantaine.

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

Le contrôle compare chaque champ métier Silver au CSV, rapproche les agrégats globaux/par type/par étape et vérifie les identités et totaux des faits ainsi que les effectifs des dimensions publiées. Il lit les tables sans les modifier. Son paramètre `--max-messages` permet de choisir un autre préfixe du CSV ; ce contrôle conserve ce préfixe en mémoire et vise des volumes locaux bornés.

Résultats attendus : 10 000 transactions, montant total 1 035 466 899,49, 68 fraudes (0,68 %) et montant frauduleux 36 275 527,43. L'étoile contient 10 000 faits, 5 types, 7 heures relatives et 16 397 comptes. Les tests `gold_check.py` et `star_check.py` ci-dessus restent dédiés au préfixe de 100 lignes ; utiliser `replay_check.py` pour ce périmètre élargi.

## Relances et interruptions

- Relancer Bronze avec la même table et le même checkpoint vérifie la reprise des offsets déjà traités.
- Relancer Silver et Gold sans changement d'entrée conserve les lignes métier et les indicateurs, tout en créant de nouvelles versions Delta.
- Le test étoile simule une interruption avant publication du manifeste et vérifie que les versions précédemment publiées restent lisibles.

L'interruption pendant une écriture Bronze, la perte de rétention Kafka et un benchmark du dataset complet restent à tester. Les tests d'intégration de ce document s'exécutent localement ; la CI couvre actuellement les tests unitaires.

## GitHub Actions

Le workflow [CI](../.github/workflows/ci.yml) se déclenche à chaque push, à chaque ouverture ou mise à jour d'une pull request, et manuellement via `workflow_dispatch`. Sur Ubuntu 24.04 avec Python 3.13, il installe uniquement `requirements.txt`, vérifie les dépendances puis exécute :

```bash
PYTHONPATH=src python -m unittest discover -s tests -p 'test_*.py' -v
```

Les tests utilisent la fixture du dépôt et des mocks. Ils ne nécessitent ni service Kafka, ni Spark, ni CSV Kaggle et ne traitent pas le dataset complet. Les tests d'intégration restent une étape distincte.

Le job est limité à cinq minutes. Les téléchargements pip sont mis en cache à partir de requirements.txt ; les exécutions précédentes du même workflow et de la même référence sont annulées si une nouvelle arrive. Le workflow demande seulement la lecture du dépôt ; aucun secret supplémentaire ni accès cloud n'est configuré. Les versions des actions sont figées par SHA, avec leur tag indiqué en commentaire.

Après avoir poussé le fichier du workflow, ouvrir **Actions → CI → Unit tests (Python 3.13)** pour consulter le résultat et le détail des six tests actuels. Une coche verte valide ce périmètre de tests ; elle ne constitue pas une validation du rejeu intégral PaySim. Le bouton **Run workflow** devient disponible lorsque le workflow est présent sur la branche par défaut.

Référence : [construire et tester un projet Python avec GitHub Actions](https://docs.github.com/en/actions/tutorials/build-and-test-code/python).
