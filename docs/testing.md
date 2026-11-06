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
| `silver_check.py` | Fixture fournie dans le dépôt | Typage, précision sans arrondi, quarantaine, conflit d'identité, déduplication et reconstructions 1→1→2 |
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

## Relances et interruptions

- Relancer Bronze avec la même table et le même checkpoint vérifie la reprise des offsets déjà traités.
- Relancer Silver et Gold sans changement d'entrée conserve les lignes métier et les indicateurs, tout en créant de nouvelles versions Delta.
- Le test étoile simule une interruption avant publication du manifeste et vérifie que les versions précédemment publiées restent lisibles.

L'interruption pendant une écriture Bronze, la perte de rétention Kafka et un benchmark du dataset complet restent à tester. Il n'y a pas encore de pipeline CI déclaré ; les commandes de ce document sont exécutées localement.
