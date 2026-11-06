# Exploitation locale

[Retour au README](../README.md)

## Environnement de référence

| Composant | Version ou configuration utilisée |
| --- | --- |
| Hôte de validation | macOS Apple Silicon |
| Python | 3.13 |
| Java | 17 |
| Docker Compose | Support de `up --wait` ; validé avec Compose 5 |
| Apache Kafka | 4.1.2, image officielle, KRaft combiné broker/controller |
| Client Kafka Python | confluent-kafka 2.14.0 |
| Spark / Scala | 4.0.1 / 2.13 |
| Delta Lake | 4.0.1 |

Les dépendances Python sont dans `requirements.txt` et `requirements-streaming.txt`. Installer les dépendances complètes avec les commandes du [démarrage local](../README.md#démarrage-local). Les scripts utilisent l'environnement `.venv` situé à la racine ; son activation dans le terminal est facultative.

## Ressources et stockage

Kafka est limité à 1 Gio de mémoire et 1,5 CPU par Compose, avec un heap Java de 256 à 512 Mio. Les outils Kafka exécutés dans le conteneur utilisent un heap maximal de 128 Mio. Spark s'exécute sur l'hôte avec deux threads et un heap driver de 768 Mio ; son utilisation mémoire totale dépasse le seul heap. La limite Docker ne couvre pas Spark.

Lancer les étapes Spark successivement. Kafka peut être arrêté après l'ingestion Bronze, pendant les traitements Silver/Gold et les tests qui n'en dépendent pas. Les paramètres sont définis dans `infrastructure/kafka/docker-compose.yml` et `scripts/bronze.sh`.

Conserver le dépôt et les données sur un stockage local disponible. Un dossier synchronisé dont les fichiers sont déchargés à la demande peut retarder ou interrompre les lectures. Un environnement virtuel déplacé n'est pas portable : le recréer au nouvel emplacement plutôt que copier ses chemins absolus.

Les dépendances Spark sont téléchargées au premier lancement. Les connecteurs Maven sont mis en cache dans `data/spark/ivy/`, les fichiers de travail Spark dans `data/spark/local/`. Tous les résultats du pipeline restent sous `data/` et sont ignorés par Git.

## Kafka

| Commande | Fonction |
| --- | --- |
| `bash scripts/kafka.sh start` | Démarrer le broker et attendre son état healthy |
| `bash scripts/kafka.sh topic` | Créer le topic s'il est absent, puis afficher ses partitions |
| `bash scripts/kafka.sh status` | Afficher l'état du conteneur |
| `bash scripts/kafka.sh logs` | Afficher les dernières lignes du journal |
| `bash scripts/kafka.sh smoke` | Tester l'envoi/réception sur un topic temporaire |
| `bash scripts/kafka.sh persistence` | Tester la conservation d'un message après redémarrage |
| `bash scripts/kafka.sh stop` | Arrêter le broker en conservant son volume |

Topic principal : `banking.transactions.raw`, trois partitions, réplication 1. Les tests smoke et persistence suppriment leur topic temporaire et n'écrivent pas dans le topic principal.

| Client | Adresse bootstrap |
| --- | --- |
| Python ou Spark sur l'hôte | `localhost:9092` ; Bronze utilise `127.0.0.1:9092` au bootstrap |
| Client sur le réseau Compose | `kafka:29092` |

`localhost:9092` est une adresse de service Kafka, pas une commande terminal ni une page web.

La rétention du topic est de 24 heures ou environ 256 Mio par partition, avec nettoyage asynchrone. Ce paramétrage n'est pas un quota disque strict. La publication du port est limitée au loopback de l'hôte. Avant un accès distant, définir la sécurité réseau et l'authentification du broker.

`stop` conserve les données. La suppression d'un volume Kafka, notamment avec l'option Compose `down -v`, est destructive ; elle ne fait pas partie du démarrage normal. Le nom du projet Compose reste `banking-lakehouse`, indépendamment du nom du dossier Git.

## Producer et consumer

Le producer commence au début du CSV à chaque exécution. `--max-messages` et `--rate` limitent respectivement le nombre de lignes et le débit. Une ligne invalide arrête le rejeu ; les événements déjà envoyés peuvent rester dans Kafka. Utiliser le même dataset ID uniquement pour le même fichier. Voir [les données et le dry-run](data.md).

Après publication de transactions et avec Kafka actif, inspecter les événements :

```bash
PYTHONPATH=src .venv/bin/python -m banking.consumer \
  --group-id banking-inspector-v1 \
  --max-messages 100
```

Le consumer s'arrête à la limite ou après 20 secondes sans nouveau message, une fois les partitions attribuées. Le même group-id reprend après les offsets confirmés. Un nouveau group-id commence aux plus anciens événements encore disponibles. Un résultat de zéro événement peut donc être normal pour un groupe ayant tout lu. Une erreur de validation bloque la confirmation du message concerné.

## Traitements et options

Tous les lanceurs Spark réutilisent `scripts/bronze.sh`. Les chemins ci-dessous sont relatifs à la racine du dépôt.

| Lanceur | Principales options |
| --- | --- |
| `scripts/bronze.sh` | `--bootstrap-servers`, `--topic`, `--output`, `--checkpoint`, `--max-offsets` |
| `scripts/silver.sh` | `--source`, `--output`, `--dataset-id` |
| `scripts/gold.sh` | `--source`, `--output` |
| `scripts/star.sh` | `--source`, `--output` |
| `scripts/explore.sh` | `--star`, `--sql` |

Les traitements Bronze, Silver, Gold et étoile ont chacun leur destination par défaut ; l'explorateur lit l'étoile publiée. Consulter `--help` pour les valeurs des paramètres. Modifier une destination ou un périmètre de dataset doit rester une décision explicite : Silver/Gold reconstruisent leurs tables en remplacement.

Les versions de tables lues sont figées pendant chaque traitement. Ne pas lancer deux reconstructions simultanées d'une même couche. Pour l'étoile, utiliser les vues chargées par `scripts/explore.sh`, qui respectent le manifeste des versions publiées.

## Dépannage

| Symptôme | Vérification |
| --- | --- |
| Connexion Kafka refusée | Docker démarré, broker healthy, topic créé et port 9092 disponible |
| Premier lancement Spark lent | Téléchargements Python/Maven et disponibilité des fichiers locaux |
| `Coordinator load in progress: retrying` | Attendre la fin d'initialisation ; contrôler ensuite le bilan de livraison du producer |
| Consumer sans nouvelles lignes | Vérifier les offsets du groupe, puis la présence d'événements encore retenus |
| Silver place des lignes en quarantaine | Lire `quality_reasons` et vérifier le dataset ID et les données source |
| Checkpoint associé à une autre source/destination | Vérifier les chemins d'origine ; conserver table et checkpoint ensemble, sans contourner le marqueur |
| `EOFError` ou lecture bloquée dans les dépendances | Vérifier que les fichiers sont réellement locaux et que l'environnement est complet |

Les avertissements `jdk.incubator.vector`, `NativeCodeLoader` avec repli Java et `SparkStringUtils` sur la représentation du plan ne suffisent pas à indiquer un échec. Contrôler le résultat final et le code de sortie ; le message SparkStringUtils ne signifie pas que des lignes de données ont été tronquées.
