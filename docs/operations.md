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

Les dépendances Python sont dans `requirements.txt` et `requirements-streaming.txt`. Installer les dépendances complètes avec les commandes du [démarrage local](../README.fr.md#démarrage-local). Les scripts utilisent l'environnement `.venv` situé à la racine ; son activation dans le terminal est facultative.

## Ressources et stockage

Kafka est limité à 1 Gio de mémoire et 1,5 CPU par Compose, avec un heap Java de 256 à 512 Mio. Les outils Kafka exécutés dans le conteneur utilisent un heap maximal de 128 Mio. Spark s'exécute sur l'hôte avec deux threads et un heap driver de 768 Mio ; son utilisation mémoire totale dépasse le seul heap. Silver incrémental répartit les shuffles en huit partitions et utilise des caches sur disque ; cela ne change pas les deux threads ni le heap de 768 Mio. La limite Docker ne couvre pas Spark.

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

## Pipeline complet

```bash
bash scripts/pipeline.sh --max-messages 10000 --rate 100 --max-offsets 1000
```

Le lanceur utilise le CSV PaySim version 2 et son dataset ID de référence. `--csv` accepte un autre chemin vers ce même fichier vérifié. Les valeurs numériques doivent être positives et finies. Le préfixe doit exister intégralement dans le CSV. Un volume inférieur au nombre de faits de l'étoile déjà publiée est refusé : les couches locales conservent les données précédentes.

Les commandes sont exécutées successivement, avec arrêt au premier échec. Kafka est arrêté après Bronze et lors d'un échec ou d'une interruption contrôlée après son démarrage. Le lanceur arrête aussi un broker déjà actif : ne pas l'utiliser si un autre travail dépend du même Kafka. Un arrêt brutal de la machine ou du processus ne permet pas de garantir ce nettoyage ; vérifier alors `bash scripts/kafka.sh status`.

Un verrou empêche deux exécutions simultanées du lanceur sur macOS/Linux. Il ne bloque pas les commandes individuelles : ne pas lancer les étapes manuellement en parallèle. Aucune table ni aucun checkpoint ne sont supprimés. Ne pas changer le volume vers le bas pour tenter de réduire une table existante.

Chaque exécution conserve sous `data/runs/` un dossier avec un journal par commande et `report.json` : paramètres, état final, erreur éventuelle, durées et codes de sortie. Un code de sortie non nul signale l'échec. Consulter le bilan et le journal de l'étape concernée avant de relancer la même commande. Des événements peuvent déjà avoir été publiés lors d'un échec ; Silver déduplique le rejeu avec le même dataset ID. Les durées sont des temps écoulés, pas une mesure de mémoire ou un benchmark du dataset complet.

## Reprise CSV par lots

Depuis le préfixe existant de 250 000 lignes :

```bash
bash scripts/pipeline.sh --resume --max-messages 500000 --batch-rows 50000 --rate 1000 --max-offsets 5000
```

`--max-messages` reste une cible **totale** : cet exemple ajoute 250 000 lignes, en cinq lots de 50 000. `--batch-rows` vaut 50 000 par défaut et ne peut pas dépasser cette valeur. Chaque lot est également plafonné à 128 Mio estimés (JSON UTF-8, clé et marge de 1 Kio par message), même si toutes les clés aboutissent dans une seule partition. Ce plafond laisse une marge face à la rétention configurée de 256 Mio par partition ; ne pas réduire cette rétention pendant une reprise. Le contrôle est effectué avant tout envoi du lot.

Au premier lancement, le nombre de faits du manifeste étoile est une proposition de point de départ, pas une preuve : le lanceur exécute d'abord le rapprochement complet du préfixe CSV avec Silver/Gold/étoile. Un échec empêche la création du curseur et toute publication Kafka. Sur un environnement neuf, la progression commence à zéro. Le CSV doit avoir l'empreinte PaySim de référence.

`data/checkpoints/csv-replay.json` lie la progression au chemin CSV, au dataset et au dépôt. Il conserve la dernière ligne entièrement validée, le manifeste étoile correspondant et, avant publication, la plage en attente. Son remplacement est atomique après écriture et synchronisation du fichier. Les numéros `source_row` restent absolus : la ligne 250 001 conserve la même identité qu'un rejeu complet.

Les 250 000 transactions ajoutées ne sont pas lancées par la préparation ou les tests : il faut exécuter la commande ci-dessus.

Chaque lot démarre Kafka, draine d'abord les messages disponibles vers Bronze, publie uniquement sa plage, ingère Bronze, arrête Kafka, met à jour les couches incrémentales puis rapproche le **préfixe cumulé entier** avec le CSV. Le drainage permet de récupérer les envois d'une tentative interrompue avant de les republier. Le curseur n'avance qu'après succès de toutes ces étapes. Relancer exactement la même commande reprend le lot en attente, même si des écritures Delta avaient déjà réussi. Bronze peut contenir des rejeux ; Silver et les faits doivent rester uniques.

Une relance à cible déjà atteinte ne publie rien. Une cible inférieure à la progression ou au lot en attente est refusée. Une fois le curseur créé, utiliser `--resume` : le mode de rejeu historique sans curseur est bloqué. Ne pas lancer le producteur ni les traitements individuels en parallèle ; le verrou du lanceur ne bloque pas ces commandes directes. Un manifeste étoile modifié hors du lanceur entre deux lots validés provoque un refus pour éviter une désynchronisation silencieuse.

Les dossiers de lot et leurs `report.json` sont conservés sous `data/runs/<exécution>/<première>-<dernière>/`. La vérification initiale écrit `bootstrap-verify.log` à la racine du dossier d'exécution. Conserver le curseur, les tables, les journaux Delta/CDF nécessaires et le checkpoint Bronze ensemble. Ne pas supprimer ni éditer un état pour forcer une reprise.

La livraison est **au moins une fois**, avec déduplication métier ; elle n'est pas une transaction globale Kafka/Delta/curseur. Une interruption dépassant la rétention de 24 h peut rendre des offsets indisponibles : Bronze échoue alors avec `failOnDataLoss=true`. Le mécanisme refuse de masquer cette perte et ne reconstruit pas automatiquement un checkpoint expiré.

Le producteur sait aussi lire une plage via `--start-row` (1 par défaut) et `--max-messages` (**nombre** de lignes dans ce cas). Il ne tient pas de curseur autonome : utiliser le lanceur pour la progression validée. La lecture saute les lignes précédentes en parcourant le CSV, sans index de position. Les contrôles de chaque lot relisent tout le préfixe ; le coût augmente donc avec le volume. La vérification des comptes utilise Spark au lieu d'un ensemble Python de tous les comptes, mais le dataset complet reste à mesurer. Vérifier l'espace disque avant de l'étendre : CSV, Kafka, tables techniques, historique Delta, journaux et fichiers temporaires s'additionnent.

## Traitements et options

Tous les lanceurs Spark réutilisent `scripts/bronze.sh`. Les chemins ci-dessous sont relatifs à la racine du dépôt.

| Lanceur | Principales options |
| --- | --- |
| `scripts/bronze.sh` | `--bootstrap-servers`, `--topic`, `--output`, `--checkpoint`, `--max-offsets` |
| `scripts/silver.sh` | `--source`, `--output`, `--dataset-id`, `--mode incremental|batch` |
| `scripts/gold.sh` | `--source`, `--output`, `--mode incremental|batch` |
| `scripts/star.sh` | `--source`, `--output`, `--mode incremental|batch` |
| `scripts/explore.sh` | `--star`, `--sql` |

Les traitements Bronze, Silver, Gold et étoile ont chacun leur destination par défaut ; l'explorateur lit l'étoile publiée. Consulter `--help` pour les valeurs des paramètres. Modifier une destination ou un périmètre de dataset doit rester une décision explicite : Silver fusionne les nouvelles données par défaut ; Gold met à jour ses agrégats depuis le CDF Silver ; l’étoile fusionne les faits concernés et ajoute les nouveaux membres de dimensions.

Les versions de tables lues sont figées pendant chaque traitement. Ne pas lancer deux reconstructions simultanées d'une même couche. Pour l'étoile, utiliser les vues chargées par `scripts/explore.sh`, qui respectent le manifeste des versions publiées.

## Silver incrémental

```bash
bash scripts/silver.sh
```

Au premier lancement, le CDF est activé sur Bronze et tout l'historique est initialisé une fois. Les lancements suivants lisent seulement les nouveaux commits. Aucune nouvelle publication Kafka n'est nécessaire pour migrer les données existantes. Le lanceur `pipeline.sh` utilise ce mode automatiquement.

Conserver `data/silver/_incremental.json`, `data/silver/_events`, `transactions` et `quarantine` ensemble. Ne pas modifier le curseur, supprimer une seule de ces destinations ou utiliser un VACUUM qui effacerait des versions CDF encore nécessaires. Une erreur de source, de dataset, de rétention ou une modification/suppression Bronze doit être examinée ; ne pas la contourner en réinitialisant le marqueur. Ne pas changer les propriétés CDF pendant l'exécution.

Une sortie avec `pending_version` nécessite une reprise Silver avant Gold/étoile. Relancer la même commande ; le curseur reste sur la dernière version entièrement traitée. En cas d'arrivée de nouvelles données pendant cette reprise, relancer ensuite Silver une seconde fois pour les traiter.

Pour produire un batch complet de référence, utiliser une sortie distincte :

```bash
bash scripts/silver.sh --mode batch --output data/silver-reference
```

## Gold incrémental

```bash
bash scripts/gold.sh
```

Le premier lancement initialise les trois agrégats et active le CDF Silver sans changer les transactions. Les suivants prennent en compte les ajouts, corrections et retraits. La commande est déjà utilisée par `pipeline.sh`. Une relance sans changement affiche `status: no_change`.

Conserver `data/gold/_gold_incremental.json` avec `summary`, `by_type` et `by_step`. Ne pas modifier le marqueur, écrire dans ces tables en dehors du lanceur, ni supprimer les versions de base d'une reprise avec VACUUM. Si `pending` est présent, relancer la même commande : les arrivées Silver postérieures à cette plage seront traitées par l'exécution suivante. Une erreur de qualité, de source ou de rétention nécessite un diagnostic ; ne pas avancer le curseur manuellement. Une reconstruction contrôlée peut être produite dans une autre destination.

Pour des lectures cohérentes, utiliser `banking.gold_incremental.read_tables(spark, 'data/gold')`, qui refuse une publication en cours et épingle les trois versions publiées. Le schéma en étoile continue de lire Silver et utilise son propre manifeste.

```bash
bash scripts/gold.sh --mode batch --output data/gold-reference
```

## Étoile incrémentale

```bash
bash scripts/star.sh
```

Le premier lancement initialise l'état incrémental sur tout Silver et réconcilie les faits existants. Les dimensions déjà présentes sont conservées ; les versions suivantes du CDF ajoutent les membres observés et mettent à jour ou retirent les faits concernés. La commande reste intégrée à `pipeline.sh`. Une relance sans nouvelle version Silver ne réécrit aucune table ni le manifeste.

Conserver `data/gold/star/_star_incremental.json`, `_snapshot.json` et les quatre tables ensemble. Le curseur identifie le chemin et l'identité Delta de Silver. Une plage en cours est enregistrée avant les écritures ; elle est reprise à l'identique en cas d'interruption. Les nouvelles arrivées Silver attendent l'exécution suivante. Ne pas modifier le curseur, écrire directement dans les tables ou supprimer des versions encore nécessaires au CDF ou au manifeste avec VACUUM.

Le manifeste reste la référence de lecture : utiliser `scripts/explore.sh` ou `banking.star.register_views`. Tant qu'une nouvelle publication n'a pas réussi, l'ancien ensemble complet reste lisible. Une interruption après publication mais avant confirmation du curseur rejoue la même plage sans dupliquer les membres ni les faits. Les lectures directes des versions courantes pendant les MERGE peuvent voir un état intermédiaire ; elles ne sont pas prises en charge. Ne pas lancer deux traitements ou des modifications externes en parallèle.

Une suppression de fait ne supprime pas les comptes, types et heures historiques. Leurs effectifs peuvent dépasser ceux d'un recalcul complet sur les seuls faits courants. La validation continue de vérifier toutes les clés et références ; son coût reste proportionnel au volume existant. Le traitement conserve les limites Spark locales et utilise des caches sur disque.

Pour une reconstruction complète de comparaison, utiliser une destination distincte :

```bash
bash scripts/star.sh --mode batch --output data/star-reference
```

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
