# Dashboard analytique versionné avec DAB

[Retour au README](../README.md)

Les textes visibles, catégories et filtres du dashboard sont en anglais ; les échanges et ce guide restent en français. Les valeurs de label sont `Fraudulent` et `Non-fraudulent`.

Le bundle `paysim-lakehouse` déploie un nouveau dashboard AI/BI et un job d'import de l'étoile, sans remplacer le dashboard créé manuellement. Les données déjà présentes dans le volume et les tables ne sont pas transportées par DAB. Aucun warehouse n'est créé : fournir l'identifiant d'un warehouse existant.

## Contenu

- [databricks.yml](../databricks.yml) : ressources, variables et cible de développement.
- [Dashboard généré](../databricks/dashboards/paysim.lvdash.json) : quatre pages analytiques, plus une page de filtres communs.
- [Générateur](../scripts/build_dashboard.py) : construction reproductible du JSON, sans dépendance supplémentaire.
- [Analyses SQL](../sql/analytics/) : seize requêtes et un filtre commun, avec paramètres liés avant agrégation.
- [Notebook d'import](../databricks/notebooks/01_import_star.py) : empreintes, clés, références et totaux contrôlés ; reprise des tables identiques existantes sans écrasement.

Pour une source S3, fournir `export_path=s3://.../<export_id>/` et `s3_volume=catalog.schema.volume` ; le notebook vérifie le volume externe avant les empreintes. Voir [le guide S3](s3-import.md). Le module `_import_support.py` est synchronisé avec le notebook par DAB.

## Pages et règles de lecture

| Page | Analyses |
| --- | --- |
| Overview | Volumes, montants, moyenne, médiane approximative, fraude, part du montant frauduleux, répartition par type et tranche de montant |
| Activity Over Time | Volumes et montants horaires, séries par type, fraudes horaires et variation entre heures observées |
| Fraud Analysis | Taux et taux relatif par type, comparaison des distributions, médiane/p90/p99 approximatifs, tranches, matrice du flag et soldes observés |
| Accounts and Transactions | Top 20 émetteurs et destinataires, contreparties distinctes, concentration cumulée, rôles des comptes impliqués et détail limité à 500 transactions |

Les filtres communs portent sur les types, les labels de fraude, les bornes d'heure et de montant. Sélection vide pour les catégories = tout le périmètre. Les bornes sont des entrées textuelles converties par `try_cast` (utiliser un point comme séparateur décimal) ; une borne invalide ou inversée produit un périmètre vide. Les taux et médianes sont recalculés après filtrage. Les requêtes de classement filtrent d'abord le périmètre, calculent leurs dénominateurs sur tout ce périmètre, puis limitent l'affichage. Le cumul de concentration n'est donc pas renormalisé sur les seuls vingt comptes visibles.

Les ratios sont stockés entre 0 et 1 : les compteurs et tables utilisent un format pourcentage. Les axes de ratio non formatés sont étiquetés « ratio 0–1 ». Une division sans dénominateur donne NULL, pas un taux inventé. Le flag fourni par PaySim n'est pas un modèle entraîné ; une forte exactitude peut masquer un rappel nul. Les participations par rôle ne sont pas des fraudes uniques : une même transaction peut compter aux deux extrémités. Ne pas additionner les soldes de transactions comme des soldes de comptes.

Le premier échantillon de référence couvrait 250 000 lignes et les heures 1 à 14. Le nouvel instantané complet couvre 6 362 620 lignes et les heures 1 à 743 ; les heures restent simulées et aucune prédiction temporelle n'est affichée. Aucune devise ni date réelle n'est attribuée. Une page de qualité n'est pas simulée : elle nécessitera l'import des rejets, conflits et journaux du pipeline.

## Validation et déploiement

La CLI installée et validée pour ce bundle est 1.24.0 ou supérieure. Réutiliser un profil authentifié existant ; ne pas ajouter de token au dépôt. Avec un profil nommé `dev` :

```bash
.venv/bin/python scripts/build_dashboard.py
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
databricks bundle validate --profile dev --strict --var warehouse_id=<ID_EXISTANT>
databricks bundle deploy --profile dev --var warehouse_id=<ID_EXISTANT>
```

Remplacer `<ID_EXISTANT>` par l'identifiant réel, sans chevrons. Le nom du profil peut être adapté. Les variables `catalog`, `schema`, `export_path`, `s3_volume` et `warehouse_id` permettent de sélectionner l'environnement. Ne pas utiliser la variable `schema` pour prétendre qu'un nouveau dataset a été chargé : le volume, le manifeste et les tables doivent correspondre. Les requêtes utilisent `transactions_dashboard` sans qualification ; DAB injecte le catalogue et le schéma par défaut du dashboard.

Le job n'a ni planification ni déclenchement automatique. Pour importer ou vérifier à nouveau un export déjà présent dans le volume :

```bash
databricks bundle run import_star --profile dev --var warehouse_id=<ID_EXISTANT>
```

Le notebook reçoit ses paramètres du bundle, compare les tables existantes à l'export et ne les écrase pas. Le déploiement seul ne lance pas ce job. Le dashboard reste un brouillon ; la publication et les permissions de partage ne sont pas modifiées. Le [workflow manuel GitHub](deployment.md) est préparé avec OIDC et sans secret persistant, mais reste non activé. OIDC nécessite une configuration de compte indisponible dans Free Edition ; une option PAT explicite nécessite un secret GitHub protégé configuré par le propriétaire.

## Validation réalisée

Le bundle a passé la validation stricte et a été déployé dans la cible `dev`. La définition relue via l'API correspond au JSON du dépôt. Le job d'import a terminé avec le statut `SUCCESS` sur les tables existantes. Une requête exécutée sur le warehouse Databricks a confirmé 250 000 transactions, 44 796 585 140,23 de montant total, 163 fraudes et un taux de 0,0652 %. L'éditeur doit encore servir à vérifier le rendu et les interactions : le contrôle automatisé du navigateur Databricks n'est pas pris en charge.

## Contrôles

```bash
BANKING_SPARK_APP=tests/integration/dashboard_analytics_check.py bash scripts/bronze.sh
```

Le contrôle local lit l'étoile publiée de 250 000 faits. Il exécute les seize analyses avec paramètres liés, réconcilie les montants et volumes, vérifie les filtres combinés et le cas vide. Les quarante et un tests unitaires incluent la synchronisation du JSON généré, les références des widgets et les liaisons des paramètres. La validation stricte DAB et l'acceptation de la ressource par l'API complètent ces contrôles ; l'affichage et l'interaction finale des filtres doivent être vérifiés dans l'éditeur Databricks.

Modifier les SQL ou le générateur, régénérer le JSON puis déployer à nouveau. Une modification manuelle du dashboard géré par DAB peut être remplacée au déploiement suivant ; reporter les changements souhaités dans le dépôt. Ne pas lier ce bundle au dashboard manuel sans décision explicite.

Références : [ressources DAB](https://docs.databricks.com/aws/en/dev-tools/bundles/resources), [exemple de dashboard paramétré](https://docs.databricks.com/aws/en/dev-tools/bundles/examples), [commandes bundle](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands).

## Refonte du dashboard sur le dataset complet

Le nouvel instantané `workspace.paysim_20261109t205841z_8ed6f88b` contient **6 362 620 transactions**, dont **8 213 fraudes**, sur les heures simulées 1 à 743. Les KPI ont été rapprochés du manifeste S3 : montant total `1144392944759.77`, montant frauduleux `12056415427.84`. Le taux de fraude est environ 0,129082 %, affiché à 0,1291 % selon la précision choisie.

La refonte conserve les 17 datasets (16 analyses et choix de filtres), les calculs métier et leurs paramètres. Quatre datasets ajoutent deux colonnes de présentation constantes pour leurs légendes, sans modifier les agrégats ; leur tri final est conservé explicitement. Elle applique un thème natif en modes clair et sombre : fond bleu-gris, cartes bleu très clair, texte bleu marine, couleurs stables par type et corail pour les séries de fraude. Le fond des cartes provient du thème global : aucune propriété non documentée de couleur individuelle de compteur n'est injectée.

La locale du dashboard est fixée à `en-US`. Les compteurs de transactions et fraudes affichent des entiers complets ; les montants de synthèse sont compacts, sans symbole monétaire. Les ratios gardent leurs valeurs 0–1 et reçoivent un format pourcentage ; les rangs et heures restent numériques. Les textes ne figent plus l'ancien échantillon de 250 000 lignes / 14 heures, et ne présentent pas les heures simulées comme des dates calendaires ou des preuves de saisonnalité réelle.

Les introductions passent de trois à deux unités de grille. Les quatre KPI principaux sont Transactions, Total Amount, Fraudulent Transactions et Fraud Rate. Les grandes tables utilisent toute la largeur disponible. Les soldes des 500 transactions sont dans une table séparée ; la référence affichée est un préfixe de 12 caractères, explicitement étiqueté. Elle sert à la lecture et n'est **pas une clé unique** ; l'identifiant complet reste dans le dataset SQL et la vue source. Les tables restent défilables sur les écrans étroits.

Les tests de présentation couvrent les chevauchements, les largeurs, les liaisons de champs, le formatage des axes sans multiplication des ratios, les entiers exacts, la suppression des anciennes références et la disponibilité des soldes. Ils complètent les contrôles SQL et le rapprochement des KPI avec le manifeste complet. L'API accepte la définition du brouillon ; l'apparence finale, notamment le rendu des formats selon le client, doit encore être inspectée visuellement après déploiement.

Pour déployer sans revenir aux anciens paramètres par défaut, conserver explicitement le schéma, l'URL S3 et le volume de l'instantané complet. Le déploiement du dashboard ne nécessite ni de relancer l'import ni de démarrer EC2. La publication doit conserver les réglages d'identifiants et les droits de partage existants.


### Correctifs après contrôle visuel

Les captures ont révélé des légendes vides pour les séries constantes, ainsi que des compteurs abrégés malgré un format sans abréviation implicite. Les datasets 03, 06, 08 et 13 produisent désormais `_banking_series` et `_fraud_series` dans SQL ; les widgets sélectionnent leurs valeurs réelles, ce qui permet le mapping bleu/corail. Les compteurs de transactions et de fraudes fixent `abbreviation: none` et utilisent un champ de présentation `format_number(..., 0)` dans le template natif, afin de conserver les séparateurs anglais. Les montants restent volontairement compacts. Les axes de taux et la disposition déjà validés visuellement sont conservés.

### Finitions de lisibilité

Les tranches de montants suivent un ordre catégoriel explicite, du zéro à un million et plus. Les formats numériques désactivent le masquage des séparateurs de milliers ; les deux compteurs principaux utilisent des champs de présentation avec virgules, sans modifier leurs valeurs numériques sources. Les petits tableaux de synthèse ont une hauteur réduite. Trois compteurs complètent la matrice du flag PaySim : faux négatifs, vrais positifs et rappel ; ils utilisent le même dataset filtré et ne représentent pas un modèle prédictif.
