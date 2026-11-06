# Données PaySim

[Retour au README](../README.md)

## Source et périmètre

Le projet utilise [Synthetic Financial Datasets for Fraud Detection — PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1), publié par Edgar Alonso Lopez-Rojas sur Kaggle. Il s'agit de transactions simulées de mobile money. La démonstration locale rejoue les 100 premières lignes ; le fichier complet n'est pas nécessairement traité.

Référence : E. A. Lopez-Rojas, A. Elmir et S. Axelsson, *PaySim: A financial mobile money simulator for fraud detection*, EMSS, 2016. Consulter la page Kaggle pour la licence et les conditions d'utilisation du dataset.

Télécharger l'archive, la décompresser et conserver le nom du CSV :

```text
data/raw/PS_20174392719_1491204439457_log.csv
```

Le CSV, les checkpoints, les tables Delta et les caches Spark sont ignorés par Git. Seule la fixture synthétique du dossier tests est incluse dans le dépôt.

## Vérifier le fichier

La version de référence est la version 2 du dataset Kaggle :

| Propriété | Valeur |
| --- | --- |
| Taille du CSV | 493 534 783 octets |
| SHA-256 | `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b` |
| Dataset ID utilisé par le pipeline | `paysim-sha256-16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b` |

Depuis la racine du dépôt :

```bash
.venv/bin/python - <<'PY'
import hashlib
from pathlib import Path

path = Path('data/raw/PS_20174392719_1491204439457_log.csv')
expected = '16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b'
with path.open('rb') as stream:
    actual = hashlib.file_digest(stream, 'sha256').hexdigest()
if path.stat().st_size != 493534783 or actual != expected:
    raise SystemExit('Le fichier diffère de la version de référence.')
print('PASS: taille et empreinte PaySim vérifiées.')
PY
```

Le producer utilise le dataset ID fourni en argument ; il ne calcule pas l'empreinte du fichier. Une autre version ou un CSV modifié doit recevoir son propre identifiant stable, puis être sélectionné explicitement avec `scripts/silver.sh --dataset-id IDENTIFIANT`. Ne pas réutiliser l'identifiant de référence pour un autre contenu.

## Colonnes attendues

Le producer exige les onze colonnes, sans colonne supplémentaire :

| Colonnes | Usage |
| --- | --- |
| `step` | Heure de simulation, sans date calendrier d'origine |
| `type` | `PAYMENT`, `TRANSFER`, `CASH_OUT`, `CASH_IN` ou `DEBIT` |
| `amount` | Montant de la transaction |
| `nameOrig`, `nameDest` | Identifiants simulés des comptes émetteur et destinataire |
| `oldbalanceOrg`, `newbalanceOrig` | Soldes du compte émetteur avant/après |
| `oldbalanceDest`, `newbalanceDest` | Soldes du compte destinataire avant/après |
| `isFraud` | Label de fraude du simulateur, utilisé pour le reporting et l'évaluation |
| `isFlaggedFraud` | Flag fourni par PaySim, comptabilisé séparément du label |

Les montants sont transmis dans le JSON sous forme de chaînes décimales. Silver exige des valeurs positives ou nulles, sans signe, représentables exactement en Decimal(20,2), sans arrondi. La notation scientifique est acceptée lorsque sa valeur respecte ces contraintes (par exemple `1.0E+7` vaut `10000000.00`) ; les valeurs inférieures au centime comme `1E-3` sont refusées. Les labels doivent valoir 0 ou 1. Les contrôles détaillés sont décrits dans [l'architecture](architecture.md#silver--qualité-et-déduplication).

Les labels ne doivent pas être utilisés comme variables prédictives d'un futur modèle de fraude. Les soldes sont des observations à la transaction ; leur somme sur plusieurs transactions n'est pas un solde consolidé. Aucun enrichissement en dates réelles, en devise ou en données démographiques n'est réalisé.

## Fixture synthétique

`tests/fixtures/paysim_synthetic.csv` contient deux lignes créées pour les tests. Elles ne sont pas extraites du dataset Kaggle.

Vérifier le producer sans Kafka et sans publier d'événements :

```bash
PYTHONPATH=src .venv/bin/python -m banking.producer \
  --csv tests/fixtures/paysim_synthetic.csv \
  --dataset-id synthetic-v1 \
  --dry-run
```

Le mode dry-run affiche les événements et annonce zéro livraison Kafka. Si cette fixture est publiée sur le topic principal, ses événements sont conservés en Bronze puis exclus du dataset de référence en Silver avec le motif `excluded_dataset`.
