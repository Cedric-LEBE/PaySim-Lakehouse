"""Run the repository's Spark SQL queries on a published star snapshot."""
import argparse
from pathlib import Path
from banking.bronze import session
from banking.star import register_views


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--star', default='data/gold/star')
    parser.add_argument('--sql', type=Path, help='Fichier contenant une requête SQL de lecture')
    args = parser.parse_args()
    paths = [args.sql] if args.sql else sorted(Path('sql/star').glob('*.sql'))
    if not paths:
        parser.error('Aucune requête SQL trouvée')
    spark = session()
    spark.sparkContext.setLogLevel('WARN')
    try:
        manifest = register_views(spark, args.star)
        print(f"Instantané Silver version {manifest['silver_version']}", flush=True)
        for path in paths:
            query = path.read_text().strip().rstrip(';').strip()
            if not query.upper().startswith('SELECT '):
                raise ValueError('Une requête SELECT est attendue')
            print(f'\n--- {path.name} ---', flush=True)
            spark.sql(query).show(50, truncate=False)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
