"""Portable manifest checks and S3-to-Unity-Catalog volume path resolution.

No AWS credentials or network clients: file access remains governed by the volume.
"""
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

PRIMARY_KEYS = {
    'dim_transaction_type': 'transaction_type_key',
    'dim_simulation_time': 'simulation_time_key',
    'dim_account': 'account_key',
    'fact_transaction': 'transaction_key',
}


def quote(identifier):
    return '`' + identifier.replace('`', '``') + '`'


def path_parts(value):
    # Reject ambiguous spellings instead of normalizing traversal or URI escapes.
    parts = value.strip('/').split('/')
    if any(part in ('', '.', '..') or not re.fullmatch(r'[A-Za-z0-9_.-]+', part) for part in parts):
        raise ValueError('Chemin invalide : segments simples sans traversée ni encodage requis')
    return parts


def s3_parts(value):
    parsed = urlsplit(value)
    if (parsed.scheme != 's3' or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', parsed.netloc)
            or parsed.query or parsed.fragment or not parsed.path.startswith('/')):
        raise ValueError('URL attendue : s3://bucket/chemin, sans paramètres ni identifiants')
    return parsed.netloc, path_parts(parsed.path)


def resolve_export_path(value, s3_volume, describe_volume):
    """Resolve an S3 export only through an external volume with a matching root.

    describe_volume accepts a quoted catalog.schema.volume and returns DESCRIBE
    VOLUME metadata. It is not called for legacy /Volumes imports.
    """
    if value.startswith('/Volumes/'):
        parts = path_parts(value)
        if len(parts) < 4 or parts[0] != 'Volumes':
            raise ValueError('Chemin attendu : /Volumes/catalog/schema/volume/export')
        return Path('/') / Path(*parts)
    bucket, key = s3_parts(value)
    names = s3_volume.split('.')
    if len(names) != 3 or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', name) for name in names):
        raise ValueError('Pour S3, renseigner s3_volume sous la forme catalog.schema.volume')
    metadata = describe_volume('.'.join(quote(name) for name in names))
    # Runtime releases expose DESCRIBE fields in camelCase or snake_case.
    metadata = {key.replace('_', '').lower(): value for key, value in metadata.items()}
    if metadata.get('volumetype') != 'EXTERNAL':
        raise ValueError('s3_volume doit désigner un volume EXTERNAL')
    root_bucket, root_key = s3_parts(metadata['storagelocation'])
    if bucket != root_bucket or key[:len(root_key)] != root_key:
        raise ValueError('Export S3 hors du chemin du volume externe sélectionné')
    return Path('/Volumes').joinpath(*names, *key[len(root_key):])


def load_manifest(directory):
    with (directory / 'manifest.json').open('rb') as stream:
        content = stream.read(1024 * 1024 + 1)
    if len(content) > 1024 * 1024:
        raise ValueError('Manifeste supérieur à 1 Mio')
    manifest = json.loads(content)
    if type(manifest.get('format_version')) is not int or manifest['format_version'] != 1:
        raise ValueError('Version de manifeste non prise en charge')
    if not isinstance(manifest.get('export_id'), str) or not re.fullmatch(r'[A-Za-z0-9_-]+', manifest['export_id']):
        raise ValueError('export_id invalide')
    if not isinstance(manifest.get('tables'), dict) or set(manifest['tables']) != set(PRIMARY_KEYS):
        raise ValueError('Les quatre tables de l’étoile sont requises')
    for name, info in manifest['tables'].items():
        if not isinstance(info, dict) or info.get('file') != name + '.parquet':
            raise ValueError('Nom de fichier invalide : ' + name)
        if (type(info.get('rows')) is not int or info['rows'] < 0 or
                type(info.get('bytes')) is not int or info['bytes'] < 1 or
                not isinstance(info.get('sha256'), str) or not re.fullmatch(r'[a-f0-9]{64}', info['sha256'])):
            raise ValueError('Métadonnées de fichier invalides : ' + name)
    return manifest


def verify_file(path, info):
    """Hash in bounded chunks; never collect a Parquet binary into the driver."""
    digest = hashlib.sha256()
    size = 0
    with path.open('rb') as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    if size != info['bytes'] or digest.hexdigest() != info['sha256']:
        raise ValueError('Taille ou empreinte SHA-256 incorrecte : ' + path.name)
