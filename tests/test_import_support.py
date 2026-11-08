"""Source confinement and streaming integrity checks; no Databricks or Spark."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

spec = importlib.util.spec_from_file_location('_import_support', 'databricks/notebooks/_import_support.py')
support = importlib.util.module_from_spec(spec)
spec.loader.exec_module(support)


class ImportSupportTests(unittest.TestCase):
    def setUp(self):
        self.describe = Mock(return_value={
            'volumeType': 'EXTERNAL', 'storageLocation': 's3://paysim-test/exports/'})

    def test_s3_resolves_to_matching_external_volume(self):
        path = support.resolve_export_path('s3://paysim-test/exports/export-A/', 'workspace.uploads.s3_exports', self.describe)
        self.assertEqual(path, Path('/Volumes/workspace/uploads/s3_exports/export-A'))
        self.describe.assert_called_once_with('`workspace`.`uploads`.`s3_exports`')
        self.describe.return_value = {'volume_type': 'EXTERNAL', 'storage_location': 's3://paysim-test/exports'}
        self.assertEqual(support.resolve_export_path('s3://paysim-test/exports/export-A', 'workspace.uploads.s3_exports', self.describe), path)

    def test_existing_volume_paths_do_not_require_s3_metadata(self):
        path = '/Volumes/workspace/uploads/exports/export-A/'
        self.assertEqual(support.resolve_export_path(path, '', self.describe), Path(path))
        self.describe.assert_not_called()

    def test_s3_cannot_escape_the_registered_bucket_or_prefix(self):
        for source in ['s3://other-bucket/exports/A', 's3://paysim-test/exports-other/A', 's3://paysim-test/raw/A']:
            with self.subTest(source=source), self.assertRaises(ValueError):
                support.resolve_export_path(source, 'workspace.uploads.s3_exports', self.describe)

    def test_ambiguous_paths_and_missing_volume_are_rejected(self):
        sources = ['s3://paysim-test/exports/../raw/A', 's3://paysim-test/exports/%2e%2e/A',
                   's3://paysim-test/exports/A?token=x', '/Volumes/workspace/uploads/exports/../A',
                   's3://user@paysim-test/exports/A', '/tmp/export-A', 's3://paysim-test/exports//A']
        for source in sources:
            with self.subTest(source=source), self.assertRaises(ValueError):
                support.resolve_export_path(source, 'workspace.uploads.s3_exports', self.describe)
        for volume in ['', 'only.two', 'workspace.uploads.volume;DROP']:
            with self.assertRaises(ValueError):
                support.resolve_export_path('s3://paysim-test/exports/A', volume, self.describe)

    def test_managed_volume_cannot_impersonate_s3(self):
        self.describe.return_value['volumeType'] = 'MANAGED'
        with self.assertRaises(ValueError):
            support.resolve_export_path('s3://paysim-test/exports/A', 'workspace.uploads.s3_exports', self.describe)

    def test_hash_and_size_detect_corruption(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'test.parquet'
            content = b'x' * (2 * 1024 * 1024 + 7)
            path.write_bytes(content)
            info = {'bytes': len(content), 'sha256': hashlib.sha256(content).hexdigest()}
            support.verify_file(path, info)
            path.write_bytes(content[:-1] + b'y')  # Same size, wrong hash.
            with self.assertRaises(ValueError):
                support.verify_file(path, info)
            path.write_bytes(content)
            with self.assertRaises(ValueError):
                support.verify_file(path, {**info, 'bytes': len(content) + 1})

    def test_manifest_rejects_traversal_missing_tables_and_bad_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = {'format_version': 1, 'export_id': 'test-A', 'tables': {
                name: {'file': name + '.parquet', 'rows': 2, 'bytes': 100, 'sha256': 'a' * 64}
                for name in support.PRIMARY_KEYS}}
            def check(value):
                (root / 'manifest.json').write_text(json.dumps(value))
                return support.load_manifest(root)
            self.assertEqual(check(manifest), manifest)
            for override in [{'format_version': True}, {'export_id': '../A'}, {'tables': {}}]:
                with self.assertRaises(ValueError):
                    check({**manifest, **override})
            for override in [{'file': '../fact_transaction.parquet'}, {'rows': True}, {'bytes': 0}, {'sha256': 'invalid'}]:
                tables = {**manifest['tables'], 'fact_transaction': {**manifest['tables']['fact_transaction'], **override}}
                with self.assertRaises(ValueError):
                    check({**manifest, 'tables': tables})

    def test_oversized_manifest_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'manifest.json').write_bytes(b' ' * (1024 * 1024 + 1))
            with self.assertRaisesRegex(ValueError, '1 Mio'):
                support.load_manifest(root)
