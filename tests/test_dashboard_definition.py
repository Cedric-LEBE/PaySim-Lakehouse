"""Keep generated dashboard SQL, filters and visualization bindings consistent."""
import importlib.util
import json
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('dashboard_builder',ROOT/'scripts/build_dashboard.py')
builder=importlib.util.module_from_spec(spec)
spec.loader.exec_module(builder)

class DashboardDefinitionTests(unittest.TestCase):
    def test_generated_definition_is_current_and_bindings_resolve(self):
        definition=json.loads((ROOT/'databricks/dashboards/paysim.lvdash.json').read_text())
        self.assertEqual(definition,builder.build())
        self.assertEqual(len(definition['datasets']),17)
        self.assertEqual(sum(p['pageType']=='PAGE_TYPE_CANVAS' for p in definition['pages']),4)
        datasets={d['name']:d for d in definition['datasets']}
        for dataset in datasets.values():
            self.assertTrue(dataset.get('displayName'))
            for parameter in dataset.get('parameters',[]):
                self.assertTrue(parameter.get('displayName'))
        names=[]
        for page in definition['pages']:
            for item in page['layout']:
                widget=item['widget']; names.append(widget['name'])
                queries={q['name']:q['query'] for q in widget.get('queries',[])}
                for query in queries.values():
                    self.assertIn(query['datasetName'],datasets)
                if 'spec' not in widget: continue
                spec=widget['spec']
                self.assertEqual(spec['version'],3 if spec['widgetType'] in ('bar','line') else 2)
                if spec['widgetType'].startswith('filter-'):
                    for f in spec['encodings']['fields']:
                        self.assertIn(f['queryName'],queries)
                        if 'parameterName' in f:
                            ds=datasets[queries[f['queryName']]['datasetName']]
                            self.assertIn(f['parameterName'],[p['keyword'] for p in ds['parameters']])
                else:
                    fields={f['name'] for f in queries['main_query']['fields']}
                    for encoding in spec['encodings'].values():
                        for field in encoding if isinstance(encoding,list) else [encoding]:
                            if 'fieldName' in field: self.assertIn(field['fieldName'],fields)
        self.assertEqual(len(names),len(set(names)))

if __name__=='__main__': unittest.main()
