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


class DashboardPresentationTests(unittest.TestCase):
    def test_layout_has_no_overlaps_and_wide_tables_are_full_width(self):
        definition = builder.build()
        for page in definition['pages']:
            boxes = [item['position'] for item in page['layout']]
            for i, a in enumerate(boxes):
                self.assertTrue(0 <= a['x'] and a['x'] + a['width'] <= 12)
                for b in boxes[i+1:]:
                    overlap = (a['x'] < b['x'] + b['width'] and b['x'] < a['x'] + a['width']
                               and a['y'] < b['y'] + b['height'] and b['y'] < a['y'] + a['height'])
                    self.assertFalse(overlap, page['name'])
            for item in page['layout']:
                spec = item['widget'].get('spec', {})
                if spec.get('widgetType') == 'table' and len(spec['encodings']['columns']) >= 5:
                    self.assertEqual(item['position']['width'], 12)

    def test_ratio_axes_use_percent_without_rescaling_data(self):
        charts = [item['widget'] for p in builder.build()['pages'] for item in p['layout']
                  if item['widget'].get('spec', {}).get('widgetType') in ('bar','line')]
        ratios = [w for w in charts if w['spec']['encodings']['y']['fieldName'] in builder.PERCENT]
        self.assertEqual(len(ratios), 2)
        for widget in ratios:
            enc = widget['spec']['encodings']['y']
            self.assertEqual(enc['format']['type'], 'number-percent')
            field = next(f for f in widget['queries'][0]['query']['fields'] if f['name'] == enc['fieldName'])
            self.assertEqual(field['expression'], '`' + field['name'] + '`')
            self.assertIn('%', widget['spec']['format']['percentFormat']['format'])
            self.assertNotIn('%', widget['spec']['format']['numberFormat']['format'])
            if widget['spec']['encodings']['x']['fieldName']=='account_rank':
                self.assertEqual(widget['spec']['encodings']['x']['format']['type'],'number-plain')

    def test_theme_and_exact_counts_have_readable_contracts(self):
        definition = builder.build()
        self.assertEqual(definition['uiSettings']['locale'], 'en-US')
        theme = definition['uiSettings']['theme']
        self.assertNotEqual(theme['canvasBackgroundColor'], theme['widgetBackgroundColor'])
        counters = [item['widget']['spec'] for p in definition['pages'] for item in p['layout']
                    if item['widget'].get('spec', {}).get('widgetType') == 'counter']
        for counter in counters:
            value = counter['encodings']['value']
            self.assertNotIn('color', value)
            if value['fieldName'] in ('transactions','fraud_transactions'):
                self.assertEqual(value['format']['type'], 'number-plain')
                self.assertEqual(value['format']['abbreviation'], 'none')
                self.assertEqual(value['formatTemplate'], '{{ ' + value['fieldName'] + '_display }}')
        text = json.dumps(definition)
        self.assertNotIn('250,000', text)
        self.assertNotIn('14 simulation hours', text)

    def test_short_references_are_presentation_only_and_balances_remain_available(self):
        definition = builder.build()
        dataset = next(d for d in definition['datasets'] if d['name']=='16_transactions')
        self.assertIn('transaction_key', ''.join(dataset['queryLines']))
        page = next(p for p in definition['pages'] if p['name']=='accounts')
        balances = next(i['widget'] for i in page['layout'] if i['widget']['name']=='accounts_balances')
        fields = {f['name'] for f in balances['queries'][0]['query']['fields']}
        self.assertTrue({'origin_balance_before','origin_balance_after','destination_balance_before','destination_balance_after'} <= fields)
        ref = next(c for c in balances['spec']['encodings']['columns'] if c['fieldName']=='transaction_reference')
        self.assertIn('12 chars', ref['displayName'])

class DatasetSeriesTests(unittest.TestCase):
    def test_single_series_colors_use_materialized_dataset_columns(self):
        definition=builder.build()
        datasets={d['name']:d for d in definition['datasets']}
        checked=0
        for page in definition['pages']:
            for item in page['layout']:
                widget=item['widget']
                color=widget.get('spec',{}).get('encodings',{}).get('color',{})
                field=color.get('fieldName','')
                if field not in ('_banking_series','_fraud_series'):continue
                query=widget['queries'][0]['query']
                sql=''.join(datasets[query['datasetName']]['queryLines'])
                self.assertIn(' AS '+field,sql)
                projection=next(f for f in query['fields'] if f['name']==field)
                self.assertEqual(projection['expression'],'`'+field+'`')
                self.assertTrue(color['scale']['mappings'][0]['value'])
                checked+=1
        self.assertGreaterEqual(checked,5)

class DashboardFinishingTests(unittest.TestCase):
    def test_table_amounts_keep_numeric_projection_and_plain_grouped_format(self):
        for page in builder.build()['pages']:
            for item in page['layout']:
                widget = item['widget']
                if widget.get('spec', {}).get('widgetType') != 'table':
                    continue
                fields = {f['name']: f['expression'] for f in widget['queries'][0]['query']['fields']}
                for column in widget['spec']['encodings']['columns']:
                    field = column['fieldName']
                    if ('amount' in field or 'balance' in field) and field not in builder.PERCENT:
                        self.assertEqual(column['format']['type'], 'number-plain')
                        self.assertFalse(column['format']['hideGroupSeparator'])
                        self.assertEqual(fields[field], '`' + field + '`')
        pages = {p['name']: p for p in builder.build()['pages']}
        for page_name, title, minimum in [('overview','Summary by Type',5), ('fraud','Fraud by Type and Relative Rate',5), ('fraud','Fraud by Amount Band',5), ('fraud','Observed Balance Changes per Transaction',6)]:
            item = next(i for i in pages[page_name]['layout'] if i['widget'].get('spec',{}).get('frame',{}).get('title') == title)
            self.assertGreaterEqual(item['position']['height'], minimum)


    def test_band_order_and_flag_detail_remain_visible(self):
        definition = builder.build()
        pages = {p['name']: p for p in definition['pages']}
        distribution = next(w['widget'] for w in pages['overview']['layout'] if w['widget'].get('spec', {}).get('encodings', {}).get('x', {}).get('fieldName') == 'amount_band')
        order = distribution['spec']['encodings']['x']['scale']['sort']['orderedValues']
        self.assertEqual(order, ['0', 'Over 0 to <1,000', '1,000 to <10,000', '10,000 to <100,000', '100,000 to <1,000,000', '1,000,000+'])
        cards = [w['widget'] for w in pages['fraud']['layout'] if w['widget'].get('spec', {}).get('widgetType') == 'counter']
        self.assertEqual({w['spec']['encodings']['value']['fieldName'] for w in cards}, {'fn', 'tp', 'recall_flag'})
        self.assertTrue(all(w['queries'][0]['query']['datasetName'] == '10_flag_metrics' for w in cards))
        summary = next(d for d in definition['datasets'] if d['name'] == '01_summary')
        self.assertIn('format_number(transactions, 0) AS transactions_display', ''.join(summary['queryLines']))

if __name__=='__main__': unittest.main()
