"""Generate the versioned AI/BI dashboard from reviewed SQL analyses (stdlib only)."""
import copy
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / 'sql/analytics'
LABELS = {'transactions':'Transactions','transaction_type':'Type','total_amount':'Total Amount','average_amount':'Average Amount','median_amount':'Approximate Median','fraud_transactions':'Fraudulent Transactions','fraud_rate':'Fraud Rate','fraud_amount':'Fraud Amount','relative_rate':'Rate Relative to Filtered Population','simulation_step':'Simulation Hour','transaction_change':'Count Change','transaction_growth':'Relative Change','origin_account':'Origin Account','destination_account':'Destination Account','distinct_counterparties':'Distinct Counterparties','account_rank':'Rank','cumulative_share':'Cumulative Share','fraud_label':'PaySim Label','amount_band':'Amount Band','precision_flag':'Flag Precision','recall_flag':'Flag Recall','accuracy_flag':'Flag Accuracy','f1_flag':'Flag F1 Score','outcome':'Outcome','account_role':'Role','account':'Account','fraud_participations':'Fraud-related Role Count','amount':'Amount','is_fraud':'Fraud Label','is_flagged_fraud':'PaySim Flag','transaction_key':'Transaction ID','p90_amount':'90th Percentile Amount','p99_amount':'99th Percentile Amount','maximum_amount':'Maximum Amount','average_origin_change':'Average Origin Balance Change','average_destination_change':'Average Destination Balance Change','origin_zero_after':'Origin Balance Zero After Transaction','origin_balance_before':'Origin Balance Before','origin_balance_after':'Origin Balance After','destination_balance_before':'Destination Balance Before','destination_balance_after':'Destination Balance After','tp':'True Positives','fp':'False Positives','fn':'False Negatives','tn':'True Negatives'}
PERCENT = {'fraud_rate','transaction_growth','cumulative_share','amount_share','precision_flag','recall_flag','accuracy_flag','f1_flag'}
PARAMS = {'types': None, 'fraud_labels': None, 'hour_min': '1', 'hour_max': '744', 'amount_min': '0', 'amount_max': '999999999999999999.99'}


# Semantic chart colors are stable across pages; they do not classify a type as fraud.
TYPE_COLORS = {'CASH_IN': '#238B8D', 'CASH_OUT': '#3274C5', 'DEBIT': '#9770B5',
               'PAYMENT': '#C28B24', 'TRANSFER': '#5364A5'}
NAVY, CORAL = '#17385D', '#C65345'
COUNT_FIELDS = {'transactions', 'fraud_transactions', 'distinct_counterparties',
                'fraud_participations', 'simulation_step', 'account_rank',
                'is_fraud', 'is_flagged_fraud', 'tp', 'fp', 'fn', 'tn'}


def number_format(field, compact=False):
    if field in PERCENT or field == 'fraud_amount_share':
        return {'type': 'number-percent', 'decimalPlaces': {'type': 'max', 'places': 4}}
    result = {'type': 'number-plain' if field in COUNT_FIELDS else 'number',
              'decimalPlaces': {'type': 'max', 'places': 0 if field in COUNT_FIELDS else 2},
              'abbreviation': 'none', 'hideGroupSeparator': False}
    if compact:
        result['type'] = 'number'
        result['abbreviation'] = 'compact'
    return result


def polish(definition):
    definition['uiSettings'] = {
        'locale': 'en-US',
        'theme': {
            'canvasBackgroundColor': {'light': '#EAF0F7', 'dark': '#101B2B'},
            'widgetBackgroundColor': {'light': '#F7FAFF', 'dark': '#19283C'},
            'widgetBorderColor': {'light': '#D6E1EF', 'dark': '#30445E'},
            'fontColor': {'light': NAVY, 'dark': '#E7EFF9'},
            'selectionColor': {'light': '#2563EB', 'dark': '#8CB9FF'},
            'visualizationColors': [*TYPE_COLORS.values(), CORAL, '#78869A'],
            'widgetHeaderAlignment': 'LEFT', 'widgetCornerRadius': 10,
            'fontFamily': 'Arial'
        }
    }
    for page in definition['pages']:
        if page['pageType'] != 'PAGE_TYPE_CANVAS':
            continue
        layout = page['layout']
        layout[0]['position']['height'] = 2
        for item in layout[1:]:
            widget = item['widget']
            spec = widget['spec']
            kind = spec['widgetType']
            enc = spec['encodings']
            if kind == 'counter':
                field = enc['value']['fieldName']
                enc['value']['format'] = number_format(field, compact=field not in COUNT_FIELDS and field not in PERCENT and field != 'fraud_amount_share')
                if field in ('transactions', 'fraud_transactions'):
                    display = field + '_display'
                    widget['queries'][0]['query']['fields'].append({'name': display, 'expression': '`' + display + '`'})
                    enc['value']['formatTemplate'] = '{{ ' + display + ' }}'
            elif kind == 'table':
                for column in enc['columns']:
                    field = column['fieldName']
                    if field in COUNT_FIELDS or field in PERCENT or any(token in field for token in ('amount','balance','change','relative_rate')):
                        column['format'] = number_format(field)
                    if field == 'relative_rate':
                        column['displayName'] = 'Rate vs Population (×)'
            else:
                measure = enc['y']['fieldName']
                if enc['x']['fieldName'] == 'amount_band':
                    enc['x']['scale']['sort'] = {'by': 'custom-order', 'orderedValues': ['0', 'Over 0 to <1,000', '1,000 to <10,000', '10,000 to <100,000', '100,000 to <1,000,000', '1,000,000+']}
                enc['y']['format'] = number_format(measure, compact=measure not in PERCENT)
                enc['y']['displayName'] = LABELS.get(measure, measure)
                # Legacy chart formatter controls tick labels on runtimes that do not
                # yet use the per-encoding number format. Ratios stay ratios in SQL.
                pattern = '0.[00]a'
                if enc['x']['fieldName'] in ('simulation_step','account_rank'):
                    enc['x']['format'] = number_format(enc['x']['fieldName'])
                spec['format'] = {'numberFormat': {'formatType': 'numeral', 'format': pattern},
                                  'percentFormat': {'formatType': 'numeral', 'format': '0.[0000]%'}}
                fields = widget['queries'][0]['query']['fields']
                if enc['x']['fieldName']=='outcome':
                    enc['color']={'fieldName':'outcome','displayName':'Flag outcome','scale':{'type':'categorical','mappings':[
                        {'value':'True Negative','color':'#78869A'}, {'value':'False Negative','color':CORAL},
                        {'value':'True Positive','color':'#238B8D'}, {'value':'False Positive','color':'#C28B24'}]}}
                elif enc['x']['fieldName']=='transaction_type' and page['name']!='fraud':
                    enc['color']={'fieldName':'transaction_type','displayName':'Transaction Type','scale':{'type':'categorical','mappings':[
                        {'value':value,'color':color} for value,color in TYPE_COLORS.items()]}}
                elif 'color' in enc:
                    enc['color']['scale']['mappings'] = [{'value': value, 'color': color} for value, color in TYPE_COLORS.items()]
                else:
                    # The series must be a real dataset column: the client does not
                    # materialize literal expressions in the widget projection.
                    label = 'Fraud labels' if 'fraud' in measure or page['name']=='fraud' else 'Banking activity'
                    series = '_fraud_series' if label=='Fraud labels' else '_banking_series'
                    fields.append({'name': series, 'expression': '`' + series + '`'})
                    enc['color'] = {'fieldName': series, 'displayName': 'Series',
                                    'scale': {'type': 'categorical', 'mappings': [
                                        {'value': label, 'color': CORAL if label=='Fraud labels' else '#3274C5'}]}}
        if page['name'] == 'overview':
            # Four headline metrics, followed by amount detail; exact transaction counts.
            by_field = {item['widget']['spec']['encodings']['value']['fieldName']: item for item in layout[1:9]}
            order = ['transactions','total_amount','fraud_transactions','fraud_rate',
                     'average_amount','median_amount','fraud_amount','fraud_amount_share']
            cards = [by_field[name] for name in order]
            for i,item in enumerate(cards):
                item['position'].update(x=(i%4)*3, y=2+(i//4)*3, width=3, height=3)
            charts = layout[9:]
            charts[0]['position'].update(x=0,y=8,width=6,height=6)
            charts[1]['position'].update(x=6,y=8,width=6,height=6)
            charts[2]['position'].update(x=0,y=14,width=12,height=5)
            charts[3]['position'].update(x=0,y=19,width=12,height=3)
            page['layout'] = [layout[0],*cards,*charts]
        elif page['name'] == 'time':
            for i,item in enumerate(layout[1:5]):
                item['position'].update(x=(i%2)*6,y=2+(i//2)*6,width=6,height=6)
            for i,item in enumerate(layout[5:]):
                item['position'].update(x=0,y=14+i*6,width=12,height=6)
        elif page['name'] == 'fraud':
            chart, risk, comparison, bands, flag, metrics, balance = layout[1:]
            for i,item in enumerate([chart,flag]):
                item['position'].update(x=i*6,y=2,width=6,height=6)
            y = 11
            for item, height in [(risk,3),(comparison,3),(bands,3),(metrics,2),(balance,4)]:
                item['position'].update(x=0,y=y,width=12,height=height)
                y += height
            # Detail counters keep the rare flag outcomes visible beside the full matrix.
            for i,(field,title) in enumerate([('fn','Frauds Missed by PaySim Flag'),('tp','Frauds Caught by PaySim Flag'),('recall_flag','PaySim Flag Recall')]):
                card = {
                    'widget': {'name':'fraud_flag_'+field, 'queries':[{'name':'main_query','query':{'datasetName':'10_flag_metrics','fields':[{'name':field,'expression':'`'+field+'`'}],'disaggregated':True}}],
                    'spec':{'version':2,'widgetType':'counter','frame':{'showTitle':True,'title':title},'encodings':{'value':{'fieldName':field,'displayName':title,'format':number_format(field)}}}},
                    'position':{'x':i*4,'y':8,'width':4,'height':3}}
                page['layout'].append(card)
        elif page['name'] == 'accounts':
            for i,item in enumerate(layout[1:3]):
                item['position'].update(x=0,y=2+i*6,width=12,height=6)
            layout[3]['position'].update(x=0,y=14,width=5,height=6)
            layout[4]['position'].update(x=5,y=14,width=7,height=6)
            detail=layout[5]
            detail['position'].update(x=0,y=20,width=12,height=8)
            balance_columns=['transaction_key','origin_account','destination_account','origin_balance_before','origin_balance_after','destination_balance_before','destination_balance_after']
            balances=copy.deepcopy(detail)
            balances['widget']['name']='accounts_balances'
            balances['widget']['spec']['frame']['title']='Transaction Balances — Up to 500 Rows'
            balances['position'].update(y=28,height=8)
            for item, selected in [(detail,['transaction_key','simulation_step','transaction_type','origin_account','destination_account','amount','is_fraud','is_flagged_fraud']), (balances,balance_columns)]:
                fields=item['widget']['queries'][0]['query']['fields']
                item['widget']['queries'][0]['query']['fields']=[f for f in fields if f['name'] in selected]
                columns=item['widget']['spec']['encodings']['columns']
                item['widget']['spec']['encodings']['columns']=[c for c in columns if c['fieldName'] in selected]
                for f in item['widget']['queries'][0]['query']['fields']:
                    if f['name']=='transaction_key':
                        f['name']='transaction_reference'
                        f['expression']='substring(`transaction_key`, 1, 12)'
                for column in item['widget']['spec']['encodings']['columns']:
                    if column['fieldName']=='transaction_key':
                        column.update(fieldName='transaction_reference',displayName='Transaction Ref. (12 chars)')
            page['layout'].append(balances)
    # Presentation-only columns are evaluated by SQL, so the widget receives
    # non-null category values and can apply semantic colors consistently.
    needed = {w['widget']['queries'][0]['query']['datasetName']
              for page in definition['pages'] if page['pageType']=='PAGE_TYPE_CANVAS'
              for w in page['layout'] if any(f['name'] in ('_banking_series','_fraud_series')
                  for q in w['widget'].get('queries',[]) for f in q['query'].get('fields',[]))}
    ordering = {'03_hourly': 'simulation_step', '06_amount_distribution': 'band_order',
                '08_type_risk': 'fraud_transactions DESC', '13_concentration': 'account_rank'}
    for dataset in definition['datasets']:
        if dataset['name'] == '01_summary':
            sql = ''.join(dataset['queryLines']).strip().rstrip(';')
            dataset['queryLines'] = ("SELECT summary.*, format_number(transactions, 0) AS transactions_display, format_number(fraud_transactions, 0) AS fraud_transactions_display FROM (\n" + sql + "\n) summary\n").splitlines(keepends=True)
        if dataset['name'] in needed:
            sql = ''.join(dataset['queryLines']).strip().rstrip(';')
            dataset['queryLines'] = ("SELECT presentation.*, 'Banking activity' AS _banking_series, "
                                     "'Fraud labels' AS _fraud_series\nFROM (\n" + sql + "\n) presentation\nORDER BY " + ordering[dataset['name']] + "\n").splitlines(keepends=True)
    return definition


def parameter(name, default):
    p = {'keyword': name, 'displayName': {'types':'Transaction Types','fraud_labels':'Fraud Labels','hour_min':'First Simulation Hour','hour_max':'Last Simulation Hour','amount_min':'Minimum Amount','amount_max':'Maximum Amount'}[name], 'dataType': 'STRING', 'defaultSelection': {'values': {'dataType': 'STRING', 'values': [] if default is None else [{'value': default}]}}}
    if default is None:
        p['complexType'] = 'MULTI'
    return p


def build():
    datasets = []
    for file in sorted(SQL.glob('[0-9]*.sql')):
        datasets.append({'name': file.stem, 'displayName': file.stem[3:].replace('_',' ').title(),
                         'queryLines': (SQL.joinpath('filter.sql').read_text() + file.read_text()).splitlines(keepends=True),
                         'parameters': [parameter(k,v) for k,v in PARAMS.items()]})
    datasets.append({'name': 'filter_choices', 'displayName':'Filter Options', 'queryLines': ["SELECT transaction_type, CASE WHEN is_fraud=1 THEN 'Fraudulent' ELSE 'Non-fraudulent' END AS fraud_label FROM transactions_dashboard"]})
    pages = []
    filters = []
    for i,(param,label) in enumerate([('types','Transaction Types'),('fraud_labels','Fraud Label'),('hour_min','First Simulation Hour (1–744)'),('hour_max','Last Simulation Hour (1–744)'),('amount_min','Minimum Amount'),('amount_max','Maximum Amount')]):
        queries, fields = [], []
        if param in ('types','fraud_labels'):
            column = 'transaction_type' if param=='types' else 'fraud_label'
            queries.append({'name':'choices','query':{'datasetName':'filter_choices','fields':[{'name':column,'expression':f'`{column}`'}],'disaggregated':False}})
            fields.append({'fieldName':column,'queryName':'choices'})
        for ds in datasets[:-1]:
            q = 'q_'+ds['name']
            queries.append({'name':q,'query':{'datasetName':ds['name'],'parameters':[{'name':param,'keyword':param}],'disaggregated':False}})
            fields.append({'parameterName':param,'queryName':q})
        filters.append({'widget':{'name':'filter_'+param,'queries':queries,'spec':{'version':2,'widgetType':'filter-multi-select' if PARAMS[param] is None else 'filter-text-entry','encodings':{'fields':fields},'frame':{'showTitle':True,'title':label}}},'position':{'x':(i%2)*6,'y':(i//2)*2,'width':6,'height':2}})
    pages.append({'name':'filters','displayName':'Global Filters','pageType':'PAGE_TYPE_GLOBAL_FILTERS','layoutVersion':'GRID_V1','layout':filters})
    def page(name,title,note):
        layout=[{'widget':{'name':name+'_intro','multilineTextboxSpec':{'lines':['# '+title+'\n\n'+note]}},'position':{'x':0,'y':0,'width':12,'height':3}}]
        pages.append({'name':name,'displayName':title,'pageType':'PAGE_TYPE_CANVAS','layoutVersion':'GRID_V1','layout':layout})
        return layout
    def widget(layout,ds,title,columns,kind='table',x=0,y=3,width=12,height=6,color=None,percent=False):
        fields=[{'name':c,'expression':f'`{c}`'} for c in columns]
        spec={'version':3 if kind in ['bar','line'] else 2,'widgetType':kind,'frame':{'showTitle':True,'title':title}}
        if kind=='counter':
            spec['encodings']={'value':{'fieldName':columns[0],'displayName':title,'format':{'type':'number-percent' if percent else 'number','decimalPlaces':{'type':'max','places':4 if percent else 2}}}}
        elif kind=='table':
            spec['encodings']={'columns':[dict({'fieldName':c,'displayName':LABELS.get(c,c.replace('_',' '))}, **({'format':{'type':'number-percent','decimalPlaces':{'type':'max','places':4}}} if c in PERCENT else {})) for c in columns]}
        else:
            spec['encodings']={'x':{'fieldName':columns[0],'displayName':LABELS.get(columns[0],columns[0].replace('_',' ').title()),'scale':{'type':'quantitative' if columns[0] in ('simulation_step','account_rank') else 'categorical'}},'y':{'fieldName':columns[1],'displayName':LABELS.get(columns[1],columns[1])+(' (ratio 0–1)' if columns[1] in PERCENT else ''),'scale':{'type':'quantitative'}}}
            if color:
                spec['encodings']['color']={'fieldName':color,'displayName':LABELS.get(color,color.replace('_',' ').title()),'scale':{'type':'categorical'}}
        layout.append({'widget':{'name':f'{pages[-1]["name"]}_{len(layout)}','queries':[{'name':'main_query','query':{'datasetName':ds,'fields':fields,'disaggregated':True}}],'spec':spec},'position':{'x':x,'y':y,'width':width,'height':height}})
    note='Synthetic PaySim banking transactions • Selected snapshot and filters • Simulation hours, no calendar dates or assigned currency • Imported data, not a live feed. Percentiles are approximate.'
    layout=page('overview','Overview',note)
    for i,(col,title,pct) in enumerate([('transactions','Transactions',False),('total_amount','Total Amount',False),('average_amount','Average Amount',False),('median_amount','Approximate Median Amount',False),('fraud_transactions','Fraudulent Transactions (PaySim Labels)',False),('fraud_rate','Fraud Rate',True),('fraud_amount','Fraud Amount',False),('fraud_amount_share','Fraudulent Share of Total Amount',True)]):
        widget(layout,'01_summary',title,[col],'counter',x=(i%4)*3,y=3+(i//4)*3,width=3,height=3,percent=pct)
    widget(layout,'02_by_type','Transactions by Type',['transaction_type','transactions'],'bar',y=9,width=6)
    widget(layout,'02_by_type','Amounts by Type',['transaction_type','total_amount'],'bar',x=6,y=9,width=6)
    widget(layout,'06_amount_distribution','Amount Distribution — Transaction Counts',['amount_band','transactions'],'bar',y=15,width=6)
    widget(layout,'02_by_type','Summary by Type',['transaction_type','transactions','total_amount','average_amount','median_amount','fraud_transactions','fraud_rate'],x=6,y=15,width=6)
    layout=page('time','Activity Over Time','Activity across the selected simulation hours. Hours are relative, not calendar dates. The first hourly change is undefined; simulated patterns do not establish real-world seasonality or forecasts.')
    for i,(ds,title,cols,kind,color) in enumerate([
        ('03_hourly','Transactions by Simulation Hour',['simulation_step','transactions'],'line',None),
        ('03_hourly','Amounts by Simulation Hour',['simulation_step','total_amount'],'line',None),
        ('04_hour_type','Transactions by Type and Hour',['simulation_step','transactions','transaction_type'],'line','transaction_type'),
        ('03_hourly','Fraudulent Transactions by Hour',['simulation_step','fraud_transactions'],'bar',None),
        ('05_hour_change','Change in Observed Transaction Counts',['simulation_step','transaction_change','transaction_growth'],'table',None),
        ('03_hourly','Hourly Metrics',['simulation_step','transactions','total_amount','average_amount','fraud_transactions','fraud_rate'],'table',None)]):
        widget(layout,ds,title,cols,kind,x=(i%2)*6,y=3+(i//2)*6,width=6,color=color)
    layout=page('fraud','Fraud Analysis','Labels identify known simulated fraud, not predictions. The PaySim flag is not our trained model. Undefined precision remains NULL; high accuracy can conceal poor fraud recall. Relative rates use the filtered population.')
    for i,(ds,title,cols,kind) in enumerate([
        ('08_type_risk','Fraud Rate by Type',['transaction_type','fraud_rate'],'bar'),
        ('08_type_risk','Fraud by Type and Relative Rate',['transaction_type','transactions','fraud_transactions','fraud_rate','relative_rate','fraud_amount'],'table'),
        ('07_fraud_comparison','Fraud vs Non-fraud — Amount Distribution',['fraud_label','transactions','total_amount','average_amount','median_amount','p90_amount','p99_amount','maximum_amount'],'table'),
        ('06_amount_distribution','Fraud by Amount Band',['amount_band','transactions','fraud_transactions','fraud_rate'],'table'),
        ('09_flag_matrix','PaySim Flag vs Ground Truth',['outcome','transactions'],'bar'),
        ('10_flag_metrics','Descriptive Evaluation of the PaySim Flag',['tp','fp','fn','tn','precision_flag','recall_flag','f1_flag','accuracy_flag'],'table'),
        ('15_balance_changes','Observed Balance Changes per Transaction',['transaction_type','fraud_label','transactions','average_amount','average_origin_change','average_destination_change','origin_zero_after'],'table')]):
        widget(layout,ds,title,cols,kind,x=(i%2)*6,y=3+(i//2)*6,width=12 if i==6 else 6)
    layout=page('accounts','Accounts and Transactions','Top 20 accounts per role, 30 fraud-related roles and 500 transaction details. Role counts are not unique frauds; transaction balances are not consolidated balances. Display references use 12 characters; full IDs remain in the dataset.')
    for i,(ds,title,cols,kind) in enumerate([
        ('11_origin_accounts','Top 20 Origin Accounts by Amount',['origin_account','transactions','total_amount','distinct_counterparties','fraud_transactions'],'table'),
        ('12_destination_accounts','Top 20 Destination Accounts by Amount',['destination_account','transactions','total_amount','distinct_counterparties','fraud_transactions'],'table'),
        ('13_concentration','Cumulative Amount Share — Top 20 Origin Accounts',['account_rank','cumulative_share'],'line'),
        ('14_fraud_accounts','Fraud-related Accounts by Role',['account_role','account','fraud_participations','fraud_amount'],'table')]):
        widget(layout,ds,title,cols,kind,x=(i%2)*6,y=3+(i//2)*6,width=6)
    widget(layout,'16_transactions','Filtered Transactions — Up to 500 Rows',['transaction_key','simulation_step','transaction_type','origin_account','destination_account','amount','is_fraud','is_flagged_fraud','origin_balance_before','origin_balance_after','destination_balance_before','destination_balance_after'],y=15,height=9)
    return polish({'datasets':datasets,'pages':pages})


if __name__=='__main__':
    path=ROOT/'databricks/dashboards/paysim.lvdash.json'
    path.write_text(json.dumps(build(),ensure_ascii=False,indent=2)+'\n')
    print('PASS: Dashboard généré : '+str(path))
