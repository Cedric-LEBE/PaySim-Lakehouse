"""Generate the versioned AI/BI dashboard from reviewed SQL analyses (stdlib only)."""
import json
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / 'sql/analytics'
LABELS = {'transactions':'Transactions','transaction_type':'Type','total_amount':'Total Amount','average_amount':'Average Amount','median_amount':'Approximate Median','fraud_transactions':'Fraudulent Transactions','fraud_rate':'Fraud Rate','fraud_amount':'Fraud Amount','relative_rate':'Rate Relative to Filtered Population','simulation_step':'Simulation Hour','transaction_change':'Count Change','transaction_growth':'Relative Change','origin_account':'Origin Account','destination_account':'Destination Account','distinct_counterparties':'Distinct Counterparties','account_rank':'Rank','cumulative_share':'Cumulative Share','fraud_label':'PaySim Label','amount_band':'Amount Band','precision_flag':'Flag Precision','recall_flag':'Flag Recall','accuracy_flag':'Flag Accuracy','f1_flag':'Flag F1 Score','outcome':'Outcome','account_role':'Role','account':'Account','fraud_participations':'Fraud-related Role Count','amount':'Amount','is_fraud':'Fraud Label','is_flagged_fraud':'PaySim Flag','transaction_key':'Transaction ID','p90_amount':'90th Percentile Amount','p99_amount':'99th Percentile Amount','maximum_amount':'Maximum Amount','average_origin_change':'Average Origin Balance Change','average_destination_change':'Average Destination Balance Change','origin_zero_after':'Origin Balance Zero After Transaction','origin_balance_before':'Origin Balance Before','origin_balance_after':'Origin Balance After','destination_balance_before':'Destination Balance Before','destination_balance_after':'Destination Balance After','tp':'True Positives','fp':'False Positives','fn':'False Negatives','tn':'True Negatives'}
PERCENT = {'fraud_rate','transaction_growth','cumulative_share','amount_share','precision_flag','recall_flag','accuracy_flag','f1_flag'}
PARAMS = {'types': None, 'fraud_labels': None, 'hour_min': '1', 'hour_max': '744', 'amount_min': '0', 'amount_max': '999999999999999999.99'}


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
    note='Synthetic PaySim data • Validated reference: first 250,000 rows, 14 simulation hours • No currency assigned • Imported snapshot, not a live feed. Filters apply before aggregation; percentiles are approximate.'
    layout=page('overview','Overview',note)
    for i,(col,title,pct) in enumerate([('transactions','Transactions',False),('total_amount','Total Amount',False),('average_amount','Average Amount',False),('median_amount','Approximate Median Amount',False),('fraud_transactions','Fraudulent Transactions (PaySim Labels)',False),('fraud_rate','Fraud Rate',True),('fraud_amount','Fraud Amount',False),('fraud_amount_share','Fraudulent Share of Total Amount',True)]):
        widget(layout,'01_summary',title,[col],'counter',x=(i%4)*3,y=3+(i//4)*3,width=3,height=3,percent=pct)
    widget(layout,'02_by_type','Transactions by Type',['transaction_type','transactions'],'bar',y=9,width=6)
    widget(layout,'02_by_type','Amounts by Type',['transaction_type','total_amount'],'bar',x=6,y=9,width=6)
    widget(layout,'06_amount_distribution','Amount Distribution — Transaction Counts',['amount_band','transactions'],'bar',y=15,width=6)
    widget(layout,'02_by_type','Summary by Type',['transaction_type','transactions','total_amount','average_amount','median_amount','fraud_transactions','fraud_rate'],x=6,y=15,width=6)
    layout=page('time','Activity Over Time','Relative simulation hours, not calendar dates. The first hourly change is undefined. This sample does not support daily or weekly seasonality analysis or forecasting.')
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
    layout=page('accounts','Accounts and Transactions','Display limits: 20 origin accounts, 20 destination accounts, 30 fraud-related account roles and 500 transaction details. One fraudulent transaction can appear in both account roles; role counts are not unique fraud counts. Summed transaction balances are not consolidated account balances.')
    for i,(ds,title,cols,kind) in enumerate([
        ('11_origin_accounts','Top 20 Origin Accounts by Amount',['origin_account','transactions','total_amount','distinct_counterparties','fraud_transactions'],'table'),
        ('12_destination_accounts','Top 20 Destination Accounts by Amount',['destination_account','transactions','total_amount','distinct_counterparties','fraud_transactions'],'table'),
        ('13_concentration','Cumulative Amount Share — Top 20 Origin Accounts',['account_rank','cumulative_share'],'line'),
        ('14_fraud_accounts','Fraud-related Accounts by Role',['account_role','account','fraud_participations','fraud_amount'],'table')]):
        widget(layout,ds,title,cols,kind,x=(i%2)*6,y=3+(i//2)*6,width=6)
    widget(layout,'16_transactions','Filtered Transactions — Up to 500 Rows',['transaction_key','simulation_step','transaction_type','origin_account','destination_account','amount','is_fraud','is_flagged_fraud','origin_balance_before','origin_balance_after','destination_balance_before','destination_balance_after'],y=15,height=9)
    return {'datasets':datasets,'pages':pages}


if __name__=='__main__':
    path=ROOT/'databricks/dashboards/paysim.lvdash.json'
    path.write_text(json.dumps(build(),ensure_ascii=False,indent=2)+'\n')
    print('PASS: Dashboard généré : '+str(path))
