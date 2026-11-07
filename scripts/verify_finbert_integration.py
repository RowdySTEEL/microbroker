"""Reproduce real-model integration on seven synthetic articles; no user DB/API."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from microbroker.intelligence.sentiment import FinBertEngine
from microbroker.intelligence.analysis import AnalysisRequest, analyze_news, analysis_summary
from microbroker.data.news import normalize_news, collect_news, NewsRequest
from microbroker.data.storage.repository import Repository
from microbroker.data.models import utc_datetime

parser = argparse.ArgumentParser()
parser.add_argument('--model-dir', default='models/finbert')
parser.add_argument('--output', default='docs/releases/R6_FINBERT_INTEGRATION.json')
args = parser.parse_args()
engine = FinBertEngine(args.model_dir)
rows = [
    {'id':1,'headline':'Microsoft beats expectations and raises guidance.','symbols':['MSFT'],'content':'<p>Microsoft revenue rose and profit increased.</p>'},
    {'id':2,'headline':'Apple cuts guidance as revenue fell.','symbols':['AAPL'],'content':''},
    {'id':3,'headline':'Microsoft announces the date of its quarterly report.','symbols':['MSFT'],'content':''},
    {'id':4,'headline':'Microsoft raises guidance while Apple cuts guidance.','symbols':['MSFT','AAPL'],'content':'<p>Microsoft sales grew. Apple revenue fell.</p>'},
    {'id':5,'headline':'How much an investor would have made owning Microsoft in the last 20 years','symbols':['MSFT'],'content':''},
    {'id':6,'headline':'Security vendor introduces a new platform','symbols':['MSFT'],'content':''},
    {'id':7,'headline':'Microsoft quarterly report','symbols':['MSFT'],'content':'<p>'+('Microsoft published a report. '*1000)+'</p>'},
]
for row in rows:
    row.update(created_at='2026-01-05T14:30:00Z',updated_at='2026-01-05T14:30:00Z',
               source='original-engineering-fixture',url='https://example.com/test/'+str(row['id']))
class Fixture:
    pages = 1
    def iter_news(self, request):
        for row in rows: yield normalize_news(row)
repository = Repository('sqlite:///:memory:')
try:
    collect_news(Fixture(),repository,NewsRequest(('AAPL','MSFT'),utc_datetime('2026-01-05T00:00:00Z'),utc_datetime('2026-01-06T00:00:00Z')))
    request = AnalysisRequest(symbols=('AAPL','MSFT'))
    first = analyze_news(repository,request,engine)
    second = analyze_news(repository,request,engine)
    summary = analysis_summary(repository,request)
    mixed=next(s for s in summary['samples'] if s['id']==4)
    long=next(s for s in summary['samples'] if s['id']==7)
    metadata_only=next(s for s in summary['samples'] if s['id']==6)
    if not (first['inserted_analyses']==7 and second['existing_analyses']==7 and second['inserted_analyses']==0 and
            summary['analyzed_current_articles']==7 and summary['pending_articles']==0 and
            summary['flag_counts'].get('token_truncated',0)==0 and summary['flag_counts']['character_truncated']==1 and
            summary['flag_counts']['metadata_only_mapping']==1 and summary['flag_counts']['historical_review']==1 and
            long['sentiment']['chunk_count']>1 and
            long['sentiment']['covered_content_tokens']==long['sentiment']['content_token_count'] and
            metadata_only['entity_sentiments']['MSFT']['sentiment'] is None and
            mixed['entity_sentiments']['MSFT']['sentiment']['score']>0 and
            mixed['entity_sentiments']['AAPL']['sentiment']['score']<0 and
            mixed['entity_sentiments']['MSFT']['shared_units_excluded']==1):
        raise SystemExit('Real FinBERT integration failed')
    import platform
    output = {'status':'passed','tested_at_utc':datetime.now(timezone.utc).isoformat(),
              'environment':platform.system()+' CPU / torch '+engine.metadata['torch']+' / transformers '+engine.metadata['transformers'],
              'fixture_source':'seven original synthetic articles, not user real news corpus',
              'first':first,'repeat':second,'summary':summary,'model':engine.metadata,
              'model_accuracy_validated':False,'entity_attribution_validated':False,'user_database_modified':False}
    Path(args.output).write_text(json.dumps(output,indent=2)+'\n')
    print(json.dumps({'status':'passed','engine':'finbert','articles':7,'repeat_existing':7,
                      'model_accuracy_validated':False,'user_database_modified':False}))
finally:
    repository.close()
