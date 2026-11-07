"""Descriptive event returns from frozen first-base receipts; no order capability."""
from collections import Counter,defaultdict
from datetime import datetime,timedelta,time,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from zoneinfo import ZoneInfo
from .alignment import nanos
from .models import utc_datetime,DataValidationError
from .news_replay import state_at
from .storage.repository import StorageError
from ..intelligence.policy import digest
from ..intelligence.research_decision import company_research_label

MINUTE=60_000_000_000
HORIZONS=(1,5,15,60)
MAX_EVENTS=2000
NY=ZoneInfo('America/New_York')
POLICY={'method':'r30-first-current-company-analysis-next-full-minute-v1',
    'benchmark':'SPY','horizons_minutes':list(HORIZONS),
    'reference':'close_of_first_full_minute_starting_at_or_after_feature',
    'horizon_anchor':'reference_bar_completion_not_publication_or_analysis_timestamp',
    'price_selection':'first_base_receipt_per_symbol_minute_updates_excluded',
    'max_receipt_delay_seconds':120,'continuous_minutes_required':True,
    'market_adjustment':'asset_simple_return_minus_spy_simple_return_beta_assumed_one',
    'event_selection':'first_current_analyzed_revision_per_article_company_in_window_including_withheld',
    'regular_clock_window':'America/New_York_weekdays_09:30_to_16:00_bar_starts',
    'exchange_calendar_validated':False,'thresholds_calibrated':False}


def timestamp(ns):
    seconds,fraction=divmod(ns,1_000_000_000)
    return datetime.fromtimestamp(seconds,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')+f'.{fraction:09d}Z'


def regular_minute(ns):
    local=utc_datetime(timestamp(ns)).astimezone(NY)
    return local.weekday()<5 and time(9,30)<=local.time()<time(16)


def number(value):return format(value,'.8f') if value is not None else None


def simple_bps(start,end):
    with localcontext() as ctx:
        ctx.prec=50
        return (Decimal(end)/Decimal(start)-1)*10000


def distribution(values):
    values=sorted(Decimal(v) for v in values if v is not None)
    if not values:return {'n':0,'mean_bps':None,'median_bps':None,'min_bps':None,'max_bps':None}
    with localcontext() as ctx:
        ctx.prec=50
        return {'n':len(values),'mean_bps':number(sum(values)/len(values)),
            'median_bps':number((values[(len(values)-1)//2]+values[len(values)//2])/2),
            'min_bps':number(values[0]),'max_bps':number(values[-1])}


def event_rows(records,symbols,start,end):
    """Evaluate visibility at each feature time, never replace it with a future label."""
    by_article=defaultdict(list)
    for item in records:by_article[item['news']['article_id']].append(item['news'])
    chosen=set();rows=[]
    def order(item):
        a=item['analysis'];n=item['news']
        return (a['timestamp'] if a else n['timestamp'],n['article_id'],n['revision_hash'])
    for item in sorted(records,key=order):
        news=item['news'];analysis=item['analysis'];at=analysis['timestamp'] if analysis else news['timestamp']
        if not start<=at<=end:continue
        visible=state_at(by_article[news['article_id']],at)
        current=visible[0]['revision_hash'] if visible else None
        for symbol in sorted(set(news['mappings'])&set(symbols)):
            if len(rows)>=MAX_EVENTS:raise StorageError('Event study exceeds 2000 article/company revision rows')
            company=(analysis['result'].get('entity_sentiments',{}).get(symbol) or {}) if analysis else {}
            row={'event_id':digest({'article_id':news['article_id'],'revision_hash':news['revision_hash'],'symbol':symbol}),
                'article_id':news['article_id'],'revision_hash':news['revision_hash'],'symbol':symbol,
                'headline':news['data']['headline'],'published_at':news['data']['created_at'],
                'first_seen_at':news['first_seen_at'],'feature_available_at':analysis['timestamp'] if analysis else None,
                'company_label':None,'analysis_format':analysis['result'].get('analysis_format') if analysis else None,
                'company_context_status':company.get('status'),
                'company_research_status':(company.get('research_decision') or {}).get('status'),
                'publication_to_receipt_seconds':(nanos(news['first_seen_at'])-nanos(news['data']['created_at']))/1e9,
                'receipt_to_feature_seconds':(nanos(at)-nanos(news['first_seen_at']))/1e9 if analysis else None,
                'outcomes':[],'baseline':None}
            identity=(news['article_id'],symbol)
            if analysis is None:status='analysis_unavailable'
            elif analysis['result'].get('analysis_format')!='R18':status='legacy_analysis_not_supported'
            elif current!=news['revision_hash']:status='revision_not_current_at_feature'
            elif identity in chosen:status='later_revision_not_independent_event'
            else:
                chosen.add(identity)
                label=company_research_label(analysis['result'],company) if company.get('status')=='scored' else None
                if label in {'positive','negative','neutral'}:row['company_label']=label;status='eligible'
                else:status='company_decision_unavailable_or_withheld'
            row['status']=status;rows.append(row)
    return rows


class Prices:
    def __init__(self,bars,end):
        self.end=nanos(end);self.minutes={};self.counts=Counter()
        previous=None
        for bar in bars:
            receipt=bar['receipt'];at=nanos(receipt['received_at'])
            if previous is not None and at<previous:raise StorageError('Event study requires nondecreasing receipt timestamps')
            previous=at
            if at>self.end:self.counts['receipts_after_study_cutoff']+=1;continue
            if receipt['event_type']!='b':self.counts['updated_receipts_excluded']+=1;continue
            key=(bar['symbol'],nanos(bar['timestamp']))
            if key in self.minutes:self.counts['later_base_receipts_excluded']+=1;continue
            completion=key[1]+MINUTE
            reason=('receipt_before_completion' if at<completion else
                'receipt_delay_exceeds_120_seconds' if at-completion>120_000_000_000 else None)
            if reason:self.counts[reason]+=1
            self.minutes[key]={'bar':bar,'completion_ns':completion,'receipt_ns':at,'reason':reason}
            self.counts['first_base_receipts']+=1

    def point(self,symbol,start):
        point=self.minutes.get((symbol,start))
        if point is None:return None,'minute_not_recorded_by_cutoff'
        return point,point['reason']

    def window(self,symbol,start,horizon):
        for minute in range(horizon+1):
            point,reason=self.point(symbol,start+minute*MINUTE)
            if reason:return None,reason
        return point,None


def price_view(point):
    b=point['bar'];r=b['receipt']
    return {'symbol':b['symbol'],'bar_start':b['timestamp'],'completion_floor':timestamp(point['completion_ns']),
        'close':b['close'],'received_at':r['received_at'],'receipt_ordinal':r['ordinal'],'event_key':r['event_key']}


def calculate(records,bars,source):
    with localcontext() as ctx:
        ctx.prec=50;ctx.rounding=ROUND_HALF_EVEN
        return _calculate(records,bars,source)


def _calculate(records,bars,source):
    rows=event_rows(records,source['symbols'],source['start'],source['event_cutoff']);prices=Prices(bars,source['end'])
    for row in rows:
        if row['status']!='eligible':continue
        feature=nanos(row['feature_available_at']);reference=((feature+MINUTE-1)//MINUTE)*MINUTE
        row['reference_bar_start']=timestamp(reference);row['reference_completed_at']=timestamp(reference+MINUTE)
        row['feature_to_reference_completion_seconds']=(reference+MINUTE-feature)/1e9
        baseline,base_reason=prices.point(row['symbol'],reference)
        spy_base,spy_base_reason=prices.point('SPY',reference)
        if baseline and not base_reason:
            row['baseline']=price_view(baseline)
            row['feature_to_reference_receipt_seconds']=(baseline['receipt_ns']-feature)/1e9
        for horizon in HORIZONS:
            target=reference+horizon*MINUTE;target_end=target+MINUTE
            outcome={'horizon_minutes':horizon,'target_completed_at':timestamp(target_end),
                'feature_to_target_completion_seconds':(target_end-feature)/1e9,
                'status':None,'benchmark_status':None,'asset_return_bps':None,'spy_return_bps':None,
                'market_adjusted_return_bps':None,'directional_market_adjusted_bps':None,
                'price':None,'spy_baseline':None,'spy_price':None}
            row['outcomes'].append(outcome)
            if not regular_minute(reference):outcome['status']='reference_outside_regular_clock_hours';continue
            if not regular_minute(target) or utc_datetime(timestamp(reference)).astimezone(NY).date()!=utc_datetime(timestamp(target)).astimezone(NY).date():
                outcome['status']='horizon_outside_same_regular_clock_day';continue
            if target_end>prices.end:outcome['status']='right_censored_by_snapshot_end';continue
            if base_reason:outcome['status']='asset_reference_'+base_reason;continue
            final,reason=prices.window(row['symbol'],reference,horizon)
            if reason:outcome['status']='asset_window_'+reason;continue
            if baseline['receipt_ns']>=target_end:outcome['status']='asset_reference_observed_after_target_completion';continue
            outcome.update(status='observed',price=price_view(final))
            asset=simple_bps(baseline['bar']['close'],final['bar']['close']);outcome['asset_return_bps']=number(asset)
            if not source['benchmark_in_snapshot']:outcome['benchmark_status']='SPY_not_in_snapshot';continue
            if spy_base_reason:outcome['benchmark_status']='spy_reference_'+spy_base_reason;continue
            spy_final,reason=prices.window('SPY',reference,horizon)
            if reason:outcome['benchmark_status']='spy_window_'+reason;continue
            if max(baseline['receipt_ns'],spy_base['receipt_ns'])>=target_end:
                outcome['benchmark_status']='paired_reference_observed_after_target_completion';continue
            spy=simple_bps(spy_base['bar']['close'],spy_final['bar']['close']);adjusted=asset-spy
            outcome.update(benchmark_status='matched',spy_return_bps=number(spy),market_adjusted_return_bps=number(adjusted),
                directional_market_adjusted_bps=number(adjusted if row['company_label']=='positive' else -adjusted) if row['company_label']!='neutral' else None,
                spy_baseline=price_view(spy_base),spy_price=price_view(spy_final))
    groups={}
    for label in ('positive','negative','neutral'):
        selected=[r for r in rows if r['company_label']==label and r['status']=='eligible'];groups[label]={}
        for horizon in HORIZONS:
            outcomes=[o for r in selected for o in r['outcomes'] if o['horizon_minutes']==horizon]
            groups[label][str(horizon)]={'article_company_rows':len(selected),
                'status_counts':dict(Counter(o['status'] for o in outcomes)),
                'benchmark_status_counts':dict(Counter(o['benchmark_status'] for o in outcomes if o['benchmark_status'])),
                'asset':distribution(o['asset_return_bps'] for o in outcomes),
                'spy':distribution(o['spy_return_bps'] for o in outcomes),
                'asset_on_benchmark_matched_rows':distribution(o['asset_return_bps'] for o in outcomes if o['benchmark_status']=='matched'),
                'market_adjusted':distribution(o['market_adjusted_return_bps'] for o in outcomes),
                'directional_market_adjusted':distribution(o['directional_market_adjusted_bps'] for o in outcomes)}
    return rows,groups,dict(prices.counts)
