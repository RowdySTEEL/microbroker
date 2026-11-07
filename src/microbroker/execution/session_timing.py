"""Bounded local timing observations. Wall-clock deltas are not execution latency."""
from collections import defaultdict
from contextlib import contextmanager
import math
import threading
import time
from ..data.alignment import nanos


def distribution(values):
    valid=sorted(v for v in values if v>=0 and math.isfinite(v))
    return {'samples':len(valid),'invalid_or_negative_samples':len(values)-len(valid),
        'median_seconds':((valid[(len(valid)-1)//2]+valid[len(valid)//2])/2) if valid else None,
        'p95_seconds':valid[math.ceil(.95*len(valid))-1] if valid else None,
        'max_seconds':valid[-1] if valid else None,'p95_method':'nearest_rank'}


class Timings:
    def __init__(self,clock=time.monotonic):
        self.clock=clock;self.lock=threading.Lock();self.values=defaultdict(list);self.dropped=0

    @contextmanager
    def measure(self,stage):
        start=self.clock()
        try:yield
        finally:
            elapsed=self.clock()-start
            self.record(stage,elapsed)

    def record(self,stage,elapsed):
        with self.lock:
            if sum(map(len,self.values.values()))<4096:self.values[stage].append(elapsed)
            else:self.dropped+=1

    def summary(self):
        with self.lock:return {'format':'R29','duration_clock':'monotonic',
            'stages':{k:distribution(v) for k,v in sorted(self.values.items())},'dropped_samples':self.dropped,
            'portfolio_capture_semantics':'new_snapshot_success_only',
            'portfolio_capture_attempt_semantics':'capture_calls_including_failed_and_existing_results_excluding_slot_checks',
            'overlapping_stages_must_not_be_summed':True,'latency_caused_lost_alpha_validated':False}


class TimedAPI:
    """Only named public operations; never retain URL, payload, headers or errors."""
    def __init__(self,api,timing,*,observer=False,check=None):
        self.api=api;self.timing=timing;self.observer=observer;self.check=check
    def now(self):return self.api.now()
    def __getattr__(self,name):
        allowed={'account','clock','open_orders','valued_positions'} if self.observer else {
            'account','clock','open_orders','valued_positions','positions','asset','lookup','submit','cancel'}
        if name not in allowed:raise AttributeError(name)
        target=getattr(self.api,name)
        def call(*args,**kwargs):
            if self.check:self.check()
            with self.timing.measure(('observer_' if self.observer else 'broker_')+name):
                result=target(*args,**kwargs)
            if self.check:self.check()
            return result
        return call


def pipeline_latency(records,orders):
    values=defaultdict(list)
    for record in records:
        news,analysis=record['news'],record['analysis']
        for name,start,end in [('publication_to_first_receipt',news['data']['created_at'],news['first_seen_at']),
                               ('first_receipt_to_analysis_recorded',news['first_seen_at'],analysis['analyzed_at'])]:
            values[name].append((nanos(end)-nanos(start))/1e9)
    for request,claimed,filled in orders:
        if claimed:values['analysis_recorded_to_post_claim'].append((nanos(claimed)-nanos(request['record']['analysis']['analyzed_at']))/1e9)
        if claimed and filled:values['post_claim_to_first_local_filled_observation'].append((nanos(filled)-nanos(claimed))/1e9)
    names=('publication_to_first_receipt','first_receipt_to_analysis_recorded','analysis_recorded_to_post_claim',
           'post_claim_to_first_local_filled_observation')
    return {'format':'R28','stages':{k:distribution(values[k]) for k in names},
        'scope':'current_revision_records_and_managed_decisions_not_random_news_sample',
        'wall_clock_deltas_not_monotonic_durations':True,'receipt_and_analysis_times_are_not_commit_times':True,
        'actual_broker_fill_timestamp_measured':False,'broker_update_time_used_as_fill_time':False,
        'filled_observation_includes_reconciliation_delay':True,'latency_caused_lost_alpha_validated':False}
