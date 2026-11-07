"""Bounded synchronous GET-only observations around an existing fenced Paper worker."""
from copy import deepcopy
from contextlib import nullcontext
import logging
from .paper_api import PaperBrokerError
from .paper_portfolio import capture_portfolio,read_history,validate_sampling
from ..data.research import begin_read
from ..data.alignment import nanos
from ..data.stream_models import canonical_time
from ..data.storage.repository import StorageError

POLICY={'method':'r26-session-portfolio-observations-v1','interval_seconds':300,
        'capture_at_start':True,'capture_at_finalization':True,'backfill_missing_slots':False}
log=logging.getLogger('microbroker.portfolio')


class PortfolioSampler:
    def __init__(self,repository,api,account_ref,session_id,started_at,*,_passive=False,_commit_gate=None,_timing=None):
        validate_sampling({'format':'R26','session_id':session_id,'slot':'start','interval_seconds':300})
        self.repository=repository;self.api=api;self.account_ref=account_ref;self.session_id=session_id
        self.started_at=canonical_time(started_at);self.attempts=[];self.recorded={};self.attempted=set()
        self.passive=_passive;self.commit_gate=_commit_gate
        self.timing=_timing
        begin_read(repository)
        try:
            history,_=read_history(repository,account_ref);repository.connection.commit()
        except BaseException:repository.connection.rollback();raise
        for row in history:
            meta=row['inputs'].get('sampling',{})
            if meta.get('session_id')==session_id:
                self.recorded[meta['slot']]={'snapshot_id':row['snapshot_id'],'observed_at':row['observed_at']}
                self.attempted.add(meta['slot'])

    def capture(self,stage='interval'):
        with self.timing.measure('portfolio_slot_check') if self.timing else nullcontext():
            meta=self.reserve(stage)
        if meta is None:return None
        slot=meta['slot'];at=canonical_time(self.api.now())
        record={'slot':slot,'attempted_at':at,'status':'attempted'};self.attempts.append(record)
        started=self.timing.clock() if self.timing else None;measurement_recorded=False
        def measured(outcome):
            nonlocal measurement_recorded
            if self.timing and not measurement_recorded:
                measurement_recorded=True
                elapsed=self.timing.clock()-started
                self.timing.record('portfolio_capture_attempt',elapsed)
                self.timing.record(outcome,elapsed)
        try:
            captured=capture_portfolio(self.repository,self.api,snapshot_id='r26-'+self.session_id+'-'+slot,
                                       _sampling=meta,_account_ref=self.account_ref,
                                       _passive=self.passive,_commit_gate=self.commit_gate)
            measured('portfolio_capture' if captured['status']=='captured' else 'portfolio_capture_existing')
            record.update(status=captured['status'],snapshot_id=captured['snapshot_id'],observed_at=captured['observed_at'])
            self.recorded[slot]={'snapshot_id':captured['snapshot_id'],'observed_at':captured['observed_at']}
            log.info('Portfolio observation: slot=%s status=%s recorded=%d',slot,captured['status'],len(self.recorded))
            return captured
        except Exception as exc:
            measured('portfolio_capture_failed')
            from ..data.session import safe_error
            record.update(status='failed',error=safe_error(exc,'portfolio_sampling'))
            log.warning('Portfolio observation failed: slot=%s diagnostics=%s',slot,record['error'])
            transient=isinstance(exc,PaperBrokerError) and (exc.code in {'transport_failed','portfolio_sources_changed','sampling_stopped'} or
                (exc.code=='http_error' and exc.http_status in {408,429,500,502,503,504}))
            if not transient:raise
            return None

    def reserve(self,stage):
        at=canonical_time(self.api.now());elapsed=nanos(at)-nanos(self.started_at)
        if elapsed<0:raise StorageError('Portfolio sampler local clock precedes session')
        if stage=='interval':
            index=elapsed//300000000000
            if index<1:return None
            slot='interval-'+str(index)
        elif stage in {'start','final'}:slot=stage
        else:raise StorageError('Invalid portfolio sampler stage')
        if slot in self.attempted:return None
        meta={'format':'R26','session_id':self.session_id,'slot':slot,'interval_seconds':300};validate_sampling(meta)
        if len(self.attempts)>=32:raise StorageError('Portfolio sampling attempt bound exceeded')
        self.attempted.add(slot)
        return meta

    def summary(self):
        errors=[{'slot':r['slot'],'attempted_at':r['attempted_at'],**r['error']} for r in self.attempts if r['status']=='failed']
        ordered=sorted(self.recorded.values(),key=lambda r:(r['observed_at'],r['snapshot_id']))
        return {'format':'R26','policy':dict(POLICY),'recorded_snapshots':len(self.recorded),
            'new_captures_this_process':sum(r['status']=='captured' for r in self.attempts),
            'attempts':deepcopy(self.attempts),'errors':errors,'sampling_degraded':bool(errors),
            'snapshot_ids':[r['snapshot_id'] for r in ordered],'last_snapshot_id':ordered[-1]['snapshot_id'] if ordered else None,
            'last_observed_at':ordered[-1]['observed_at'] if ordered else None,
            'missed_slots_backfilled':False,'orders_sent_by_sampling':0,'capture_success_is_not_valuation_completeness':True}
