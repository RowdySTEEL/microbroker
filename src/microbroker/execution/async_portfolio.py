"""One bounded passive observer, separate DB/API ownership, no order methods."""
from contextlib import contextmanager
from copy import deepcopy
import threading
from .paper_api import PaperBrokerError
from .portfolio_sampling import PortfolioSampler,POLICY
from .session_timing import TimedAPI

ASYNC_POLICY={**POLICY,'method':'r28-passive-background-observations-v1',
    'execution':'single_background_thread','queue_capacity':1,'source_order_reconciliation':False,
    'shutdown_timeout_seconds':60,'coalesce_pending_intervals':True}


class AsyncPortfolioSampler:
    def __init__(self,repository_factory,api_factory,settings,account_ref,session_id,started_at,timing):
        self.factory=repository_factory;self.api_factory=api_factory;self.settings=settings
        self.ref=account_ref;self.sid=session_id;self.started=started_at;self.timing=timing
        self.condition=threading.Condition();self.commit_lock=threading.Lock();self.cancelled=False
        self.pending=None;self.closing=False;self.in_flight=None;self.failure=None;self.coalesced=0;self.shutdown_timed_out=False
        self.committing=False;self.commit_in_flight_at_timeout=False
        self.state={'format':'R28','recorded_snapshots':0,'new_captures_this_process':0,'attempts':[],
            'errors':[],'sampling_degraded':False,'snapshot_ids':[],'last_snapshot_id':None,'last_observed_at':None,
            'orders_sent_by_sampling':0,'missed_slots_backfilled':False,'capture_success_is_not_valuation_completeness':True}
        self.thread=threading.Thread(target=self.run,name='microbroker-portfolio-observer',daemon=True)
        self.thread.start()

    def check(self):
        if self.cancelled:raise PaperBrokerError('sampling_stopped')

    @contextmanager
    def commit_gate(self):
        with self.commit_lock:
            self.check();self.committing=True
        try:yield
        finally:
            with self.commit_lock:self.committing=False

    def request(self,stage='interval'):
        with self.condition:
            if self.closing:return
            if stage not in {'start','interval'}:raise ValueError('Invalid observer request')
            if self.pending is not None:
                self.coalesced+=1
                # Never replace the initial baseline by a later interval.
                if self.pending=='start':return
            self.pending=stage;self.condition.notify_all()

    def raise_if_failed(self):
        with self.condition:failure=self.failure
        if failure is not None:raise failure

    def run(self):
        repository=None;sampler=None
        try:
            repository=self.factory(self.settings.database_url)
            api=TimedAPI(self.api_factory(self.settings),self.timing,observer=True,check=self.check)
            sampler=PortfolioSampler(repository,api,self.ref,self.sid,self.started,_passive=True,_commit_gate=self.commit_gate,_timing=self.timing)
            while True:
                with self.condition:
                    self.condition.wait_for(lambda:self.pending is not None or self.closing)
                    if self.cancelled:break
                    stage=self.pending;self.pending=None
                    if stage is None:stage='final' if self.closing else None
                    if stage is None:continue
                    self.in_flight=stage
                try:
                    sampler.capture(stage)
                finally:
                    with self.condition:self.state=sampler.summary();self.in_flight=None;self.condition.notify_all()
                if stage=='final':break
        except BaseException as exc:
            from ..data.session import safe_error
            with self.condition:
                if not self.cancelled:
                    self.failure=exc;self.state['observer_error']=safe_error(exc,'portfolio_observer')
                    self.state['sampling_degraded']=True
        finally:
            try:
                if repository is not None:repository.close()
            except Exception:
                with self.condition:self.state['close_error']='observer_repository_close_failed';self.state['sampling_degraded']=True
            with self.condition:self.in_flight=None;self.condition.notify_all()

    def finish(self,timeout=60):
        with self.condition:
            self.closing=True
            if self.pending!='start':self.pending=None
            self.condition.notify_all()
        self.thread.join(timeout)
        if self.thread.is_alive():
            # Fence new writes when a blocked HTTP call returns. A database write
            # already inside the gate cannot be recalled; disclose that case.
            # The observer owns and eventually closes its own DB connection.
            with self.commit_lock:
                self.cancelled=True;self.commit_in_flight_at_timeout=self.committing
            with self.condition:self.shutdown_timed_out=True
        self.raise_if_failed()

    def summary(self):
        with self.condition:
            value=deepcopy(self.state)
            value.update(format='R28',policy=dict(ASYNC_POLICY),in_flight=self.in_flight,
                pending_request=self.pending,observer_thread_alive=self.thread.is_alive(),
                coalesced_requests=self.coalesced,orders_sent_by_sampling=0,shutdown_timed_out=self.shutdown_timed_out,
                commit_in_flight_at_timeout=self.commit_in_flight_at_timeout,
                already_started_database_write_may_finish=self.commit_in_flight_at_timeout,
                sampling_degraded=value['sampling_degraded'] or self.shutdown_timed_out)
            return value
