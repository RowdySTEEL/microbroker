"""Controlled fake Paper service. These tests never call Alpaca."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from pathlib import Path
from urllib.parse import urlparse,parse_qs
import uuid
from .paper_api import PaperAPI, Reply, TERMINAL, check_connection, PaperBrokerError
from .paper_broker import prepare_buy,prepare_exit,submit,reconcile,cancel,report,reconcile_account
from ..config import Settings, UNIVERSE
from ..data.models import DataValidationError
from ..data.storage.repository import Repository,StorageError
from ..data.stream_models import canonical_time


class FakePaper:
    def __init__(self,name='fixture',mode=None):
        self.identity=str(uuid.uuid5(uuid.NAMESPACE_URL,'microbroker-original-paper-fixture:'+name))
        self.at='2026-01-05T15:00:00.000000000Z';self.mode=mode;self.calls=[];self.orders={}
        self.cash='10000';self.equity='10000';self.last_equity='10000';self.open=True
        self.blocked=False;self.extra_positions=[];self.extra_open=[]
        self.client=PaperAPI(Settings(api_key='FAKE_KEY',api_secret='FAKE_SECRET'),send=self.send,now=lambda:self.at)

    def advance(self):
        self.at=canonical_time((datetime.fromisoformat(self.at[:26]+'+00:00')+timedelta(seconds=1)).isoformat())

    @property
    def posts(self):return sum(c[0]=='POST' for c in self.calls)

    def update(self,client_id,status,quantity='0',price='100'):
        self.advance();o=self.orders[client_id]
        o.update(status=status,filled_qty=quantity,filled_avg_price=price if Decimal(quantity)>0 else None,updated_at=self.at)

    def response(self,value,status=200):return Reply(status,json.dumps(value).encode())

    def send(self,method,url,headers,body,timeout):
        self.calls.append((method,url));path=urlparse(url).path;query=parse_qs(urlparse(url).query)
        if method=='GET' and path=='/v2/account':
            return self.response({'id':self.identity,'account_number':'DO_NOT_RETAIN_ACCOUNT_NUMBER','currency':'USD','status':'ACTIVE',
                'trading_blocked':self.blocked,'account_blocked':False,'trade_suspended_by_user':False,
                'cash':self.cash,'equity':self.equity,'last_equity':self.last_equity,'buying_power':'40000'})
        if method=='GET' and path=='/v2/clock':
            return self.response({'timestamp':self.at,'is_open':self.open,
                                 'next_open':'2026-01-06T14:30:00Z','next_close':'2026-01-05T21:00:00Z'})
        if method=='GET' and path.startswith('/v2/assets/'):
            return self.response({'symbol':path.split('/')[-1],'class':'us_equity','status':'active','tradable':True,'fractionable':True})
        if method=='GET' and path=='/v2/positions':
            totals={}
            for o in self.orders.values():
                totals.setdefault(o['symbol'],Decimal(0));totals[o['symbol']]+=Decimal(o['filled_qty'])*(1 if o['side']=='buy' else -1)
            return self.response([{'symbol':s,'qty':str(q),'side':'long' if q>0 else 'short'} for s,q in totals.items() if q]+self.extra_positions)
        if method=='GET' and path=='/v2/orders':
            return self.response([deepcopy(o) for o in self.orders.values() if o['status'] not in TERMINAL]+self.extra_open)
        if method=='GET' and path=='/v2/orders:by_client_order_id':
            o=self.orders.get(query['client_order_id'][0]);return self.response(o,200 if o else 404)
        if method=='POST' and path=='/v2/orders':
            if self.mode=='http_422':return self.response({},422)
            data=json.loads(body);cid=data['client_order_id']
            if cid in self.orders:return self.response({},422)
            o={**data,'id':str(uuid.uuid5(uuid.NAMESPACE_URL,cid)),'status':'new','filled_qty':'0','filled_avg_price':None,
               'qty':data.get('qty'),'notional':data.get('notional'),'updated_at':self.at,'asset_class':'us_equity','order_class':'simple'}
            if self.mode!='timeout_before_accept':self.orders[cid]=o
            if self.mode in {'timeout_before_accept','timeout_after_accept'}:raise TimeoutError('FAKE_SECRET must never leak')
            return self.response(o)
        if method=='DELETE' and path.startswith('/v2/orders/'):
            o=next(o for o in self.orders.values() if o['id']==path.split('/')[-1])
            if self.mode=='cancel_race_fill':self.update(o['client_order_id'],'filled','0.1')
            elif self.mode!='cancel_ack_only':self.update(o['client_order_id'],'canceled',o['filled_qty'])
            return Reply(204,b'')
        raise AssertionError('Unexpected fake Paper route')


def broker_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Paper broker verification failed: '+name)
        checks.append(name)
    def rejected(call):
        try:call()
        except (PaperBrokerError,StorageError,DataValidationError):return True
        return False
    f=FakePaper('primary',mode='timeout_after_accept');api=f.client
    connection=check_connection(api)
    require(connection['orders_sent']==0 and all('paper-api.alpaca.markets' in c[1] for c in f.calls),'broker_readonly_fixed_paper_connection')
    p=prepare_buy(repository,api,'broker-main','AAPL')
    again=prepare_buy(repository,api,'broker-main','AAPL')
    require(again['status']=='existing' and p['request']==again['request'] and f.posts==0,'broker_prepare_stable_no_post')
    require(rejected(lambda:prepare_buy(repository,api,'broker-main','AAPL','11')),'broker_conflicting_intent_rejected')
    first=submit(repository,api,'broker-main')
    require(first['phase']=='uncertain' and f.posts==1 and first['post_attempts']==1,'broker_timeout_preserves_uncertainty_and_claim')
    found=submit(repository,api,'broker-main')
    require(found['order']['status']=='new' and f.posts==1,'broker_retry_queries_client_id_no_second_post')
    cid=p['request']['client_order_id'];f.update(cid,'partially_filled','0.04')
    partial=reconcile(repository,api,'broker-main')
    require(partial['order']['filled_qty']=='0.04','broker_partial_fill_recorded_cumulatively')
    same=reconcile(repository,api,'broker-main')
    require(same['audit_sha256']==partial['audit_sha256'],'broker_repeat_observation_no_double_fill_event')
    f.update(cid,'filled','0.1');full=reconcile(repository,api,'broker-main')
    require(full['order']['filled_qty']=='0.1' and full['order']['filled_avg_price']=='100','broker_final_fill_replaces_cumulative_quantity')
    exit_plan=prepare_exit(repository,api,'broker-exit','broker-main')
    require(exit_plan['request']['qty']=='0.1','broker_exit_uses_only_managed_filled_quantity')
    require(rejected(lambda:prepare_exit(repository,api,'broker-exit-duplicate','broker-main')),'broker_exit_reservation_prevents_oversell')
    f.mode=None;sent=submit(repository,api,'broker-exit');f.update(sent['request']['client_order_id'],'filled','0.1')
    reconcile(repository,api,'broker-exit')
    positions=reconcile_account(repository,api)
    require(positions['status']=='matched' and positions['positions']=={},'broker_managed_position_roundtrip_matches')
    positions_snapshot=report(repository,'broker-main')
    require('DO_NOT_RETAIN' not in json.dumps(positions_snapshot) and not positions_snapshot['pnl_calculated'],
            'broker_account_number_and_simulated_pnl_not_retained')
    f.extra_positions=[{'symbol':'MSFT','qty':'1','side':'long'}]
    require(reconcile_account(repository,api)['status']=='attention_required','broker_unmanaged_position_difference_disclosed')
    g=FakePaper('missing','timeout_before_accept');prepare_buy(repository,g.client,'broker-missing','MSFT')
    submit(repository,g.client,'broker-missing');missing=submit(repository,g.client,'broker-missing')
    require(missing['phase']=='uncertain' and g.posts==1,'broker_404_after_uncertain_send_never_resubmitted')
    h=FakePaper('rejected','http_422');prepare_buy(repository,h.client,'broker-http','MSFT')
    http=submit(repository,h.client,'broker-http')
    require(http['phase']=='uncertain' and http['order'] is None and h.posts==1,'broker_post_rejection_not_claimed_fill')
    c=FakePaper('cancel','cancel_ack_only');q=prepare_buy(repository,c.client,'broker-cancel','AAPL');submit(repository,c.client,'broker-cancel')
    ack=cancel(repository,c.client,'broker-cancel')
    require(ack['cancel_requested'] and ack['order']['status']=='new','broker_cancel_ack_is_not_terminal_cancel')
    c.mode='cancel_race_fill';race=cancel(repository,c.client,'broker-cancel')
    require(race['order']['status']=='filled' and race['order']['filled_qty']=='0.1','broker_cancel_fill_race_preserves_actual_fill')
    closed=FakePaper('closed');closed.open=False;prepare_buy(repository,closed.client,'broker-closed','AAPL')
    require(rejected(lambda:submit(repository,closed.client,'broker-closed')) and closed.posts==0,'broker_closed_market_not_queued')
    poor=FakePaper('cash');poor.cash='5';prepare_buy(repository,poor.client,'broker-cash','AAPL')
    require(rejected(lambda:submit(repository,poor.client,'broker-cash')) and poor.posts==0,'broker_cash_cap_no_margin_fallback')
    loss=FakePaper('loss');loss.equity='9700';prepare_buy(repository,loss.client,'broker-loss','AAPL')
    rejected(lambda:submit(repository,loss.client,'broker-loss'));loss.equity='10100'
    require(rejected(lambda:submit(repository,loss.client,'broker-loss')) and loss.posts==0,'broker_daily_loss_latched_after_recovery')
    cap=FakePaper('cap')
    for index,symbol in enumerate(UNIVERSE[:10]):
        key='broker-cap-'+str(index);p=prepare_buy(repository,cap.client,key,symbol);submit(repository,cap.client,key)
        cap.update(p['request']['client_order_id'],'filled','0.1');reconcile(repository,cap.client,key)
    prepare_buy(repository,cap.client,'broker-cap-over',UNIVERSE[10])
    require(rejected(lambda:submit(repository,cap.client,'broker-cap-over')) and cap.posts==10,'broker_daily_buy_attempt_cap_persists_across_intents')
    changed=FakePaper('changed');prepare_buy(repository,changed.client,'broker-changed','AAPL');changed.identity=str(uuid.uuid4())
    require(rejected(lambda:submit(repository,changed.client,'broker-changed')) and changed.posts==0,'broker_account_identity_change_rejected')
    # Post-response corruption must retain the durable claim, not prompt a retry.
    bad=FakePaper('mismatch');plan=prepare_buy(repository,bad.client,'broker-mismatch','AAPL');submit(repository,bad.client,'broker-mismatch')
    bad.orders[plan['request']['client_order_id']]['symbol']='MSFT'
    mismatch=reconcile(repository,bad.client,'broker-mismatch')
    require(mismatch['phase']=='uncertain' and bad.posts==1,'broker_mismatched_response_quarantined')
    f2=FakePaper('regress');plan=prepare_buy(repository,f2.client,'broker-regress','AAPL');submit(repository,f2.client,'broker-regress')
    f2.update(plan['request']['client_order_id'],'partially_filled','0.04');reconcile(repository,f2.client,'broker-regress')
    f2.update(plan['request']['client_order_id'],'partially_filled','0.02')
    regression=reconcile(repository,f2.client,'broker-regress')
    require(regression['phase']=='uncertain' and regression['order']['filled_qty']=='0.04','broker_regressive_cumulative_fill_rejected')
    m=repository.marker;raw=repository.connection.execute(f'SELECT intent_json FROM broker_paper_intents WHERE id={m}',('broker-main',)).fetchone()[0]
    repository.connection.commit();repository.connection.execute(f'UPDATE broker_paper_intents SET intent_json={m} WHERE id={m}',('{}','broker-main'));repository.connection.commit()
    try:require(rejected(lambda:report(repository,'broker-main')),'broker_intent_tamper_rejected')
    finally:
        repository.connection.execute(f'UPDATE broker_paper_intents SET intent_json={m} WHERE id={m}',(raw,'broker-main'));repository.connection.commit()
    # Failed claim insert must roll back before network mutation; failed post observation must not erase a claim.
    atomic=FakePaper('atomic');prepare_buy(repository,atomic.client,'broker-atomic','AAPL')
    base=repository.connection
    class FailInsert:
        def __init__(self,kind):self.kind=kind
        def __getattr__(self,name):return getattr(base,name)
        def execute(self,query,params=()):
            result=base.execute(query,params)
            if query.startswith('INSERT INTO broker_paper_events') and self.kind in params[3]:
                raise StorageError('Controlled paper event insert failure')
            return result
    repository.connection=FailInsert('post_claimed')
    try:failed=rejected(lambda:submit(repository,atomic.client,'broker-atomic'))
    finally:repository.connection=base
    require(failed and atomic.posts==0 and report(repository,'broker-atomic')['post_attempts']==0,
            'broker_claim_write_rollback_before_any_post')
    repository.connection=FailInsert('broker_observed')
    try:failed=rejected(lambda:submit(repository,atomic.client,'broker-atomic'))
    finally:repository.connection=base
    prior=report(repository,'broker-atomic')
    recovered=submit(repository,atomic.client,'broker-atomic')
    require(failed and prior['phase']=='submitting' and recovered['phase']=='observed' and atomic.posts==1,
            'broker_observation_write_failure_preserves_no_repost_claim')
    require(all('FAKE_SECRET' not in json.dumps(report(repository,key)) for key in ('broker-main','broker-missing','broker-http')),
            'broker_diagnostics_do_not_echo_transport_secrets')
    return checks


def verify_paper_broker():
    repository=Repository('sqlite:///:memory:')
    try:return {'status':'passed','checks':broker_checks(repository),'transport':'original_controlled_fake_paper_service',
                'network_required':False,'model_required':False,'user_database_modified':False,
                'actual_alpaca_connection_tested':False,'orders_sent_to_alpaca':0,'profitability_validated':False}
    finally:repository.close()


def demo_paper_broker(output):
    folder=Path(output)
    if folder.exists():raise DataValidationError('Demo output already exists; choose a new folder')
    folder.mkdir(parents=True);repository=Repository('sqlite:///'+str((folder/'scenario.sqlite3').resolve()))
    try:
        result={'status':'completed','checks':broker_checks(repository),'output':str(folder),
                'scope':'original_fake_broker_scenarios_not_live_execution','actual_alpaca_connection_tested':False,
                'user_database_modified':False,'orders_sent_to_alpaca':0}
        (folder/'summary.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
        return result
    finally:repository.close()
