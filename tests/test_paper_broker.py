from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
import contextlib
from decimal import Decimal
import io
import json
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from unittest.mock import MagicMock
import tempfile
import threading
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.config import Settings,ConfigurationError
from microbroker.execution.paper_api import PaperAPI,PaperBrokerError,Reply,BASE,MAX_BYTES,check_connection,order
from microbroker.execution.paper_broker import (prepare_buy,prepare_exit,submit,reconcile,cancel,report,
    reconcile_account,intent_payload,read_intent,append,locked)
from microbroker.execution.paper_broker_verification import FakePaper,verify_paper_broker,demo_paper_broker
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.data.models import DataValidationError
from microbroker.intelligence.policy import digest


class BrokerTests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:');self.fake=FakePaper();self.api=self.fake.client
    def tearDown(self):self.repo.close()
    def plan(self,key='entry',symbol='AAPL',notional='10'):return prepare_buy(self.repo,self.api,key,symbol,notional)
    def filled(self,key='entry'):
        p=self.plan(key);submit(self.repo,self.api,key)
        self.fake.update(p['request']['client_order_id'],'filled','0.1')
        return reconcile(self.repo,self.api,key)

    def test_private_verifier_all_controlled_checks(self):
        result=verify_paper_broker();self.assertEqual(len(result['checks']),28)
        self.assertFalse(result['actual_alpaca_connection_tested']);self.assertEqual(result['orders_sent_to_alpaca'],0)
    def test_readonly_check_never_posts_or_retains_account_number(self):
        result=check_connection(self.api)
        self.assertEqual(self.fake.posts,0);self.assertEqual(result['environment'],'alpaca_paper')
        self.assertNotIn('DO_NOT_RETAIN',json.dumps(result))
    def test_prepare_normalizes_notional_and_is_idempotent(self):
        a=self.plan(notional='10.00');self.fake.advance();b=self.plan(notional='10')
        self.assertEqual(a['request'],b['request']);self.assertEqual(b['status'],'existing');self.assertEqual(self.fake.posts,0)
    def test_prepare_conflicting_order_rejected(self):
        self.plan()
        for symbol,amount in [('MSFT','10'),('AAPL','11')]:
            with self.assertRaises(DataValidationError):self.plan(symbol=symbol,notional=amount)
    def test_stable_client_id_is_account_bound(self):
        first=self.plan()['request']['client_order_id'];other=FakePaper('other')
        second=prepare_buy(self.repo,other.client,'other-entry','AAPL')['request']['client_order_id']
        self.assertNotEqual(first,second);self.assertLessEqual(len(first),48)
    def test_post_claim_is_committed_before_network(self):
        self.plan();send=self.api.send
        def inspect(method,*args):
            if method=='POST':
                self.assertFalse(self.repo.connection.in_transaction)
                state=report(self.repo,'entry');self.assertEqual(state['phase'],'submitting');self.assertEqual(state['post_attempts'],1)
            return send(method,*args)
        self.api.send=inspect
        submit(self.repo,self.api,'entry')
    def test_retry_existing_order_only_looks_up(self):
        self.plan();submit(self.repo,self.api,'entry');again=submit(self.repo,self.api,'entry')
        self.assertEqual(again['post_attempts'],1);self.assertEqual(self.fake.posts,1)
    def test_timeout_after_accept_reconciles_without_resend(self):
        self.fake.mode='timeout_after_accept';self.plan();result=submit(self.repo,self.api,'entry')
        self.assertEqual(result['phase'],'uncertain')
        self.assertEqual(submit(self.repo,self.api,'entry')['phase'],'observed');self.assertEqual(self.fake.posts,1)
    def test_timeout_before_accept_404_does_not_resend(self):
        self.fake.mode='timeout_before_accept';self.plan();submit(self.repo,self.api,'entry')
        self.assertEqual(submit(self.repo,self.api,'entry')['last_error']['http_status'],404)
        self.assertEqual(self.fake.posts,1)
    def test_http_post_failure_is_uncertain_and_not_filled(self):
        self.fake.mode='http_422';self.plan();result=submit(self.repo,self.api,'entry')
        self.assertEqual(result['phase'],'uncertain');self.assertIsNone(result['order'])
    def test_partial_then_final_not_added_twice(self):
        p=self.plan();submit(self.repo,self.api,'entry');cid=p['request']['client_order_id']
        self.fake.update(cid,'partially_filled','0.04');reconcile(self.repo,self.api,'entry')
        self.fake.update(cid,'filled','0.1');result=reconcile(self.repo,self.api,'entry')
        self.assertEqual(result['order']['filled_qty'],'0.1')
        same=reconcile(self.repo,self.api,'entry');self.assertEqual(result['audit_sha256'],same['audit_sha256'])
    def test_partial_fill_cannot_exit_until_entry_terminal(self):
        p=self.plan();submit(self.repo,self.api,'entry');self.fake.update(p['request']['client_order_id'],'partially_filled','0.04')
        reconcile(self.repo,self.api,'entry')
        with self.assertRaises(DataValidationError):prepare_exit(self.repo,self.api,'exit','entry')
    def test_canceled_partial_entry_can_exit_exact_filled_shares(self):
        p=self.plan();submit(self.repo,self.api,'entry');self.fake.update(p['request']['client_order_id'],'canceled','0.04')
        reconcile(self.repo,self.api,'entry');exit=prepare_exit(self.repo,self.api,'exit','entry')
        self.assertEqual(exit['request']['qty'],'0.04')
    def test_exit_reservation_and_prepare_retry(self):
        self.filled();first=prepare_exit(self.repo,self.api,'exit','entry');again=prepare_exit(self.repo,self.api,'exit','entry')
        self.assertEqual(first['request'],again['request']);self.assertEqual(again['status'],'existing')
        with self.assertRaises(DataValidationError):prepare_exit(self.repo,self.api,'exit-other','entry')
    def test_canceled_partial_exit_releases_only_unsold_reservation(self):
        self.filled();p=prepare_exit(self.repo,self.api,'exit','entry');submit(self.repo,self.api,'exit')
        self.fake.update(p['request']['client_order_id'],'canceled','0.04');reconcile(self.repo,self.api,'exit')
        other=prepare_exit(self.repo,self.api,'exit-rest','entry');self.assertEqual(other['request']['qty'],'0.06')
    def test_no_exit_after_full_exit(self):
        self.filled();p=prepare_exit(self.repo,self.api,'exit','entry');submit(self.repo,self.api,'exit')
        self.fake.update(p['request']['client_order_id'],'filled','0.1');reconcile(self.repo,self.api,'exit')
        with self.assertRaises(DataValidationError):prepare_exit(self.repo,self.api,'extra','entry')
    def test_sell_requires_broker_position_even_when_local_entry_filled(self):
        self.filled();prepare_exit(self.repo,self.api,'exit','entry');self.fake.orders={}
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'exit')
        self.assertEqual(self.fake.posts,1)
    def test_cancel_ack_only_does_not_invent_terminal_cancel(self):
        self.fake.mode='cancel_ack_only';self.plan();submit(self.repo,self.api,'entry');result=cancel(self.repo,self.api,'entry')
        self.assertTrue(result['cancel_requested']);self.assertTrue(result['cancel_acknowledged']);self.assertEqual(result['order']['status'],'new')
    def test_cancel_fill_race_retains_filled_qty(self):
        self.fake.mode='cancel_race_fill';self.plan();submit(self.repo,self.api,'entry');result=cancel(self.repo,self.api,'entry')
        self.assertEqual(result['order']['status'],'filled');self.assertEqual(result['order']['filled_qty'],'0.1')
    def test_cancel_before_any_send_does_not_delete_unknown_order(self):
        self.plan();result=cancel(self.repo,self.api,'entry')
        self.assertEqual(result['phase'],'prepared');self.assertFalse(any(m=='DELETE' for m,u in self.fake.calls))
    def test_cancel_attempt_persisted_before_delete(self):
        self.plan();submit(self.repo,self.api,'entry');send=self.api.send
        def check(method,*args):
            if method=='DELETE':self.assertTrue(report(self.repo,'entry')['cancel_requested'])
            return send(method,*args)
        self.api.send=check;cancel(self.repo,self.api,'entry')
    def test_closed_market_leaves_intent_prepared(self):
        self.plan();self.fake.open=False
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'entry')
        self.assertEqual(report(self.repo,'entry')['post_attempts'],0);self.assertEqual(self.fake.posts,0)
    def test_account_blocked_prevents_post(self):
        self.plan();self.fake.blocked=True
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'entry')
        self.assertEqual(self.fake.posts,0)
    def test_cash_buffer_not_buying_power_controls_buy(self):
        self.plan();self.fake.cash='10.09'
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'entry')
        self.assertEqual(self.fake.posts,0)
    def test_any_unresolved_order_blocks_another_send(self):
        self.fake.mode='timeout_before_accept';self.plan();submit(self.repo,self.api,'entry')
        self.fake.mode=None;self.plan('second','MSFT')
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'second')
        self.assertEqual(self.fake.posts,1)
    def test_managed_holding_blocks_pyramiding_when_position_api_lags(self):
        self.filled();self.plan('second');send=self.api.send
        def lag(method,url,*args):
            if url.endswith('/positions'):return Reply(200,b'[]')
            return send(method,url,*args)
        self.api.send=lag
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'second')
        self.assertEqual(self.fake.posts,1)
    def test_open_broker_order_blocks_symbol(self):
        self.plan();self.fake.extra_open=[{'symbol':'AAPL'}]
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'entry')
        self.assertEqual(self.fake.posts,0)
    def test_daily_loss_latches_for_other_intents_same_account(self):
        self.plan();self.fake.equity='9800'
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'entry')
        self.fake.equity='11000';self.plan('second','MSFT')
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'second')
        self.assertEqual(self.fake.posts,0)
    def test_daily_loss_does_not_block_managed_exit(self):
        self.filled();prepare_exit(self.repo,self.api,'exit','entry');self.fake.equity='9700'
        result=submit(self.repo,self.api,'exit');self.assertEqual(result['phase'],'observed');self.assertEqual(self.fake.posts,2)
    def test_wrong_account_on_submit_does_not_lookup_or_post(self):
        self.plan();self.fake.identity='11111111-2222-3333-4444-555555555555'
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'entry')
        self.assertEqual(self.fake.posts,0)
    def test_cumulative_regression_is_uncertain_and_keeps_last_good_qty(self):
        p=self.plan();submit(self.repo,self.api,'entry');cid=p['request']['client_order_id']
        self.fake.update(cid,'partially_filled','0.04');reconcile(self.repo,self.api,'entry')
        self.fake.update(cid,'partially_filled','0.02');result=reconcile(self.repo,self.api,'entry')
        self.assertEqual(result['phase'],'uncertain');self.assertEqual(result['order']['filled_qty'],'0.04')
    def test_older_provider_observation_does_not_downgrade(self):
        p=self.plan();submit(self.repo,self.api,'entry');old=deepcopy(self.fake.orders[p['request']['client_order_id']])
        self.fake.update(p['request']['client_order_id'],'filled','0.1');good=reconcile(self.repo,self.api,'entry')
        self.fake.orders[p['request']['client_order_id']]=old;again=reconcile(self.repo,self.api,'entry')
        self.assertEqual(again['order'],good['order']);self.assertEqual(again['audit_sha256'],good['audit_sha256'])
    def test_order_identity_and_amount_mismatch_not_applied(self):
        for field,value in [('symbol','MSFT'),('side','sell'),('notional','11'),('type','limit'),('extended_hours',True)]:
            with self.subTest(field=field):
                f=FakePaper('mismatch-'+field);p=prepare_buy(self.repo,f.client,'key-'+field,'AAPL');submit(self.repo,f.client,'key-'+field)
                f.orders[p['request']['client_order_id']][field]=value
                self.assertEqual(reconcile(self.repo,f.client,'key-'+field)['phase'],'uncertain')
    def test_overfill_and_unknown_status_are_uncertain(self):
        p=self.plan();submit(self.repo,self.api,'entry');o=self.fake.orders[p['request']['client_order_id']]
        o.update(qty='0.1',filled_qty='0.2',filled_avg_price='100')
        self.assertEqual(reconcile(self.repo,self.api,'entry')['phase'],'uncertain')
    def test_claim_insert_failure_rolls_back_without_post(self):
        self.plan();base=self.repo.connection
        class Fail:
            def __getattr__(self,n):return getattr(base,n)
            def execute(self,q,p=()):
                v=base.execute(q,p)
                if q.startswith('INSERT INTO broker_paper_events'):raise StorageError('controlled insertion failure')
                return v
        self.repo.connection=Fail()
        try:
            with self.assertRaises(StorageError):submit(self.repo,self.api,'entry')
        finally:self.repo.connection=base
        self.assertEqual(self.fake.posts,0);self.assertEqual(report(self.repo,'entry')['phase'],'prepared')
    def test_post_observation_storage_failure_never_reposts(self):
        self.plan();base=self.repo.connection
        class Fail:
            def __getattr__(self,n):return getattr(base,n)
            def execute(self,q,p=()):
                v=base.execute(q,p)
                if q.startswith('INSERT INTO broker_paper_events') and 'broker_observed' in p[3]:raise StorageError('controlled observation failure')
                return v
        self.repo.connection=Fail()
        try:
            with self.assertRaises(StorageError):submit(self.repo,self.api,'entry')
        finally:self.repo.connection=base
        self.assertEqual(report(self.repo,'entry')['phase'],'submitting')
        self.assertEqual(submit(self.repo,self.api,'entry')['phase'],'observed');self.assertEqual(self.fake.posts,1)
    def test_event_tampering_rejected(self):
        self.plan();submit(self.repo,self.api,'entry')
        self.repo.connection.execute("UPDATE broker_paper_events SET event_json='{}' WHERE ordinal=0");self.repo.connection.commit()
        with self.assertRaises(StorageError):report(self.repo,'entry')
    def test_intent_tampering_blocks_submission(self):
        self.plan();self.repo.connection.execute("UPDATE broker_paper_intents SET intent_json='{}'");self.repo.connection.commit()
        with self.assertRaises(StorageError):submit(self.repo,self.api,'entry')
        self.assertEqual(self.fake.posts,0)
    def test_local_report_no_network_credentials_required(self):
        self.plan();self.api.send=lambda *args: (_ for _ in ()).throw(AssertionError())
        self.assertEqual(report(self.repo,'entry')['phase'],'prepared')
    def test_account_reconciliation_tracks_partial_fills_without_pnl_claim(self):
        p=self.plan();submit(self.repo,self.api,'entry');self.fake.update(p['request']['client_order_id'],'partially_filled','0.04')
        result=reconcile_account(self.repo,self.api)
        self.assertEqual(result['status'],'matched');self.assertEqual(result['positions']['AAPL']['qty'],'0.04')
        self.assertFalse(result['pnl_calculated']);self.assertFalse(result['atomic_broker_snapshot'])
    def test_account_reconciliation_discloses_external_positions(self):
        self.fake.extra_positions=[{'symbol':'MSFT','qty':'1','side':'long'}]
        self.assertEqual(reconcile_account(self.repo,self.api)['status'],'attention_required')
    def test_account_reconciliation_detects_unmanaged_open_order_without_position(self):
        self.fake.extra_open=[{'symbol':'MSFT','client_order_id':'external-strategy'}]
        result=reconcile_account(self.repo,self.api)
        self.assertEqual(result['status'],'attention_required')
        self.assertEqual(result['unmatched_open_broker_orders'],1)

    def test_account_reconciliation_discovers_order_after_local_claim_loss(self):
        p=self.plan();self.api.submit(p['request'])
        result=reconcile_account(self.repo,self.api)
        self.assertEqual(result['status'],'matched')
        self.assertEqual(report(self.repo,'entry')['phase'],'observed')
        self.assertEqual(self.fake.posts,1)

    def test_live_or_arbitrary_urls_are_rejected_before_transport(self):
        for route in ['/v2/orders/../../account','https://api.alpaca.markets/v2/orders','/v2/assets/BOGUS']:
            with self.assertRaises(PaperBrokerError):self.api.call('GET',route)
        self.assertEqual(self.fake.calls,[])
    def test_broker_auth_mode_not_supported(self):
        with self.assertRaises(ConfigurationError):PaperAPI(Settings(api_key='x',api_secret='x',auth_mode='broker'))
    def test_non_ascii_credentials_rejected_before_transport(self):
        with self.assertRaises(ConfigurationError):PaperAPI(Settings(api_key='x',api_secret='А'))
    def test_transport_errors_do_not_echo_secrets(self):
        self.api.send=lambda *a: (_ for _ in ()).throw(OSError('DO_NOT_LEAK_SECRET'))
        with self.assertRaises(PaperBrokerError) as exc:self.api.account()
        self.assertNotIn('DO_NOT_LEAK',str(exc.exception))
    def test_real_http_transport_does_not_follow_post_redirect(self):
        from microbroker.execution.paper_api import transport
        received=[]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                received.append(('POST',self.path))
                self.rfile.read(int(self.headers.get('Content-Length','0')))
                self.send_response(302);self.send_header('Location','/unexpected-target');self.end_headers()
            def do_GET(self):
                received.append(('GET',self.path));self.send_response(200);self.end_headers()
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            answer=transport('POST','http://127.0.0.1:'+str(server.server_port)+'/orders',{'APCA-API-KEY-ID':'FAKE_KEY'},b'{}',2)
            self.assertEqual(answer.status,302)
            self.assertEqual(received,[('POST','/orders')])
        finally:server.shutdown();server.server_close();thread.join(timeout=2)

    def test_http_transport_bounds_read_and_sends_exact_body_once(self):
        from microbroker.execution.paper_api import transport
        response=MagicMock();response.status=200;response.read.return_value=b'{}'
        opener=MagicMock();opener.open.return_value.__enter__.return_value=response
        with patch('microbroker.execution.paper_api.build_opener',return_value=opener):
            result=transport('POST',BASE+'/v2/orders',{'APCA-API-KEY-ID':'FAKE_KEY'},b'{"notional":"10"}',2)
        request=opener.open.call_args.args[0]
        self.assertEqual(request.get_method(),'POST');self.assertEqual(request.data,b'{"notional":"10"}')
        self.assertEqual(opener.open.call_count,1);response.read.assert_called_once_with(MAX_BYTES+1)
        self.assertEqual(result.body,b'{}')

    def test_response_bound_and_json_validation(self):
        for body in [b'x'*(MAX_BYTES+1),b'not-json',b'{"x":NaN}',b'\xff']:
            self.api.send=lambda *a: Reply(200,body)
            with self.assertRaises(PaperBrokerError):self.api.account()
    def test_missing_critical_account_flags_fail_closed(self):
        self.api.send=lambda *a: Reply(200,b'{"id":"11111111-2222-3333-4444-555555555555"}')
        with self.assertRaises(PaperBrokerError):self.api.account()
    def test_500_open_orders_fails_instead_of_assuming_complete(self):
        self.fake.extra_open=[{'symbol':'MSFT'}]*500
        with self.assertRaises(PaperBrokerError):self.api.open_orders()
    def test_invalid_buy_amount_symbol_and_id(self):
        for k,s,a in [('bad id','AAPL','10'),('entry','FAKE','10'),('entry','AAPL','0.5'),('entry','AAPL','101'),('entry','AAPL','10.001')]:
            with self.subTest(k=k,s=s,a=a),self.assertRaises(DataValidationError):self.plan(k,s,a)
    def test_stale_market_clock_prevents_post(self):
        self.plan();self.api.now=lambda:'2026-01-05T16:00:00.000000000Z'
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'entry')
        self.assertEqual(self.fake.posts,0)
    def test_durable_restart_reconciles_same_claim(self):
        with tempfile.TemporaryDirectory() as folder:
            url='sqlite:///'+str(Path(folder)/'paper.db');r=Repository(url)
            self.fake.mode='timeout_after_accept';prepare_buy(r,self.api,'persist','AAPL');submit(r,self.api,'persist');r.close()
            reopened=Repository(url)
            try:
                self.assertEqual(submit(reopened,self.api,'persist')['phase'],'observed');self.assertEqual(self.fake.posts,1)
            finally:reopened.close()
    def test_concurrent_submit_claims_one_post(self):
        with tempfile.TemporaryDirectory() as folder:
            url='sqlite:///'+str(Path(folder)/'paper.db');r=Repository(url);prepare_buy(r,self.api,'parallel','AAPL');r.close()
            barrier=threading.Barrier(2);send=self.api.send
            def synchronized(method,url,*args):
                if method=='GET' and url.endswith('/v2/clock'):barrier.wait(timeout=5)
                return send(method,url,*args)
            self.api.send=synchronized
            def call():
                local=Repository(url)
                try:return submit(local,self.api,'parallel')
                finally:local.close()
            with ThreadPoolExecutor(max_workers=2) as pool:
                results=list(pool.map(lambda _:call(),range(2)))
            self.assertEqual(self.fake.posts,1)
            final=Repository(url)
            try:self.assertEqual(report(final,'parallel')['post_attempts'],1)
            finally:final.close()
    def test_lookup_auth_failure_before_send_does_not_claim_or_post(self):
        self.plan();send=self.api.send
        def fail(method,url,*args):
            if 'by_client_order_id' in url:return Reply(401,b'FAKE_SECRET')
            return send(method,url,*args)
        self.api.send=fail
        with self.assertRaises(PaperBrokerError) as exc:submit(self.repo,self.api,'entry')
        self.assertNotIn('FAKE_SECRET',str(exc.exception))
        self.assertEqual(report(self.repo,'entry')['post_attempts'],0)

    def test_unknown_provider_status_is_quarantined(self):
        p=self.plan();submit(self.repo,self.api,'entry')
        self.fake.orders[p['request']['client_order_id']]['status']='future_unknown_status'
        self.assertEqual(reconcile(self.repo,self.api,'entry')['phase'],'uncertain')

    def test_redirect_reply_is_not_accepted_or_retried(self):
        self.plan();send=self.api.send
        def redirect(method,url,*args):
            if method=='POST':return Reply(302,b'')
            return send(method,url,*args)
        self.api.send=redirect
        result=submit(self.repo,self.api,'entry')
        self.assertEqual(result['phase'],'uncertain');self.assertEqual(result['last_error']['http_status'],302)
        self.assertTrue(all(url.startswith(BASE) for method,url in self.fake.calls))

    def test_reserved_exit_blocks_changed_entry_quantity(self):
        self.filled();prepare_exit(self.repo,self.api,'exit','entry')
        # A new provider fill regression is unsupported; exit must be withheld until reconciled.
        cid=report(self.repo,'entry')['request']['client_order_id']
        self.fake.update(cid,'filled','0.05');reconcile(self.repo,self.api,'entry')
        with self.assertRaises(PaperBrokerError):submit(self.repo,self.api,'exit')
        self.assertEqual(self.fake.posts,1)

    def test_simulation_tables_untouched_by_paper_adapter(self):
        self.filled()
        for table in ('paper_accounts','paper_ledger','strategy_simulations'):
            self.assertEqual(self.repo.connection.execute('SELECT COUNT(*) FROM '+table).fetchone()[0],0)
        self.repo.connection.commit()

    def test_demo_private_no_user_environment_or_api(self):
        with tempfile.TemporaryDirectory() as folder,patch('microbroker.config.Settings.from_env',side_effect=AssertionError()):
            result=demo_paper_broker(str(Path(folder)/'demo'))
            self.assertEqual(result['orders_sent_to_alpaca'],0)
            with self.assertRaises(DataValidationError):demo_paper_broker(str(Path(folder)/'demo'))
    def test_cli_verifier_runs_without_env(self):
        with patch('microbroker.config.Settings.from_env',side_effect=AssertionError()),contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(['verify-paper-broker']),0)
        self.assertEqual(json.loads(output.getvalue())['status'],'passed')
    def test_cli_check_uses_no_database(self):
        with patch('microbroker.cli.PaperAPI',return_value=self.api),patch('microbroker.cli.Repository',side_effect=AssertionError()),contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(['paper-broker-check']),0)
        self.assertEqual(json.loads(output.getvalue())['orders_sent'],0)


if __name__=='__main__':unittest.main()
