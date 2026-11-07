"""Controlled original cumulative-fill and broker-mark scenarios; never contacts Alpaca."""
from decimal import Decimal
import json
from urllib.parse import urlparse
from .paper_api import valued_positions,PaperBrokerError
from .paper_broker_verification import FakePaper
from . import paper_broker as broker
from .paper_portfolio import capture_portfolio,portfolio_report,calculate
from ..data.storage.repository import Repository,StorageError
from ..data.models import DataValidationError


class ValuedFakePaper(FakePaper):
    def __init__(self,name='r25-fixture',mode=None):
        super().__init__(name,mode);self.mark='110';self.missing_mark=False
    def send(self,method,url,headers,body,timeout):
        reply=super().send(method,url,headers,body,timeout)
        if method=='GET' and urlparse(url).path=='/v2/positions':
            rows=json.loads(reply.body)
            for p in rows:
                qty=Decimal(p['qty']);price=Decimal(self.mark)
                p.update(current_price=self.mark,avg_entry_price='100',cost_basis=str(qty*100),market_value=str(qty*price),unrealized_pl=str(qty*(price-100)))
                if self.missing_mark:p.pop('current_price')
            return self.response(rows)
        return reply


def seeded(repository,name,*,quantity='0.1',price='100',status='filled'):
    f=ValuedFakePaper(name);entry=name+'-buy';p=broker.prepare_buy(repository,f.client,entry,'AAPL','10')
    broker.submit(repository,f.client,entry);f.update(p['request']['client_order_id'],status,quantity,price)
    broker.reconcile(repository,f.client,entry)
    return f,entry


def portfolio_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Portfolio verification failed: '+name)
        checks.append(name)
    def reject(call):
        try:call()
        except (StorageError,DataValidationError,PaperBrokerError):return True
        return False
    f,entry=seeded(repository,'r25-main');api=f.client
    start_call=len(f.calls)
    snap=capture_portfolio(repository,api,snapshot_id='r25-first');r=portfolio_report(repository,snapshot_id='r25-first',now=f.at)
    require(r['known_gross_realized_pnl']=='0' and r['known_gross_unrealized_pnl']=='1' and r['gross_total_pnl']=='1','portfolio_open_lot_basis_and_mark')
    require(r['net_profit'] is None and r['fees_paid'] is None and not r['fees_reconciled'] and not r['cashflows_verified'],'portfolio_gross_does_not_invent_net_or_zero_fees')
    require(len(f.calls)>start_call and all(c[0]=='GET' for c in f.calls[start_call:]) and snap['orders_sent']==0,'portfolio_capture_reports_no_orders')
    before=f.posts;same=capture_portfolio(repository,api,snapshot_id='r25-first')
    require(same['status']=='existing' and not same['snapshot_refreshed'] and f.posts==before,'portfolio_snapshot_retry_no_double_count')
    plan=broker.prepare_exit(repository,api,'r25-partial-exit',entry);broker.submit(repository,api,'r25-partial-exit')
    f.update(plan['request']['client_order_id'],'partially_filled','0.04','120')
    f.cash='9994.8';capture_portfolio(repository,api,snapshot_id='r25-partial')
    partial=portfolio_report(repository,snapshot_id='r25-partial',now=f.at)
    require(partial['known_gross_realized_pnl']=='0.8' and partial['known_gross_unrealized_pnl']=='0.6' and partial['lots'][0]['remaining_cost_basis']=='6','portfolio_partial_exit_allocates_parent_basis')
    require(partial['cash_comparison']['unexplained_cash_residual']=='-10','portfolio_cash_residual_not_mislabeled_fee')
    require(portfolio_report(repository,snapshot_id='r25-first',now='2026-01-05T15:00:01Z')['gross_total_pnl']=='1','portfolio_old_snapshot_isolated_from_later_fills')
    f.update(plan['request']['client_order_id'],'filled','0.1','120');capture_portfolio(repository,api,snapshot_id='r25-closed')
    closed=portfolio_report(repository,snapshot_id='r25-closed',now=f.at)
    require(closed['known_gross_realized_pnl']=='2' and closed['known_gross_unrealized_pnl']=='0' and closed['lots'][0]['remaining_cost_basis']=='0','portfolio_full_exit_clears_basis')
    require(closed['by_origin']['manual_paper']['lots']==1 and closed['by_origin']['automatic_news']['lots']==0,'portfolio_manual_paper_separate_from_news_strategy')
    require(not closed['period_pnl_calculated'] and not closed['tax_basis_reconciled'] and not closed['profitability_validated'],'portfolio_lifetime_lot_not_tax_or_period_profit')
    a,e=seeded(repository,'r25-cumulative',quantity='0.04',status='partially_filled');capture_portfolio(repository,a.client,snapshot_id='r25-partial-entry')
    a.update(next(iter(a.orders)),'filled','0.1','102');capture_portfolio(repository,a.client,snapshot_id='r25-updated-entry')
    full=portfolio_report(repository,snapshot_id='r25-updated-entry',now=a.at)
    require(full['lots'][0]['entry_quantity']=='0.1' and full['lots'][0]['remaining_cost_basis']=='10.2','portfolio_cumulative_fills_replace_not_sum')
    a.extra_positions=[{'symbol':'MSFT','qty':'1','side':'long'}];capture_portfolio(repository,a.client,snapshot_id='r25-unmanaged')
    require(portfolio_report(repository,snapshot_id='r25-unmanaged',now=a.at)['gross_total_pnl'] is None,'portfolio_unmanaged_position_withholds_total')
    b,e=seeded(repository,'r25-missing');b.missing_mark=True;capture_portfolio(repository,b.client,snapshot_id='r25-missing-mark')
    missing=portfolio_report(repository,snapshot_id='r25-missing-mark',now=b.at)
    require(missing['known_gross_unrealized_pnl'] is None and missing['gross_total_pnl'] is None,'portfolio_missing_mark_is_not_zero_pnl')
    require(not missing['broker_price_freshness_validated'],'portfolio_capture_time_not_price_time')
    c=ValuedFakePaper('r25-uncertain','timeout_before_accept');broker.prepare_buy(repository,c.client,'r25-uncertain-buy','MSFT');broker.submit(repository,c.client,'r25-uncertain-buy')
    before=c.posts;capture_portfolio(repository,c.client,snapshot_id='r25-uncertain')
    uncertain=portfolio_report(repository,snapshot_id='r25-uncertain',now=c.at)
    require(uncertain['status']=='attention_required' and uncertain['gross_total_pnl'] is None and c.posts==before,'portfolio_uncertain_order_not_zero_success_or_resubmit')
    old=portfolio_report(repository,snapshot_id='r25-first',now='2026-01-05T15:10:01Z')
    require(old['saved_observation_stale'] and not old['network_required'],'portfolio_stale_saved_observation_disclosed_offline')
    m=repository.marker;raw=repository.connection.execute(f'SELECT snapshot_json FROM paper_portfolio_snapshots WHERE id={m}',('r25-first',)).fetchone()[0];repository.connection.commit()
    repository.connection.execute(f'UPDATE paper_portfolio_snapshots SET snapshot_json={m} WHERE id={m}',('{}','r25-first'));repository.connection.commit()
    require(reject(lambda:portfolio_report(repository,snapshot_id='r25-first')),'portfolio_snapshot_tamper_rejected')
    repository.connection.execute(f'UPDATE paper_portfolio_snapshots SET snapshot_json={m} WHERE id={m}',(raw,'r25-first'));repository.connection.commit()
    require(portfolio_report(repository,snapshot_id='r25-first',now='2026-01-05T15:00:01Z')['gross_total_pnl']=='1','portfolio_frozen_repeat_digest_and_economics')
    return checks


def verify_portfolio():
    repository=Repository('sqlite:///:memory:')
    try:return {'status':'passed','checks':portfolio_checks(repository),'network_required':False,'model_required':False,
        'user_database_modified':False,'actual_alpaca_connection_tested':False,'orders_sent_to_alpaca':0,
        'fixture_source':'original_controlled_cumulative_orders_and_position_marks','profitability_validated':False,'fees_reconciled':False}
    finally:repository.close()
