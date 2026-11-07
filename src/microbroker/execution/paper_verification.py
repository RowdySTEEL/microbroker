"""Controlled manual prices in a private database/schema; no market/model/broker I/O."""
from .paper_ledger import (create_account, append_event, fill_request, mark_request,
                           account_summary, period_report, encode)
from ..data.models import DataValidationError
from ..data.storage.repository import Repository, StorageError

START='2026-01-05T10:00:00Z'
BUY='2026-01-05T10:01:00Z'
MARK='2026-01-05T10:02:00Z'
SELL='2026-01-05T10:03:00Z'

def paper_checks(repository):
    checks=[]
    def require(ok,name):
        if not ok:raise StorageError('Paper ledger verification failed: '+name)
        checks.append(name)
    def rejects(call):
        try:call()
        except (DataValidationError,StorageError):return True
        return False
    account='r19-controlled'
    made=create_account(repository,account,'10000',START)
    require(made['status']=='created' and made['orders_sent']==0,'paper_account_created_without_orders')
    require(create_account(repository,account,'10000',START)['status']=='existing', 'paper_create_idempotency')
    require(rejects(lambda:create_account(repository,account,'9999',START)), 'paper_config_conflict_rejected')
    buy=fill_request('buy-1','AAPL','buy','10','100',BUY)
    first=append_event(repository,account,buy)
    require(first['cash']=='8998.50000000' and first['positions']['AAPL']['cost_basis']=='1001.50000000'
        and first['total_pnl']=='-1.50000000', 'paper_buy_cash_basis_and_costs')
    require(append_event(repository,account,buy)['status']=='duplicate' and account_summary(repository,account)['fills']==1,
        'paper_fill_exact_retry_no_double_charge')
    require(rejects(lambda:append_event(repository,account,fill_request('buy-1','AAPL','buy','11','100',BUY))),
        'paper_conflicting_event_key_rejected')
    require(rejects(lambda:append_event(repository,account,fill_request('cap','AAPL','buy','20','100',MARK))),
        'paper_position_cost_cap_rejected')
    require(rejects(lambda:append_event(repository,account,fill_request('short','MSFT','sell','1','100',MARK))),
        'paper_short_sale_rejected')
    require(rejects(lambda:append_event(repository,account,fill_request('old','AAPL','buy','1','100',START))),
        'paper_out_of_order_event_rejected')
    marked=append_event(repository,account,mark_request('mark-1','AAPL','110',MARK))
    require(marked['total_pnl']=='98.50000000' and marked['realized_pnl']=='0.00000000',
        'paper_mark_unrealized_equity_identity')
    before=account_summary(repository,account,BUY)
    require(before['total_pnl']=='-1.50000000' and before['events_available_at']==1,
        'paper_future_mark_excluded_at_cutoff')
    sold=append_event(repository,account,fill_request('sell-1','AAPL','sell','4','110',SELL))
    require(sold['cash']=='9437.28000000' and sold['realized_pnl']=='38.18000000'
        and sold['unrealized_pnl']=='59.10000000' and sold['fees_paid']=='2.00000000',
        'paper_partial_sell_realized_unrealized_costs')
    report=period_report(repository,account,START,SELL)
    require(report['period_pnl']=='97.28000000' and report['return_percent']=='0.97280000'
        and report['fills_in_period']==2 and report['fees_in_period']=='2.00000000',
        'paper_period_report_inclusive_boundaries')
    stale=account_summary(repository,account,'2026-01-05T10:09:00Z')
    require(not stale['fresh_mark_coverage'] and stale['stale_symbols']==['AAPL'], 'paper_stale_marks_disclosed')
    # Roll back deliberate corruption so further tests can safely reuse this private account.
    m=repository.marker
    original=repository.connection.execute(f'SELECT state_json FROM paper_ledger WHERE account_id={m} AND ordinal=0',(account,)).fetchone()[0]
    repository.connection.commit()
    repository.connection.execute(f'UPDATE paper_ledger SET state_json={m} WHERE account_id={m} AND ordinal=0',
        (encode({'cash':'999999'}),account))
    repository.connection.commit()
    try:
        require(rejects(lambda:account_summary(repository,account)), 'paper_state_tamper_rejected')
    finally:
        repository.connection.execute(f'UPDATE paper_ledger SET state_json={m} WHERE account_id={m} AND ordinal=0',
            (original,account))
        repository.connection.commit()
    require(account_summary(repository,account)['ledger_events']==3, 'paper_rejected_requests_preserve_committed_ledger')
    mark_retry=append_event(repository,account,mark_request('mark-1','AAPL','110',MARK))
    require(mark_retry['status']=='duplicate' and mark_retry['marks']==1,'paper_mark_retry_no_double_event')
    before=account_summary(repository,account)
    class FailAfterInsert:
        def __init__(self,base):self.base=base
        def __getattr__(self,name):return getattr(self.base,name)
        def execute(self,query,params=()):
            result=self.base.execute(query,params)
            if query.startswith('INSERT INTO paper_ledger'):
                raise StorageError('Controlled failure after event insert')
            return result
    original_connection=repository.connection
    repository.connection=FailAfterInsert(original_connection)
    try:
        failed=rejects(lambda:append_event(repository,account,mark_request('rollback','AAPL','120',SELL)))
    finally:repository.connection=original_connection
    require(failed and account_summary(repository,account)==before,'paper_insert_and_audit_atomic_rollback')
    create_account(repository,'r19-small','10',START,max_position_fraction='1')
    require(rejects(lambda:append_event(repository,'r19-small',fill_request('overcash','AAPL','buy','1','10',BUY)))
        and account_summary(repository,'r19-small')['cash']=='10.00000000','paper_cash_no_leverage')
    closed=append_event(repository,account,fill_request('exit','AAPL','sell','6','110',SELL))
    require(not closed['positions'] and closed['unrealized_pnl']=='0.00000000'
        and closed['cash']=='10095.95000000' and closed['realized_pnl']=='95.95000000','paper_full_exit_basis_cleared')
    create_account(repository,'r19-fraction','100',START,fee_per_fill='0',slippage_bps='0',max_position_fraction='1')
    for key,side,quantity in [('b','buy','0.3'),('s','sell','0.1'),('s2','sell','0.2')]:
        fractional=append_event(repository,'r19-fraction',fill_request(key,'AAPL',side,quantity,'0.333333',BUY))
    require(fractional['cash']=='100.00000000' and not fractional['positions'],'paper_fractional_basis_exact')
    require(period_report(repository,account,START,SELL)==period_report(repository,account,START,SELL),
        'paper_report_repeat_same_committed_ledger')
    return checks

def verify_paper_ledger():
    repository=Repository('sqlite:///:memory:')
    try:
        return {'status':'passed','checks':paper_checks(repository),
            'fixture_source':'original_synthetic_manual_prices','user_database_modified':False,
            'network_required':False,'model_required':False,'broker_connected':False,
            'strategy_implemented':False,'profitability_validated':False,'orders_sent':0}
    finally:repository.close()
