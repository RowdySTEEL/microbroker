"""Exercise the entire verifier control flow with SQLite and a PG protocol shim.
This is not a real PostgreSQL acceptance test.
"""
from decimal import Decimal
import sys
from types import ModuleType
import unittest
from unittest.mock import MagicMock,patch
from microbroker.config import Settings
from microbroker.data.storage.repository import Repository,StorageError
from microbroker.data.storage.postgres_verification import verify_postgres

class ConnectionShim:
    def __init__(self,connection):self.connection=connection
    def __getattr__(self,name):return getattr(self.connection,name)
    def execute(self,query,params=()):
        cursor=self.connection.execute(query.replace('%s','?'),params)
        if query.startswith(('SELECT open FROM bars','SELECT close FROM bars')):
            result=MagicMock()
            result.fetchone=lambda:tuple(Decimal(x) for x in cursor.fetchone())
            return result
        return cursor

class VerifierFlowTests(unittest.TestCase):
    def fake_driver(self):
        pg=ModuleType('psycopg');pg.sql=MagicMock()
        admin=MagicMock();admin.__enter__.return_value=admin
        admin.execute.return_value.fetchone.return_value=(0,)
        pg.connect=MagicMock(return_value=admin)
        return pg
    def test_all_313_checks_reach_analysis_news_research_and_cleanup(self):
        repo=Repository('sqlite:///:memory:')
        repo.connection=ConnectionShim(repo.connection)
        pg=self.fake_driver()
        with patch.dict(sys.modules,{'psycopg':pg}),patch('microbroker.data.storage.postgres_verification.Repository',return_value=repo):
            result=verify_postgres(Settings(database_url='postgresql://test/db'))
        self.assertEqual(result['status'],'passed')
        self.assertEqual(len(result['checks']),313)
        self.assertIn('health_missing_prices_override_wait_headline',result['checks'])
        self.assertIn('history_snapshot_chain_tamper_rejected',result['checks'])
        self.assertIn('research_postgres_repeat_replay',result['checks'])
        self.assertIn('research_postgres_tamper_rejected',result['checks'])
        self.assertIn('news_failure_audit',result['checks'])
        self.assertIn('analysis_failure_audit',result['checks'])
        self.assertEqual(result['checks'][-1],'temporary_schema_cleanup')
    def test_alignment_count_select_transaction_is_closed_before_research_read(self):
        class ReadTransactionShim(ConnectionShim):
            def execute(self,query,params=()):
                if query == 'SELECT COUNT(*) FROM replay_runs' and not self.connection.in_transaction:
                    self.connection.execute('BEGIN')
                return super().execute(query,params)
        repo=Repository('sqlite:///:memory:')
        repo.connection=ReadTransactionShim(repo.connection)
        pg=self.fake_driver()
        with patch.dict(sys.modules,{'psycopg':pg}),patch('microbroker.data.storage.postgres_verification.Repository',return_value=repo):
            result=verify_postgres(Settings(database_url='postgresql://test/db'))
        self.assertEqual(result['status'],'passed')
        self.assertEqual(len(result['checks']),313)

    def test_r28_passive_observation_and_coverage_with_implicit_postgres_transactions(self):
        from types import SimpleNamespace
        from microbroker.execution.observation_verification import observation_checks
        class StrictShim(ConnectionShim):
            @property
            def info(self):return SimpleNamespace(transaction_status=2 if self.connection.in_transaction else 0)
            def execute(self,query,params=()):
                query=query.replace('%s','?').replace(' FOR UPDATE','')
                if query.startswith('SET TRANSACTION'):
                    if self.connection.in_transaction:raise StorageError('Nested implicit transaction')
                    return self.connection.execute('BEGIN')
                if query.lstrip().startswith('SELECT') and not self.connection.in_transaction:
                    self.connection.execute('BEGIN')
                return self.connection.execute(query,params)
        repo=Repository('sqlite:///:memory:');repo.connection=StrictShim(repo.connection);repo.postgres=True;repo.marker='%s'
        try:self.assertEqual(len(observation_checks(repo)),13)
        finally:repo.close()

    def test_driver_error_is_diagnostic_without_secret_message(self):
        pg=self.fake_driver()
        error=RuntimeError('TEST_SECRET_CONNECTION_STRING');error.sqlstate='28000'
        pg.connect.side_effect=error
        with patch.dict(sys.modules,{'psycopg':pg}),self.assertRaises(StorageError) as raised:
            verify_postgres(Settings(database_url='postgresql://test/db'))
        value=str(raised.exception)
        self.assertIn('type=RuntimeError',value)
        self.assertIn('passed_checks=0',value)
        self.assertIn('sqlstate=28000',value)
        self.assertNotIn('TEST_SECRET',value)
