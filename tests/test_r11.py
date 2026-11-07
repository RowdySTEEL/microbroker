"""Retrospective timing, missing company evidence, snapshot isolation and gates."""
import contextlib
from copy import deepcopy
import io
import json
import unittest
from unittest.mock import patch
from microbroker.cli import main
from microbroker.data.alignment import (alignment_fixture, research_alignment, verify_alignment,
                                       read_bars_snapshot, align_records)
from microbroker.data.analysis_replay import replay_analysis
from microbroker.data.models import DataValidationError
from microbroker.data.storage.repository import Repository, StorageError

class R11Tests(unittest.TestCase):
    def setUp(self):
        self.repo=Repository('sqlite:///:memory:')
        self.analysis_id,self.bars_id=alignment_fixture(self.repo)
        self.selected,_,_=replay_analysis(self.repo,self.analysis_id,_include_records=True)
        self.bars,_,_=read_bars_snapshot(self.repo,self.bars_id)
    def tearDown(self):self.repo.close()
    def report(self,**kwargs):return research_alignment(self.repo,self.analysis_id,self.bars_id,**kwargs)
    def rows(self,selected=None,bars=None,lag=5):return align_records(self.selected if selected is None else selected,self.bars if bars is None else bars,['MSFT'],lag)
    def test_boundary_completed_close_not_bar_start(self):
        row=self.rows()[0];self.assertEqual(row['price']['bar_start'],'2026-01-07T10:00:00.000000000Z')
        self.assertEqual(row['price']['completion_floor'],row['feature_available_at'])
        self.assertEqual(row['lag_seconds'],0)
    def test_future_bar_excluded_one_nanosecond_before_end(self):
        selected=deepcopy(self.selected);selected[0]['analysis']['timestamp']='2026-01-07T10:00:59.999999999Z'
        self.assertEqual(self.rows(selected)[0]['price']['bar_start'],'2026-01-07T09:59:00.000000000Z')
    def test_zero_lag_exact_boundary_only(self):
        self.assertEqual(self.rows(lag=0)[0]['status'],'matched')
        selected=deepcopy(self.selected);selected[0]['analysis']['timestamp']='2026-01-07T10:01:00.000000001Z'
        self.assertEqual(self.rows(selected,lag=0)[0]['status'],'previous_bar_too_old')
    def test_no_earlier_bar_does_not_use_future(self):
        row=self.rows(bars=self.bars[-1:])[0]
        self.assertEqual(row['status'],'no_completed_bar_at_or_before_feature');self.assertIsNone(row['price'])
    def test_empty_bars_not_zero_price(self):
        self.assertIsNone(self.rows(bars=[])[0]['price'])
    def test_stale_price_not_filled(self):
        selected=deepcopy(self.selected);selected[0]['analysis']['timestamp']='2026-01-08T10:01:00.000000000Z'
        row=self.rows(selected)[0];self.assertEqual(row['status'],'previous_bar_too_old');self.assertIsNone(row['price'])
    def test_company_missing_does_not_use_document_label(self):
        selected=deepcopy(self.selected);selected[0]['analysis']['result']['entity_sentiments']={}
        row=self.rows(selected)[0];self.assertEqual(row['document_label'],'positive');self.assertIsNone(row['company_label']);self.assertIsNone(row['price'])
    def test_shared_context_excluded(self):
        selected=deepcopy(self.selected);company=selected[0]['analysis']['result']['entity_sentiments']['MSFT'];company['status']='shared_context_only'
        from microbroker.intelligence.research_decision import research_decision
        from microbroker.intelligence.unit_evidence import company_unit_decision
        company['research_decision']=company_unit_decision(company['sentiment'],company.get('evidence_summary'),company['status'],company['unit_evidence'])
        self.assertEqual(self.rows(selected)[0]['status'],'company_sentiment_unavailable')
    def test_pending_analysis_cannot_match_price(self):
        report=self.report(at='2026-01-06T12:00:00Z');self.assertEqual(report['status_counts'],{'pending_analysis':1});self.assertEqual(report['matched_rows'],0)
    def test_before_receipt_no_company_rows(self):
        self.assertEqual(self.report(at='2026-01-05T12:00:00Z')['company_rows'],0)
    def test_other_symbol_price_not_used(self):
        self.assertIsNone(self.rows(bars=[{**bar,'symbol':'AAPL'} for bar in self.bars])[0]['price'])
    def test_decimal_close_remains_string(self):
        bars=deepcopy(self.bars);bars[1]['close']='101.000000000000000001'
        self.assertEqual(self.rows(bars=bars)[0]['price']['close'],'101.000000000000000001')
    def test_no_writes_and_no_inference(self):
        before=list(self.repo.connection.iterdump())
        with patch('microbroker.intelligence.analysis.compute_analysis',side_effect=AssertionError('No inference')):
            report=self.report()
        self.assertEqual(list(self.repo.connection.iterdump()),before);self.assertFalse(report['database_modified'])
    def test_repeat_identical_full_report(self):self.assertEqual(self.report(),self.report())
    def test_live_source_deletion_does_not_change_report(self):
        before=self.report();self.repo.connection.execute('DELETE FROM bars');self.repo.connection.execute('DELETE FROM news_analyses');self.repo.connection.commit()
        self.assertEqual(self.report(),before)
    def test_wrong_price_snapshot_kind(self):
        with self.assertRaises(DataValidationError):research_alignment(self.repo,self.analysis_id,self.analysis_id)
    def test_daily_bars_rejected(self):
        raw=self.repo.connection.execute('SELECT request_json FROM replay_runs WHERE id=?',(self.bars_id,)).fetchone()[0]
        metadata=json.loads(raw);metadata['timeframe']='1Day'
        self.repo.connection.execute('UPDATE replay_runs SET request_json=? WHERE id=?',(json.dumps(metadata),self.bars_id));self.repo.connection.commit()
        with self.assertRaises(DataValidationError):self.report()
    def test_wrong_feed_rejected(self):
        raw=self.repo.connection.execute('SELECT request_json FROM replay_runs WHERE id=?',(self.bars_id,)).fetchone()[0]
        metadata=json.loads(raw);metadata['feed']='sip'
        self.repo.connection.execute('UPDATE replay_runs SET request_json=? WHERE id=?',(json.dumps(metadata),self.bars_id));self.repo.connection.commit()
        with self.assertRaises(StorageError):self.report()
    def test_corrupt_price_payload_rejected(self):
        self.repo.connection.execute("UPDATE replay_items SET record_json='{}' WHERE replay_id=?",(self.bars_id,));self.repo.connection.commit()
        with self.assertRaises(StorageError):self.report()
    def test_snapshot_byte_bound(self):
        with patch('microbroker.data.alignment.MAX_BYTES',1):
            with self.assertRaises(StorageError):self.report()
    def test_invalid_lag_rejected(self):
        for value in (-1,1441,True,1.5):
            with self.assertRaises(DataValidationError):self.report(max_lag_minutes=value)
    def test_unknown_price_id(self):
        with self.assertRaises(DataValidationError):research_alignment(self.repo,self.analysis_id,'unknown')
    def test_context_report_preserves_research_limitations(self):
        report=self.report();self.assertFalse(report['point_in_time_validated']);self.assertFalse(report['returns_calculated']);self.assertFalse(report['strategy_implemented']);self.assertFalse(report['usable_for_trading'])
        self.assertEqual(report['quality_status'],'warning')
    def test_verifier_cli_without_settings(self):
        self.assertEqual(len(verify_alignment()['checks']),10)
        with patch('microbroker.cli.Settings.from_env',side_effect=AssertionError('No env')),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main(['verify-research-alignment']),0)
    def test_intraday_gaps_reported_without_calendar_assumption(self):
        self.repo.connection.execute("DELETE FROM bars WHERE timestamp LIKE '%10:00:%'");self.repo.connection.commit()
        from microbroker.data.research import create_replay,ResearchRequest
        snapshot=create_replay(self.repo,ResearchRequest(symbols=('MSFT',)))
        report=research_alignment(self.repo,self.analysis_id,snapshot['replay_id'])
        self.assertEqual(report['candidate_intraday_bar_gap_intervals'],1)
        self.assertEqual(report['samples'][0]['lag_seconds'],60)
    def test_last_bar_too_old_report_exposes_lag_without_price(self):
        row=self.rows(bars=[{**self.bars[0],'timestamp':'2026-01-06T10:00:00.000000000Z'}])[0]
        self.assertGreater(row['lag_seconds'],300);self.assertIsNotNone(row['previous_bar_completed_at']);self.assertIsNone(row['price'])
    def test_bar_ordinal_corruption_rejected(self):
        self.repo.connection.execute('UPDATE replay_items SET ordinal=9 WHERE replay_id=? AND ordinal=2',(self.bars_id,));self.repo.connection.commit()
        with self.assertRaises(StorageError):self.report()
    def test_bar_row_bound_before_consumer(self):
        with patch('microbroker.data.alignment.MAX_VERSIONS',1):
            with self.assertRaises(StorageError):self.report()
    def test_cli_reports_snapshot_alignment(self):
        from microbroker.config import Settings
        settings=Settings.from_env('/nonexistent')
        with patch('microbroker.cli.Settings.from_env',return_value=settings),patch('microbroker.cli.Repository',return_value=self.repo),patch.object(self.repo,'close'):
            out=io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(main(['research-alignment','--analysis-replay-id',self.analysis_id,'--bars-replay-id',self.bars_id]),0)
            self.assertEqual(json.loads(out.getvalue())['matched_rows'],1)
