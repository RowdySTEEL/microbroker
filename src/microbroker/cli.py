import argparse
import json
import sqlite3
import sys
from pathlib import Path
from . import __version__
from .config import Settings, ConfigurationError, UNIVERSE
from .data.adapters.alpaca import AlpacaHistoricalData, MarketDataError
from .data.ingestion.collector import collect_historical
from .data.ingestion.gap_recovery import recover_stream_gaps
from .data.models import utc_datetime, DataValidationError
from .data.request import BarsRequest
from .data.storage.repository import Repository, StorageError
from .data.storage.postgres_verification import verify_postgres
from .data.stream_models import StreamRequest
from .data.adapters.alpaca_stream import AlpacaStream
from .observability.logging import configure_logging
from .data.research import ResearchRequest, inspect_dataset, create_replay, replay, verify_research
from .data.news import NewsRequest,AlpacaNews,collect_news,news_summary
from .intelligence.analysis import AnalysisRequest, analyze_news, analysis_summary
from .intelligence.sentiment import make_engine, AnalysisError
from .intelligence.model_files import prepare_finbert
from .intelligence.verification import verify_analysis
from .intelligence.decision_verification import verify_research_decision
from .intelligence.unit_verification import verify_unit_evidence
from .intelligence.review import export_review, evaluate_review
from .data.news_replay import NewsReplayRequest, create_news_replay, replay_news, verify_news_replay

from .data.analysis_replay import create_analysis_replay, replay_analysis, verify_analysis_replay

from .data.alignment import research_alignment, verify_alignment

from .data.session import SessionRequest, run_session, verify_session, session_summary
from .data.session_price_verification import verify_session_prices
from .intelligence.explanation import explain_analysis
from .intelligence.review_verification import verify_review_workflow
from .intelligence.attribution_verification import verify_attribution

from .execution.paper_ledger import create_account, append_event, fill_request, mark_request, account_summary, period_report
from .execution.paper_verification import verify_paper_ledger
from .execution.strategy import run_strategy, strategy_report
from .execution.strategy_verification import verify_strategy, demo_strategy
from .execution.paper_api import PaperAPI, PaperBrokerError, check_connection
from .execution.paper_broker import prepare_buy, prepare_exit, submit as submit_paper, reconcile as reconcile_paper, cancel as cancel_paper, report as broker_report, reconcile_account
from .execution.paper_broker_verification import verify_paper_broker, demo_paper_broker
from .execution.paper_monitor import report_session,render_report
from .data.news_probe import inspect_news
from .execution.paper_worker import run_paper_session
from .execution.paper_worker_verification import verify_paper_session, demo_paper_session
from .execution.equity_history import capture_equity, performance_report
from .execution.paper_portfolio import capture_portfolio,portfolio_report,render_portfolio
from .execution.portfolio_history import history_report,render_history
from .execution.risk_controls import risk_policy
from .execution.risk_verification import verify_risk_controls, demo_risk_controls

def main(argv=None):
    parser = argparse.ArgumentParser(prog="microbroker")
    parser.add_argument("--env-file", default=".env")
    sub = parser.add_subparsers(dest="command", required=True)
    portfolio=sub.add_parser('paper-portfolio-history',help='Offline observed day/week/30-day gross snapshot changes; not execution-date net profit')
    portfolio.add_argument('--period',choices=['day','week','month'],default='week')
    portfolio.add_argument('--account-ref',default=None)
    portfolio.add_argument('--session-id',default=None)
    portfolio.add_argument('--start',default=None)
    portfolio.add_argument('--end',default=None)
    portfolio.add_argument('--limit',type=int,default=20)
    portfolio.add_argument('--format',choices=['json','text'],default='json')
    sub.add_parser('verify-portfolio-history',help='Private automatic sampling and observed gross history checks; no API/model')
    sub.add_parser('verify-session-observation',help='Private R28 latency, deadline and passive observer checks')
    sub.add_parser('verify-report-evidence',help='Private R29 capture timing, revision coverage and warning history checks')
    sub.add_parser('verify-event-study',help='Private R30 event returns and SPY alignment checks; no API/model/orders')
    study=sub.add_parser('demo-event-study',help='Create a clearly marked synthetic outcome report; no credentials/model/orders')
    study.add_argument('--output',required=True)
    study=sub.add_parser('event-study',help='Offline descriptive company outcomes and SPY differences from frozen receipts')
    choice=study.add_mutually_exclusive_group(required=True)
    choice.add_argument('--session-id',default=None)
    choice.add_argument('--latest',action='store_true')
    choice.add_argument('--analysis-replay-id',default=None)
    study.add_argument('--bars-replay-id',default=None)
    study.add_argument('--end',default=None)
    study.add_argument('--limit',type=int,default=20)
    study.add_argument('--format',choices=['json','text'],default='json')
    study.add_argument('--output',default=None,help='Save the JSON report to a new file')
    study=sub.add_parser('run-event-study-session',help='Market data only: company news and company+SPY receipts; never Paper orders')
    study.add_argument('--symbols',default='AAPL,MSFT,NVDA,TSLA')
    study.add_argument('--feed',choices=['iex','sip'],default='iex')
    study.add_argument('--duration',type=int,default=5400)
    study.add_argument('--poll-seconds',type=int,default=30)
    study.add_argument('--lookback-minutes',type=int,default=10)
    study.add_argument('--model-dir',default='models/finbert')
    sub.add_parser('verify-paper-report-health',help='Private saved-price health report regressions; no API/model')
    portfolio=sub.add_parser('demo-portfolio-history',help='Separate synthetic portfolio history demo; no Alpaca orders')
    portfolio.add_argument('--output',required=True)
    portfolio=sub.add_parser('capture-paper-portfolio',help='GET-only reconcile and freeze cumulative fills, positions and account; no orders')
    portfolio.add_argument('--snapshot-id',default=None)
    portfolio=sub.add_parser('paper-portfolio-report',help='Offline gross realized/unrealized managed lot P&L; not net profit')
    portfolio.add_argument('--snapshot-id',default=None)
    portfolio.add_argument('--account-ref',default=None)
    portfolio.add_argument('--format',choices=['json','text'],default='json')
    sub.add_parser('verify-paper-portfolio',help='Private cumulative fill, lot, valuation and snapshot checks; no API/model')
    latency=sub.add_parser('paper-session-latency',help='Offline recorded stage and publication-to-order latency observations')
    latency.add_argument('--session-id');latency.add_argument('--format',choices=['json','text'],default='text')
    monitor=sub.add_parser('paper-session-report', help='Offline saved Paper session, news decisions and latest locally recorded fills')
    monitor.add_argument('--session-id', default=None)
    monitor.add_argument('--limit', type=int, default=20)
    monitor.add_argument('--format', choices=['json','text'], default='json')
    probe=sub.add_parser('inspect-news-feed', help='GET-only recent news response; no stored arrivals, model or orders')
    probe.add_argument('--symbols', default='AAPL,MSFT')
    probe.add_argument('--lookback-minutes', type=int, default=60)
    monitor_demo=sub.add_parser('demo-paper-monitor', help='Readable automatic fake buy/fill/exit/neutral/conflict demo; no Alpaca orders')
    monitor_demo.add_argument('--output', required=True)
    sub.add_parser('verify-paper-monitor', help='Private session audit/news-probe checks; no API/model')
    sub.add_parser('verify-paper-session', help='Private fake-news/Paper worker and equity history checks; no API/model')
    worker_demo=sub.add_parser('demo-paper-session', help='Separate synthetic news/Paper database; no real API orders')
    worker_demo.add_argument('--output', required=True)
    worker=sub.add_parser('run-paper-session', help='Bounded real-news automatic Alpaca Paper orders; keeps positions at end')
    worker.add_argument('--symbols', default='AAPL,MSFT')
    worker.add_argument('--feed', choices=['iex','sip'], default='iex')
    worker.add_argument('--duration', type=int, default=900)
    worker.add_argument('--poll-seconds', type=int, choices=[30,60], default=30)
    worker.add_argument('--lookback-minutes', type=int, default=5)
    worker.add_argument('--notional', default='10')
    worker.add_argument('--max-buys', type=int, default=3)
    worker.add_argument('--model-dir', default='models/finbert')
    sample=sub.add_parser('capture-paper-equity', help='GET Paper account equity and store one observation; no orders')
    sample.add_argument('--sample-id', default=None)
    history=sub.add_parser('paper-performance', help='Offline trailing day/week/30-day account equity change, not net profit')
    history.add_argument('--period', choices=['day','week','month'], default='week')
    history.add_argument('--account-ref', default=None)
    history.add_argument('--start', default=None)
    history.add_argument('--end', default=None)
    sub.add_parser('verify-paper-broker', help='Private controlled broker transport and persistence checks; sends no API orders')
    sub.add_parser('reconcile-paper-account', help='Reconcile managed cumulative fills against Paper positions; sends no orders')
    sub.add_parser('paper-broker-check', help='Read-only Paper account and market-clock connection check')
    broker_demo=sub.add_parser('demo-paper-broker', help='Automatic fake-broker timeout/partial-fill demo in separate SQLite')
    broker_demo.add_argument('--output', required=True)
    broker=sub.add_parser('prepare-paper-buy', help='Record a bounded Paper buy intent; no order sent')
    broker.add_argument('--intent-id', required=True)
    broker.add_argument('--symbol', required=True)
    broker.add_argument('--notional', default='10')
    broker=sub.add_parser('prepare-paper-exit', help='Reserve only unreserved filled shares from a terminal managed entry')
    broker.add_argument('--intent-id', required=True)
    broker.add_argument('--entry-intent-id', required=True)
    for name in ('submit-paper-order','reconcile-paper-order','cancel-paper-order','paper-broker-report'):
        broker=sub.add_parser(name, help='Paper-only durable intent operation; report reads local records only')
        broker.add_argument('--intent-id', required=True)
    sub.add_parser('verify-risk-controls', help='Private daily loss, entry limit and price interruption checks; no API/model')
    risk_demo = sub.add_parser('demo-risk-controls', help='Automatic synthetic risk scenarios in separate SQLite files')
    risk_demo.add_argument('--output', required=True)
    sub.add_parser('verify-strategy', help='Private guarded-signal/receipt/accounting simulation checks; no API/model')
    strategy = sub.add_parser('demo-strategy', help='Automatic synthetic scenario in a separate SQLite file; not live performance')
    strategy.add_argument('--output', required=True)
    strategy = sub.add_parser('run-strategy-simulation', help='Simulate a sealed long-only strategy on frozen guarded analyses and stream receipts')
    strategy.add_argument('--run-id', required=True)
    strategy.add_argument('--analysis-replay-id', required=True)
    strategy.add_argument('--bars-replay-id', required=True)
    strategy.add_argument('--start', default=None)
    strategy.add_argument('--end', default=None)
    strategy.add_argument('--cash', default='10000')
    strategy.add_argument('--fee-per-fill', default='1')
    strategy.add_argument('--slippage-bps', default='5')
    strategy.add_argument('--max-position-fraction', default='0.25')
    strategy.add_argument('--entry-fraction', default='0.10')
    strategy.add_argument('--max-signal-age-seconds', type=int, default=300)
    strategy.add_argument('--max-price-age-seconds', type=int, default=120)
    strategy.add_argument('--max-news-age-seconds', type=int, default=900)
    strategy.add_argument('--daily-loss-fraction', default='0.02')
    strategy.add_argument('--max-daily-entries', type=int, default=10)
    strategy.add_argument('--max-feed-gap-seconds', type=int, default=180)
    strategy = sub.add_parser('strategy-simulation-report', help='Recompute and verify a sealed simulation without model/API')
    strategy.add_argument('--run-id', required=True)
    sub.add_parser('verify-paper-ledger', help='Private synthetic cash, position, cost and persistence checks; no API/model')
    paper = sub.add_parser('paper-create', help='Create a local manual-price USD scenario account')
    paper.add_argument('--account', required=True)
    paper.add_argument('--cash', required=True)
    paper.add_argument('--start', required=True)
    paper.add_argument('--fee-per-fill', default='1')
    paper.add_argument('--slippage-bps', default='5')
    paper.add_argument('--max-position-fraction', default='0.25')
    paper.add_argument('--max-mark-age-seconds', type=int, default=300)
    for command in ('paper-fill', 'paper-mark'):
        paper = sub.add_parser(command, help='Record an explicit local scenario event; no broker order')
        paper.add_argument('--account', required=True)
        paper.add_argument('--event-key', required=True)
        paper.add_argument('--symbol', required=True, choices=UNIVERSE)
        paper.add_argument('--price', required=True)
        paper.add_argument('--at', required=True)
        if command == 'paper-fill':
            paper.add_argument('--side', required=True, choices=['buy','sell'])
            paper.add_argument('--quantity', required=True)
    paper = sub.add_parser('paper-summary', help='Verify and value a local scenario account at an inclusive cutoff')
    paper.add_argument('--account', required=True)
    paper.add_argument('--at', default=None)
    paper = sub.add_parser('paper-report', help='Compare inclusive start/end valuations; manual assumptions only')
    paper.add_argument('--account', required=True)
    paper.add_argument('--start', required=True)
    paper.add_argument('--end', required=True)
    freeze_news = sub.add_parser('create-news-replay', help='Freeze recorded news versions up to an inclusive receipt cutoff')
    freeze_news.add_argument('--symbols', default=None)
    freeze_news.add_argument('--as-of', required=True)
    freeze_news.add_argument('--first-seen-since', default=None)
    play_news = sub.add_parser('replay-news', help='Reconstruct news state from a frozen version snapshot')
    play_news.add_argument('--replay-id', required=True)
    play_news.add_argument('--at', default=None)
    sub.add_parser('verify-news-replay', help='Run controlled-time news fixtures in memory; no API or model')
    freeze_analysis = sub.add_parser('create-analysis-replay', help='Freeze existing analyses for exact frozen news revisions; no inference')
    freeze_analysis.add_argument('--news-replay-id', required=True)
    freeze_analysis.add_argument('--as-of', required=True)
    freeze_analysis.add_argument('--analyzer-key', required=True)
    freeze_analysis.add_argument('--engine', choices=['finbert', 'rules'], default='finbert')
    play_analysis = sub.add_parser('replay-analysis', help='Replay sealed news and timestamp-gated analyses')
    play_analysis.add_argument('--replay-id', required=True)
    play_analysis.add_argument('--at', default=None)
    sub.add_parser('verify-analysis-replay', help='Controlled-time analysis fixtures without model or API')
    alignment = sub.add_parser('research-alignment', help='Retrospective company sentiment/price coverage from verified snapshots')
    alignment.add_argument('--analysis-replay-id', required=True)
    alignment.add_argument('--bars-replay-id', required=True)
    alignment.add_argument('--at', default=None)
    alignment.add_argument('--max-lag-minutes', type=int, default=5)
    sub.add_parser('verify-research-alignment', help='Controlled company/price timing checks without model or API')
    session = sub.add_parser('run-research-session', help='Bounded concurrent prices + polling news and analysis, no orders')
    session.add_argument('--symbols', default='AAPL,MSFT')
    session.add_argument('--feed', choices=['iex','sip'], default='iex')
    session.add_argument('--duration', type=int, default=300)
    session.add_argument('--poll-seconds', type=int, default=60)
    session.add_argument('--lookback-minutes', type=int, default=10)
    session.add_argument('--engine', choices=['finbert','rules'], default='finbert')
    session.add_argument('--model-dir', default='models/finbert')
    sub.add_parser('verify-research-session', help='Private concurrent session fixtures without API or model')
    sub.add_parser('verify-session-prices', help='Private session receipt isolation and time boundary checks')
    session_report = sub.add_parser('research-session-summary', help='Read a saved session audit without API/model')
    session_report.add_argument('--session-id', required=True)
    doctor = sub.add_parser("doctor", help="Check local configuration without revealing credentials")
    doctor.add_argument('--require-postgres', action='store_true')
    doctor.add_argument('--require-credentials', action='store_true')
    sub.add_parser("init-db", help="Create/extend research schema without deleting market data")
    model = sub.add_parser('prepare-finbert', help='Explicitly download and verify pinned public FinBERT files')
    model.add_argument('--model-dir', default='models/finbert')
    model_test = sub.add_parser('verify-analysis', help='Run sentiment and persistence smoke cases in memory')
    model_test.add_argument('--engine', choices=['rules', 'finbert'], default='finbert')
    model_test.add_argument('--model-dir', default='models/finbert')
    review = sub.add_parser('export-analysis-review', help='Export frozen news for human labeling without model predictions')
    review.add_argument('--symbols', default=None)
    review.add_argument('--engine', choices=['rules','finbert'], default='finbert')
    review.add_argument('--analyzer-key', default=None)
    review.add_argument('--output', required=True)
    evaluation = sub.add_parser('evaluate-analysis-review', help='Evaluate reviewer labels against frozen predictions; no API/model/database access')
    evaluation.add_argument('--review-dir', required=True)
    evaluation.add_argument('--labels-file', default=None)
    sub.add_parser('verify-analysis-review', help='Private evidence, labeling and exposure metric checks')
    sub.add_parser('verify-unit-evidence', help='Private bounded unit scores, conflict and historical hint checks')
    sub.add_parser('verify-research-decision', help='Private conflict, abstention, alignment and review checks')
    sub.add_parser('verify-attribution', help='Private ambiguous subject/counterparty controls')
    explanation = sub.add_parser('explain-news-analysis', help='Inspect stored verified input/context without inference')
    explanation.add_argument('--article-id', required=True, type=int)
    explanation.add_argument('--symbol', default=None)
    explanation.add_argument('--revision-hash', default=None)
    explanation.add_argument('--analyzer-key', default=None)
    explanation.add_argument('--engine', choices=['rules','finbert'], default='finbert')
    explanation.add_argument('--output', default=None)
    for command in ('analyze-news', 'analysis-summary'):
        analysis = sub.add_parser(command, help='Analyze stored news or inspect revision-based analysis')
        analysis.add_argument('--symbols', default=None)
        analysis.add_argument('--engine', choices=['rules', 'finbert'], default='finbert')
        if command == 'analyze-news':
            analysis.add_argument('--model-dir', default='models/finbert')
        else:
            analysis.add_argument('--analyzer-key', default=None)
    news=sub.add_parser('collect-news',help='Fetch Alpaca news; preserve revisions, no sentiment or orders')
    news.add_argument('--symbols',default=','.join(UNIVERSE))
    news.add_argument('--start',required=True)
    news.add_argument('--end',required=True)
    news.add_argument('--page-limit',type=int,default=50)
    news.add_argument('--max-pages',type=int,default=100)
    summary=sub.add_parser('news-summary',help='Report stored news and explicit symbol mappings')
    summary.add_argument('--symbols',default=None)
    for command in ('data-quality','create-replay'):
        research=sub.add_parser(command,help='Inspect or freeze stored data; no API calls')
        research.add_argument('--source',choices=['bars','events'],default='bars')
        research.add_argument('--symbols',default=None)
        research.add_argument('--feed',choices=['iex','sip','test'])
        research.add_argument('--timeframe',choices=['1Min','1Day'],default='1Min')
        research.add_argument('--start')
        research.add_argument('--end')
        research.add_argument('--order',choices=['received','event'],default='received')
    play=sub.add_parser('replay',help='Validate and play a frozen dataset; no orders')
    play.add_argument('--replay-id',required=True)
    verify=sub.add_parser('verify-research',help='Freeze bars/events and replay each twice')
    verify.add_argument('--symbols',default='AAPL,MSFT')
    verify.add_argument('--feed',choices=['iex','sip','test'])
    collect = sub.add_parser("collect-bars", help="Fetch historical stock OHLCV bars; no orders")
    collect.add_argument("--symbols", default=",".join(UNIVERSE))
    collect.add_argument("--start", required=True, help="ISO 8601 with timezone")
    collect.add_argument("--end", required=True, help="ISO 8601 with timezone")
    collect.add_argument("--timeframe", default="1Day")
    collect.add_argument("--feed", choices=["iex", "sip"])
    collect.add_argument("--adjustment", choices=["raw", "split", "dividend", "all"], default="raw")
    collect.add_argument("--page-limit", type=int, default=1000)
    sub.add_parser("verify-postgres", help="Test rollback/upserts in an owned temporary PostgreSQL schema")
    recovery = sub.add_parser('recover-stream-gaps', help='Retry persisted gaps; insert missing minute bars only')
    recovery.add_argument('--run-id', default=None, help='Restrict recovery to one stream session')
    stream = sub.add_parser("stream-market-data", help="Read Alpaca stock stream; no orders")
    stream.add_argument("--symbols", default=None)
    stream.add_argument("--feed", choices=["iex", "sip"])
    stream.add_argument("--test-feed", action="store_true", help="Use isolated FAKEPACA test data")
    stream.add_argument("--channels", default=None, help="Comma separated bars,updatedBars,trades,quotes")
    stream.add_argument("--duration", type=float, default=0, help="Seconds; 0 runs until Ctrl+C")
    stream.add_argument("--max-events", type=int, default=0, help="Data events received; 0 is unlimited")
    stream.add_argument("--max-reconnects", type=int, default=10)
    args = parser.parse_args(argv)
    repository = None
    try:
        if args.command=='demo-event-study':
            from .data.event_study_verification import demo_event_study
            print(json.dumps(demo_event_study(args.output)));return 0
        if args.command == 'verify-event-study':
            from .data.event_study_verification import verify_event_study
            print(json.dumps(verify_event_study()));return 0
        if args.command == 'verify-report-evidence':
            from .execution.report_evidence_verification import verify_evidence
            print(json.dumps(verify_evidence()));return 0
        if args.command == 'verify-session-observation':
            from .execution.observation_verification import verify_observation
            print(json.dumps(verify_observation()));return 0
        if args.command == 'verify-paper-report-health':
            from .execution.report_health_verification import verify_health
            print(json.dumps(verify_health()));return 0
        if args.command == 'verify-portfolio-history':
            from .execution.portfolio_history_verification import verify_history
            print(json.dumps(verify_history()));return 0
        if args.command == 'demo-portfolio-history':
            from .execution.portfolio_history_verification import demo_history
            print(json.dumps(demo_history(args.output)));return 0
        if args.command == 'verify-paper-portfolio':
            from .execution.portfolio_verification import verify_portfolio
            print(json.dumps(verify_portfolio()));return 0
        if args.command == 'demo-paper-monitor':
            from .execution.paper_monitor_verification import demo_paper_monitor
            print(json.dumps(demo_paper_monitor(args.output)));return 0
        if args.command == 'verify-paper-monitor':
            from .execution.paper_monitor_verification import verify_paper_monitor
            print(json.dumps(verify_paper_monitor()));return 0
        if args.command == 'verify-paper-session':
            print(json.dumps(verify_paper_session())); return 0
        if args.command == 'demo-paper-session':
            print(json.dumps(demo_paper_session(args.output))); return 0
        if args.command == 'verify-paper-broker':
            print(json.dumps(verify_paper_broker())); return 0
        if args.command == 'demo-paper-broker':
            print(json.dumps(demo_paper_broker(args.output))); return 0
        if args.command == 'verify-risk-controls':
            print(json.dumps(verify_risk_controls())); return 0
        if args.command == 'demo-risk-controls':
            print(json.dumps(demo_risk_controls(args.output))); return 0
        if args.command == 'verify-strategy':
            print(json.dumps(verify_strategy())); return 0
        if args.command == 'demo-strategy':
            print(json.dumps(demo_strategy(args.output))); return 0
        if args.command == 'verify-paper-ledger':
            print(json.dumps(verify_paper_ledger())); return 0
        if args.command == 'verify-research-session':
            print(json.dumps(verify_session())); return 0
        if args.command == 'verify-session-prices':
            print(json.dumps(verify_session_prices())); return 0
        if args.command == 'verify-research-alignment':
            print(json.dumps(verify_alignment())); return 0
        if args.command == 'verify-analysis-replay':
            print(json.dumps(verify_analysis_replay())); return 0
        if args.command == 'verify-news-replay':
            print(json.dumps(verify_news_replay())); return 0
        if args.command == 'evaluate-analysis-review':
            print(json.dumps(evaluate_review(args.review_dir,args.labels_file)))
            return 0
        if args.command == 'verify-analysis-review':
            print(json.dumps(verify_review_workflow()));return 0
        if args.command == 'verify-unit-evidence':
            print(json.dumps(verify_unit_evidence()));return 0
        if args.command == 'verify-research-decision':
            print(json.dumps(verify_research_decision()));return 0
        if args.command == 'verify-attribution':
            print(json.dumps(verify_attribution()));return 0
        settings = Settings.from_env(args.env_file)
        configure_logging(settings.log_level)
        if args.command == 'paper-portfolio-history':
            repository=Repository(settings.database_url)
            result=history_report(repository,args.account_ref,period=args.period,start=args.start,end=args.end,session_id=args.session_id,limit=args.limit)
            print(render_history(result) if args.format=='text' else json.dumps(result));return 3 if result['status']=='attention_required' else 0
        if args.command == 'capture-paper-portfolio':
            repository=Repository(settings.database_url)
            print(json.dumps(capture_portfolio(repository,PaperAPI(settings),snapshot_id=args.snapshot_id)));return 0
        if args.command == 'paper-portfolio-report':
            repository=Repository(settings.database_url)
            result=portfolio_report(repository,args.account_ref,snapshot_id=args.snapshot_id)
            print(render_portfolio(result) if args.format=='text' else json.dumps(result));return 0 if result['status']=='ready' else 3
        if args.command == 'inspect-news-feed':
            print(json.dumps(inspect_news(settings,tuple(args.symbols.split(',')),args.lookback_minutes)));return 0
        if args.command == 'paper-session-latency':
            from .execution.latency_report import latency_report,render_latency
            repository=Repository(settings.database_url)
            result=latency_report(repository,args.session_id)
            print(render_latency(result) if args.format=='text' else json.dumps(result));return 0
        if args.command == 'paper-session-report':
            repository=Repository(settings.database_url)
            result=report_session(repository,args.session_id,limit=args.limit)
            print(render_report(result) if args.format=='text' else json.dumps(result));return 0
        if args.command == 'capture-paper-equity':
            api=PaperAPI(settings);repository=Repository(settings.database_url)
            print(json.dumps(capture_equity(repository,api,sample_id=args.sample_id)));return 0
        if args.command == 'paper-performance':
            repository=Repository(settings.database_url)
            print(json.dumps(performance_report(repository,args.account_ref,period=args.period,start=args.start,end=args.end)));return 0
        if args.command == 'run-paper-session':
            request=SessionRequest(tuple(args.symbols.split(',')),args.feed,args.duration,args.poll_seconds,args.lookback_minutes)
            settings.require_credentials()
            if not settings.database_url.startswith('postgresql://') or settings.data_env!='production':
                raise DataValidationError('Paper sessions require production market data and PostgreSQL')
            engine=make_engine('finbert',args.model_dir)
            result=run_paper_session(settings,request,engine,notional=args.notional,max_buys=args.max_buys)
            print(json.dumps(result));return 2 if result['status']=='failed' or result.get('coverage_error') or result.get('equity_capture_error') or result.get('worker_cleanup_error') or result.get('paper_audit_error') or result.get('portfolio_capture_error') or result.get('portfolio_sampling',{}).get('sampling_degraded') else (3 if result['status']=='no_data' else 0)
        if args.command == 'paper-broker-check':
            print(json.dumps(check_connection(PaperAPI(settings)))); return 0
        if args.command == 'reconcile-paper-account':
            api=PaperAPI(settings);repository=Repository(settings.database_url)
            result=reconcile_account(repository,api);print(json.dumps(result));return 0 if result['status']=='matched' else 3
        if args.command in {'prepare-paper-buy','prepare-paper-exit','submit-paper-order','reconcile-paper-order','cancel-paper-order','paper-broker-report'}:
            api=PaperAPI(settings) if args.command!='paper-broker-report' else None
            repository=Repository(settings.database_url)
            if args.command=='prepare-paper-buy':result=prepare_buy(repository,api,args.intent_id,args.symbol,args.notional)
            elif args.command=='prepare-paper-exit':result=prepare_exit(repository,api,args.intent_id,args.entry_intent_id)
            elif args.command=='submit-paper-order':result=submit_paper(repository,api,args.intent_id)
            elif args.command=='reconcile-paper-order':result=reconcile_paper(repository,api,args.intent_id)
            elif args.command=='cancel-paper-order':result=cancel_paper(repository,api,args.intent_id)
            else:result=broker_report(repository,args.intent_id)
            print(json.dumps(result)); return 3 if result.get('phase')=='uncertain' else 0
        if args.command == 'run-event-study-session':
            from .data.event_collection import EventStudySessionRequest,run_event_collection
            request=EventStudySessionRequest(tuple(args.symbols.split(',')),args.feed,args.duration,args.poll_seconds,args.lookback_minutes)
            settings.require_credentials()
            if not settings.database_url.startswith('postgresql://') or settings.data_env!='production':
                raise DataValidationError('Event research requires production market data and PostgreSQL')
            result=run_event_collection(settings,request,make_engine('finbert',args.model_dir))
            print(json.dumps(result));return {'completed':0,'stopped':0,'no_data':3,'failed':2}[result['status']]
        if args.command == 'run-research-session':
            request = SessionRequest(tuple(args.symbols.split(',')),args.feed,args.duration,args.poll_seconds,args.lookback_minutes)
            settings.require_credentials()
            if not settings.database_url.startswith('postgresql://') or settings.data_env!='production':
                raise DataValidationError('Research sessions require production market data and PostgreSQL')
            engine = make_engine(args.engine,args.model_dir)
            result = run_session(settings,request,engine)
            print(json.dumps(result))
            return {'completed':0,'stopped':0,'no_data':3,'failed':2}[result['status']]
        if args.command == "doctor":
            credential_checks = settings.credential_checks()
            print(json.dumps({"version": __version__, "mode": "research/paper-only", "credentials_present": bool(settings.api_key and settings.api_secret),
                              "credential_checks": credential_checks,
                              "data_env": settings.data_env, "auth_mode": settings.auth_mode, "feed": settings.feed, "database_backend": "postgresql" if settings.database_url.startswith("postgresql://") else "sqlite",
                              "env_file_found": Path(args.env_file).is_file(), "env_file_path": str(Path(args.env_file).expanduser().resolve())}))
            return 2 if ((args.require_postgres and not settings.database_url.startswith('postgresql://')) or
                         (args.require_credentials and not credential_checks['format_valid'])) else 0
        if args.command == "verify-postgres":
            print(json.dumps(verify_postgres(settings)))
            return 0
        if args.command == 'prepare-finbert':
            print(json.dumps(prepare_finbert(args.model_dir)))
            return 0
        if args.command == 'verify-analysis':
            print(json.dumps(verify_analysis(make_engine(args.engine, args.model_dir))))
            return 0
        if args.command in {'analyze-news', 'analysis-summary', 'export-analysis-review'}:
            request = AnalysisRequest('alpaca-sandbox' if settings.data_env == 'sandbox' else 'alpaca',
                                      tuple(args.symbols.split(',')) if args.symbols else ())
            if args.command == 'analyze-news':
                engine = make_engine(args.engine, args.model_dir)
        if args.command == "stream-market-data":
            if args.test_feed and args.feed:
                raise DataValidationError("Use --test-feed without --feed")
            symbols = args.symbols.split(",") if args.symbols is not None else (("FAKEPACA",) if args.test_feed else UNIVERSE)
            channels = tuple(args.channels.split(",")) if args.channels is not None else (("bars", "quotes") if args.test_feed else ("bars", "updatedBars", "trades", "quotes"))
            request = StreamRequest(tuple(symbols), "test" if args.test_feed else (args.feed or settings.feed), channels, args.duration, args.max_events, args.max_reconnects)
            adapter = AlpacaStream(settings)
        if args.command == "collect-bars":
            request = BarsRequest(tuple(args.symbols.split(",")), utc_datetime(args.start), utc_datetime(args.end),
                                  args.timeframe, args.feed or settings.feed, args.adjustment, args.page_limit)
            adapter = AlpacaHistoricalData(settings)
        if args.command in {'data-quality','create-replay'}:
            request=ResearchRequest(source=args.source,symbols=tuple(args.symbols.split(',')) if args.symbols else (),
                feed=args.feed or settings.feed,timeframe=args.timeframe,
                start=utc_datetime(args.start) if args.start else None,end=utc_datetime(args.end) if args.end else None,order=args.order)
        if args.command=='collect-news':
            request=NewsRequest(tuple(args.symbols.split(',')),utc_datetime(args.start),utc_datetime(args.end),args.page_limit,args.max_pages)
            adapter=AlpacaNews(settings)
        if args.command == 'create-news-replay':
            request = NewsReplayRequest(args.as_of, tuple(args.symbols.split(',')) if args.symbols else (),
                'alpaca-sandbox' if settings.data_env == 'sandbox' else 'alpaca',args.first_seen_since)
        repository = Repository(settings.database_url)
        if args.command=='event-study':
            from .data.event_study import event_study,session_study,render_study
            if bool(args.analysis_replay_id)!=bool(args.bars_replay_id):
                raise DataValidationError('Supply analysis-replay-id and bars-replay-id together, or use a session')
            if args.output and Path(args.output).exists():raise DataValidationError('Event study output already exists; choose a new path')
            result=(event_study(repository,args.analysis_replay_id,args.bars_replay_id,end=args.end,limit=args.limit,_include_rows=bool(args.output)) if args.analysis_replay_id else
                    session_study(repository,args.session_id,end=args.end,limit=args.limit,_include_rows=bool(args.output)))
            if args.output:
                output=Path(args.output);output.parent.mkdir(parents=True,exist_ok=True)
                with output.open('x',encoding='utf-8') as file:file.write(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
            print(render_study(result) if args.format=='text' else json.dumps({k:v for k,v in result.items() if k!='event_rows'}));return 0
        if args.command == 'run-strategy-simulation':
            print(json.dumps(run_strategy(repository,args.run_id,args.analysis_replay_id,args.bars_replay_id,
                start=args.start,end=args.end,initial_cash=args.cash,fee_per_fill=args.fee_per_fill,
                slippage_bps=args.slippage_bps,max_position_fraction=args.max_position_fraction,
                entry_fraction=args.entry_fraction,max_signal_age_seconds=args.max_signal_age_seconds,
                max_price_age_seconds=args.max_price_age_seconds,max_news_age_seconds=args.max_news_age_seconds,
                risk=risk_policy(args.daily_loss_fraction,args.max_daily_entries,args.max_feed_gap_seconds)))); return 0
        if args.command == 'strategy-simulation-report':
            print(json.dumps(strategy_report(repository,args.run_id))); return 0
        if args.command == 'paper-create':
            print(json.dumps(create_account(repository,args.account,args.cash,args.start,
                fee_per_fill=args.fee_per_fill,slippage_bps=args.slippage_bps,
                max_position_fraction=args.max_position_fraction,max_mark_age_seconds=args.max_mark_age_seconds))); return 0
        if args.command in {'paper-fill','paper-mark'}:
            request = (fill_request(args.event_key,args.symbol,args.side,args.quantity,args.price,args.at)
                if args.command == 'paper-fill' else mark_request(args.event_key,args.symbol,args.price,args.at))
            print(json.dumps(append_event(repository,args.account,request))); return 0
        if args.command == 'paper-summary':
            print(json.dumps(account_summary(repository,args.account,args.at))); return 0
        if args.command == 'paper-report':
            print(json.dumps(period_report(repository,args.account,args.start,args.end))); return 0
        if args.command == 'explain-news-analysis':
            result=explain_analysis(repository,args.article_id,symbol=args.symbol,revision=args.revision_hash,
                analyzer_key=args.analyzer_key,engine=args.engine,
                provider='alpaca-sandbox' if settings.data_env=='sandbox' else 'alpaca')
            if args.output:
                path=Path(args.output);path.parent.mkdir(parents=True,exist_ok=True)
                if path.exists():raise DataValidationError('Explanation output already exists; choose a new filename')
                with path.open('x',encoding='utf-8') as file:file.write(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
                print(json.dumps({'status':'exported','output':str(path),'article_id':result['article_id'],
                    'analyzer_key':result['analyzer_key'],'database_modified':False,'inference_performed':False}))
            else:print(json.dumps(result))
            return 0
        if args.command == 'create-news-replay':
            print(json.dumps(create_news_replay(repository, request))); return 0
        if args.command == 'replay-news':
            print(json.dumps(replay_news(repository,args.replay_id,args.at))); return 0
        if args.command == 'research-session-summary':
            print(json.dumps(session_summary(repository,args.session_id))); return 0
        if args.command == 'research-alignment':
            print(json.dumps(research_alignment(repository,args.analysis_replay_id,args.bars_replay_id,args.at,args.max_lag_minutes))); return 0
        if args.command == 'create-analysis-replay':
            print(json.dumps(create_analysis_replay(repository,args.news_replay_id,args.as_of,args.analyzer_key,args.engine))); return 0
        if args.command == 'replay-analysis':
            print(json.dumps(replay_analysis(repository,args.replay_id,args.at))); return 0
        if args.command == 'export-analysis-review':
            print(json.dumps(export_review(repository, request, args.output, analyzer_key=args.analyzer_key, engine=args.engine)))
            return 0
        if args.command == "init-db":
            print(json.dumps({"schema": "R30", "event_study_available": True, "status": "ready", "report_evidence_available": True, "background_portfolio_observer_available": True, "session_latency_report_available": True, "analysis_format": "R18", "schema_change_required": False, "schema_changes": [], "paper_report_price_health_available": True, "automatic_portfolio_sampling_available": True, "portfolio_history_available": True, "paper_portfolio_available": True, "paper_session_report_available": True, "paper_worker_available": True, "paper_broker_tables_available": True, "strategy_simulation_table_available": True, "paper_ledger_available": True, "session_audit_table_available": True, "stream_bar_receipt_table_available": True}))
            return 0
        if args.command == 'analyze-news':
            result = analyze_news(repository, request, engine)
            print(json.dumps(result)); return 0 if result['status'] == 'completed' else 3
        if args.command == 'analysis-summary':
            print(json.dumps(analysis_summary(repository, request, analyzer_key=args.analyzer_key, engine=args.engine)))
            return 0
        if args.command=='collect-news':
            result=collect_news(adapter,repository,request)
            print(json.dumps(result));return 0 if result['status']=='completed' else 3
        if args.command=='news-summary':
            result=news_summary(repository,provider='alpaca-sandbox' if settings.data_env=='sandbox' else 'alpaca',
                symbols=tuple(args.symbols.split(',')) if args.symbols else ())
            print(json.dumps(result));return 0
        if args.command=='data-quality':
            _,result=inspect_dataset(repository,request)
            print(json.dumps(result))
            return 2 if result['status']=='failed' else (3 if result['status']=='warning' else 0)
        if args.command=='create-replay':
            print(json.dumps(create_replay(repository,request)));return 0
        if args.command=='replay':
            print(json.dumps(replay(repository,args.replay_id)));return 0
        if args.command=='verify-research':
            print(json.dumps(verify_research(repository,tuple(args.symbols.split(',')),args.feed or settings.feed)));return 0
        if args.command == 'recover-stream-gaps':
            result = recover_stream_gaps(settings, repository, args.run_id)
            print(json.dumps(result))
            return 0 if result['status'] == 'passed' else 3
        if args.command == "stream-market-data":
            result = adapter.run(request, repository,
                recovery=lambda run_id: recover_stream_gaps(settings, repository, run_id))
            print(json.dumps(result))
            return {"completed":0, "stopped":0, "no_data":3, "failed":2}[result["status"]]
        result = collect_historical(adapter, repository, request)
        print(json.dumps(result))
        return 0 if result["status"] == "completed" and not result["missing_symbols"] else 3
    except (ConfigurationError, DataValidationError, MarketDataError, StorageError, AnalysisError, PaperBrokerError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except (OSError, sqlite3.Error):
        print("Local storage/network operation failed; check permissions and configuration", file=sys.stderr)
        return 2
    except Exception:
        # DB driver errors must never expose a connection string or credentials.
        print("Collection failed; inspect local configuration and rerun the test suite", file=sys.stderr)
        return 2
    finally:
        if repository is not None:
            repository.close()

if __name__ == "__main__":
    raise SystemExit(main())
