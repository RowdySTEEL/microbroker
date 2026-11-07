# Validation status

Portfolio package verified on 7 November 2026 with Python 3.12.14 on Linux.

| Check | Result |
| --- | --- |
| Build and install from the source directory | Passed; the installed `microbroker` command ran from a separate working directory |
| `python -m unittest discover -s tests` | 1,001 passed |
| `microbroker verify-event-study` | 19 synthetic checks passed |
| `microbroker demo-event-study --output event-study` | 3 eligible company events, 12 SPY-matched horizon results, zero orders |
| Real PostgreSQL 16 on the R30 portfolio package | Pending; the CI workflow defines a separate service-backed job |
| R30 live Alpaca market-data collection with local FinBERT | Pending |

The regression suite includes mocked PostgreSQL protocol checks; these are separate from running against a PostgreSQL server. The synthetic demo verifies report behavior, not financial performance.

The application source is the R30 delivery (0.30.0). Publication preparation adds documentation, repository exclusions and CI, and removes personal metadata from a saved warning-history test fixture. Historical acceptance transcripts, local configuration, models and database files are not part of this distribution.
