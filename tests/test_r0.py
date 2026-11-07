import contextlib
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from microbroker.config import Settings, ConfigurationError, UNIVERSE
from microbroker.data.models import utc_datetime, DataValidationError, validate_symbols
from microbroker.cli import main

class R0Tests(unittest.TestCase):
    def test_secret_repr(self):
        s = Settings(api_key="sentinel-key", api_secret="sentinel-secret", database_url="postgresql://user:password@localhost/db")
        self.assertNotIn("sentinel", repr(s))
        self.assertNotIn("password", repr(s))
    def test_env_priority(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"ALPACA_API_KEY": "process"}, clear=True):
            p = Path(tmp)/".env"
            p.write_text('ALPACA_API_KEY=file\nALPACA_API_SECRET="local secret"\n')
            s = Settings.from_env(p)
            self.assertEqual(s.api_key, "process")
            self.assertEqual(s.api_secret, "local secret")
    def test_invalid_config(self):
        with self.assertRaises(ConfigurationError): Settings(timeout=0)
        with self.assertRaises(ConfigurationError): Settings(data_env="sandbox")
        with self.assertRaises(ConfigurationError): Settings().require_credentials()
    def test_utc(self):
        self.assertEqual(utc_datetime("2026-01-01T03:00:00+03:00").hour, 0)
        with self.assertRaises(DataValidationError): utc_datetime("2026-01-01")
    def test_symbols(self):
        self.assertEqual(validate_symbols(["aapl", "AAPL"]), ("AAPL",))
        self.assertEqual(len(UNIVERSE), 12)
        with self.assertRaises(DataValidationError): validate_symbols([""])
    def test_doctor(self):
        with patch.dict(os.environ, {}, clear=True), contextlib.redirect_stdout(io.StringIO()) as out:
            self.assertEqual(main(["--env-file", "/nonexistent", "doctor"]), 0)
            self.assertIn('"credentials_present": false', out.getvalue())
