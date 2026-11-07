"""Configuration without printing secrets. .env supports literal KEY=VALUE only."""
from dataclasses import dataclass, field
import os
from pathlib import Path

UNIVERSE = ("AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AMD", "NFLX", "AVGO", "SPY", "QQQ")

class ConfigurationError(ValueError):
    pass

def load_env(path: Path) -> dict[str, str]:
    # No shell execution, interpolation or multiline secrets. Process env wins.
    values = {}
    if path.exists():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, sep, value = line.partition("=")
            key, value = key.strip(), value.strip()
            if not sep or not key.isidentifier():
                raise ConfigurationError(f"Invalid .env assignment at line {number}")
            if value.startswith(('"', "'")):
                if len(value) < 2 or value[-1] != value[0]:
                    raise ConfigurationError(f"Invalid .env quoting at line {number}")
                value = value[1:-1]
            values[key] = value
    return values

@dataclass(frozen=True)
class Settings:
    api_key: str = field(default="", repr=False)
    api_secret: str = field(default="", repr=False)
    auth_mode: str = "trading"
    data_env: str = "production"
    feed: str = "iex"
    database_url: str = field(default="sqlite:///data/microbroker.db", repr=False)
    log_level: str = "INFO"
    timeout: float = 30
    max_retries: int = 3

    def __post_init__(self):
        if self.auth_mode not in {"trading", "broker"}:
            raise ConfigurationError("ALPACA_AUTH_MODE must be trading or broker")
        if self.data_env not in {"production", "sandbox"}:
            raise ConfigurationError("ALPACA_DATA_ENV must be production or sandbox")
        if self.auth_mode == "trading" and self.data_env == "sandbox":
            raise ConfigurationError("Broker sandbox requires ALPACA_AUTH_MODE=broker")
        if self.feed not in {"iex", "sip"}:
            raise ConfigurationError("ALPACA_DATA_FEED must be iex or sip")
        if self.log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ConfigurationError("Invalid LOG_LEVEL")
        if not 0 < self.timeout <= 120 or not 0 <= self.max_retries <= 10:
            raise ConfigurationError("HTTP timeout/retry settings outside supported range")
        if not self.database_url.startswith(("sqlite:///", "postgresql://")):
            raise ConfigurationError("DATABASE_URL must use sqlite:/// or postgresql://")

    @property
    def data_url(self):
        return "https://data.sandbox.alpaca.markets" if self.data_env == "sandbox" else "https://data.alpaca.markets"

    def require_credentials(self):
        if not self.api_key or not self.api_secret:
            raise ConfigurationError("Set ALPACA_API_KEY and ALPACA_API_SECRET in your local .env")
        for name, value in (("ALPACA_API_KEY", self.api_key), ("ALPACA_API_SECRET", self.api_secret)):
            if not isinstance(value,str) or any(not 33 <= ord(char) <= 126 for char in value):
                raise ConfigurationError(name + " contains non-ASCII, whitespace or control characters; recopy the original value into .env")

    def credential_checks(self):
        """Local syntax only: never reveal values or claim provider authentication."""
        fields={}
        for name,value in (("ALPACA_API_KEY",self.api_key),("ALPACA_API_SECRET",self.api_secret)):
            present=isinstance(value,str) and bool(value)
            fields[name]={"present":present,
                "format_valid":present and all(33<=ord(char)<=126 for char in value)}
        return {"format_valid":all(item["format_valid"] for item in fields.values()),
                "fields":fields,"provider_authentication_checked":False}

    @classmethod
    def from_env(cls, path=Path(".env")):
        values = load_env(Path(path))
        def get(key, default):
            return os.environ.get(key, values.get(key, default))
        try:
            return cls(api_key=get("ALPACA_API_KEY", ""), api_secret=get("ALPACA_API_SECRET", ""),
                       auth_mode=get("ALPACA_AUTH_MODE", "trading"), data_env=get("ALPACA_DATA_ENV", "production"),
                       feed=get("ALPACA_DATA_FEED", "iex"), database_url=get("DATABASE_URL", "sqlite:///data/microbroker.db"),
                       log_level=get("LOG_LEVEL", "INFO").upper(), timeout=float(get("HTTP_TIMEOUT_SECONDS", "30")),
                       max_retries=int(get("HTTP_MAX_RETRIES", "3")))
        except (TypeError, ValueError) as exc:
            if isinstance(exc, ConfigurationError):
                raise
            raise ConfigurationError("Invalid numeric configuration") from None
