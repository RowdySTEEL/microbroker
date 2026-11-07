import json
import logging
from datetime import datetime, timezone

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({"time": datetime.now(timezone.utc).isoformat(), "level": record.levelname,
                           "logger": record.name, "message": record.getMessage()}, ensure_ascii=False)

def configure_logging(level="INFO"):
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("microbroker")
    logger.handlers = [handler]
    logger.setLevel(level)
    logger.propagate = False
