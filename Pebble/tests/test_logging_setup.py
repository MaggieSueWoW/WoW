import json
import sys
from pathlib import Path

# Ensure the project root is on the path for imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

from pebble.logging_setup import setup_logging


def test_extra_fields_are_logged(capfd):
    log = setup_logging()
    log.info("hello", extra={"stage": "demo"})
    log.handlers[0].flush()
    captured = capfd.readouterr()
    line = captured.out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["msg"] == "hello"
    assert payload["stage"] == "demo"
