import subprocess, sys


def test_cli_help():
    r = subprocess.run(
        [sys.executable, "-m", "pebble.cli", "--help"], capture_output=True, text=True
    )
    assert r.returncode == 0 and "Show this message and exit" in r.stdout
