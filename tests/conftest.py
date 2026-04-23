"""Shared test fixtures for slate-data-mirror.

Adds scripts/ to sys.path so tests can import fetch_all / bootstrap_history
/ slow_decide / healthcheck directly without a package layout.
"""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
_SCRIPTS = _REPO / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
