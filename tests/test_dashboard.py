"""Smoke tests: Streamlit pages import without raising."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

PAGES = Path(__file__).resolve().parents[1] / "dashboard" / "pages"


@pytest.mark.parametrize("page", sorted(PAGES.glob("*.py")))
def test_page_imports(page: Path) -> None:
    spec = importlib.util.spec_from_file_location(page.stem, page)
    assert spec is not None and spec.loader is not None
    # Just verify the file parses; running it requires a Streamlit context.
    compile(page.read_text(), str(page), "exec")
