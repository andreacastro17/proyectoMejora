"""
Configuración compartida para todos los tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest


@pytest.fixture()
def make_programas_xlsx(tmp_path: Path):
    """
    Crea un Programas.xlsx mínimo con hoja 'Programas'.
    """

    def _make(path: Path, rows: list[dict[str, Any]]) -> Path:
        df = pd.DataFrame(rows)
        path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(path, mode="w", engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Programas", index=False)
        return path

    return _make


@pytest.fixture(autouse=True)
def skip_slow_tests_if_requested(request):
    """
    Permite saltar tests lentos con --skip-slow.
    """
    if request.config.getoption("--skip-slow", default=False):
        if "slow" in request.keywords:
            pytest.skip("Test marcado como lento (usa --skip-slow para saltarlo)")


def pytest_addoption(parser):
    """Agrega opciones de línea de comandos para pytest."""
    parser.addoption(
        "--skip-slow",
        action="store_true",
        default=False,
        help="Saltar tests marcados como 'slow'",
    )
