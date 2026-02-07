from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.main import validate_programas_schema


def test_validate_programas_schema_ok(tmp_path: Path):
    xlsx = tmp_path / "Programas.xlsx"
    df = pd.DataFrame(
        [
            {
                "CÓDIGO_SNIES_DEL_PROGRAMA": "123",
                "NOMBRE_DEL_PROGRAMA": "ingenieria",
                "NOMBRE_INSTITUCIÓN": "eafit",
                "NIVEL_DE_FORMACIÓN": "universitario",
            }
        ]
    )
    with pd.ExcelWriter(xlsx, mode="w", engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Programas", index=False)

    ok, msg = validate_programas_schema(xlsx)
    assert ok is True
    assert msg == "OK"


def test_validate_programas_schema_missing_columns(tmp_path: Path):
    xlsx = tmp_path / "Programas.xlsx"
    df = pd.DataFrame([{"CÓDIGO_SNIES_DEL_PROGRAMA": "123"}])
    with pd.ExcelWriter(xlsx, mode="w", engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Programas", index=False)

    ok, msg = validate_programas_schema(xlsx)
    assert ok is False
    assert "Faltan:" in msg

