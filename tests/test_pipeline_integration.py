"""
Test de integración: normalización + procesamiento de programas nuevos.

Ejecuta un flujo mínimo sin descarga SNIES ni clasificación ML, sobre datos fixture.
Verifica que el archivo resultante tenga las columnas esperadas.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("unidecode")


def test_normalizacion_y_procesamiento_nuevos(tmp_path: Path, make_programas_xlsx, monkeypatch):
    """Normalización + procesamiento de programas nuevos en secuencia; verifica columnas de salida."""
    from etl import config
    from etl import normalizacion as norm
    from etl import procesamientoSNIES as proc

    # Estructura temporal
    out_dir = tmp_path / "outputs"
    hist_dir = out_dir / "historico"
    out_dir.mkdir(parents=True, exist_ok=True)
    hist_dir.mkdir(parents=True, exist_ok=True)
    current = out_dir / "Programas.xlsx"
    historic = hist_dir / "Programas_20260101_120000.xlsx"

    # Columnas mínimas para normalización y procesamiento
    def row(codigo: str, nombre: str, inst: str = "Inst", nivel: str = "Pregrado") -> dict:
        return {
            "CÓDIGO_SNIES_DEL_PROGRAMA": codigo,
            "NOMBRE_DEL_PROGRAMA": nombre,
            "NOMBRE_INSTITUCIÓN": inst,
            "NIVEL_DE_FORMACIÓN": nivel,
        }
    rows_historic = [row("1", "Prog A"), row("2", "Prog B")]
    rows_current = [row("1", "Prog A"), row("2", "Prog B"), row("3", "Prog C nuevo")]

    make_programas_xlsx(historic, rows_historic)
    make_programas_xlsx(current, rows_current)

    monkeypatch.setattr(config, "ARCHIVO_PROGRAMAS", current)
    monkeypatch.setattr(norm, "ARCHIVO_PROGRAMAS", current)
    monkeypatch.setattr(proc, "ARCHIVO_PROGRAMAS", current)
    monkeypatch.setattr(proc, "HISTORIC_DIR", hist_dir)

    norm.normalizar_programas()
    df_after_norm = pd.read_excel(current, sheet_name="Programas")
    assert len(df_after_norm) >= 1
    assert "NOMBRE_DEL_PROGRAMA" in df_after_norm.columns

    proc.procesar_programas_nuevos()
    df_out = pd.read_excel(current, sheet_name="Programas")
    assert "PROGRAMA_NUEVO" in df_out.columns
    assert "CÓDIGO_SNIES_DEL_PROGRAMA" in df_out.columns
    nuevos = df_out[df_out["PROGRAMA_NUEVO"] == "Sí"]
    assert len(nuevos) >= 1
