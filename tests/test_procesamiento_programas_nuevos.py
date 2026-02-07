from __future__ import annotations

from pathlib import Path

import pandas as pd


def test_procesar_programas_nuevos_marks_new(tmp_path: Path, make_programas_xlsx, monkeypatch):
    import etl.procesamientoSNIES as p

    out_dir = tmp_path / "outputs"
    hist_dir = out_dir / "historico"
    out_dir.mkdir(parents=True, exist_ok=True)
    hist_dir.mkdir(parents=True, exist_ok=True)

    current = out_dir / "Programas.xlsx"
    historic = hist_dir / "Programas_20260101_000000.xlsx"

    make_programas_xlsx(
        historic,
        [
            {"CÓDIGO_SNIES_DEL_PROGRAMA": "1", "NOMBRE_DEL_PROGRAMA": "a"},
            {"CÓDIGO_SNIES_DEL_PROGRAMA": "2", "NOMBRE_DEL_PROGRAMA": "b"},
        ],
    )
    make_programas_xlsx(
        current,
        [
            {"CÓDIGO_SNIES_DEL_PROGRAMA": "1", "NOMBRE_DEL_PROGRAMA": "a"},
            {"CÓDIGO_SNIES_DEL_PROGRAMA": "2", "NOMBRE_DEL_PROGRAMA": "b"},
            {"CÓDIGO_SNIES_DEL_PROGRAMA": "3", "NOMBRE_DEL_PROGRAMA": "c"},
            {"CÓDIGO_SNIES_DEL_PROGRAMA": None, "NOMBRE_DEL_PROGRAMA": "nota"},
        ],
    )

    # Patch módulo para usar rutas temporales
    monkeypatch.setattr(p, "ARCHIVO_PROGRAMAS", current)
    monkeypatch.setattr(p, "HISTORIC_DIR", hist_dir)
    monkeypatch.setattr(p, "HOJA_PROGRAMAS", "Programas")

    p.procesar_programas_nuevos()

    df_out = pd.read_excel(current, sheet_name="Programas")
    assert "PROGRAMA_NUEVO" in df_out.columns
    # Debe haber "Sí" para el código 3
    mask_3 = df_out["CÓDIGO_SNIES_DEL_PROGRAMA"].astype(str).str.replace(".0", "", regex=False) == "3"
    assert (df_out.loc[mask_3, "PROGRAMA_NUEVO"] == "Sí").any()

