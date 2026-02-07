from __future__ import annotations

from pathlib import Path

import pandas as pd


def _read_programas(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name="Programas")


def test_commit_programas_replaces_and_archives(tmp_path: Path, monkeypatch):
    """
    Valida que el commit transaccional:
    - reemplace Programas.xlsx
    - archive el anterior en historico/
    - escriba FUENTE_DATOS en el archivo final
    """
    import etl.descargaSNIES as d

    out_dir = tmp_path / "outputs"
    hist_dir = out_dir / "historico"
    staging_dir = out_dir / "_staging"
    out_dir.mkdir(parents=True, exist_ok=True)
    hist_dir.mkdir(parents=True, exist_ok=True)
    staging_dir.mkdir(parents=True, exist_ok=True)

    # Patch globals del módulo para no tocar outputs reales
    monkeypatch.setattr(d, "DOWNLOAD_DIR", out_dir)
    monkeypatch.setattr(d, "HISTORIC_DIR", hist_dir)
    monkeypatch.setattr(d, "RENAME_TO", "Programas")

    # Programas.xlsx actual
    current = out_dir / "Programas.xlsx"
    df_current = pd.DataFrame([{"CÓDIGO_SNIES_DEL_PROGRAMA": "1"}])
    with pd.ExcelWriter(current, mode="w", engine="openpyxl") as writer:
        df_current.to_excel(writer, sheet_name="Programas", index=False)

    # Staged nuevo
    staged = staging_dir / "Programas.xlsx"
    df_new = pd.DataFrame([{"CÓDIGO_SNIES_DEL_PROGRAMA": "2"}])
    with pd.ExcelWriter(staged, mode="w", engine="openpyxl") as writer:
        df_new.to_excel(writer, sheet_name="Programas", index=False)

    result_path = d._commit_programas(staged, "WEB_SNIES")
    assert result_path is not None

    dest = out_dir / "Programas.xlsx"
    assert dest.exists()
    df_dest = _read_programas(dest)
    assert "FUENTE_DATOS" in df_dest.columns
    assert df_dest["FUENTE_DATOS"].iloc[0] == "WEB_SNIES"
    assert str(df_dest["CÓDIGO_SNIES_DEL_PROGRAMA"].iloc[0]) == "2"

    # staged debe haberse movido
    assert not staged.exists()

    # debe existir un histórico Programas_*.xlsx
    hist_files = list(hist_dir.glob("Programas_*.xlsx"))
    assert len(hist_files) >= 1

