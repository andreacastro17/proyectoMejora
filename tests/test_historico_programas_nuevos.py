from __future__ import annotations

from pathlib import Path

import pandas as pd


def _write_programas(path: Path, rows: list[dict]) -> None:
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, mode="w", engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Programas", index=False)


def test_actualizar_historico_no_nuevos(tmp_path: Path, monkeypatch):
    import etl.historicoProgramasNuevos as h

    out_dir = tmp_path / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    programas = out_dir / "Programas.xlsx"
    historico = out_dir / "HistoricoProgramasNuevos.xlsx"

    _write_programas(
        programas,
        [
            {
                "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
                "NOMBRE_INSTITUCIÓN": "x",
                "NOMBRE_DEL_PROGRAMA": "a",
                "PROGRAMA_NUEVO": "No",
                "ES_REFERENTE": "No",
                "PROGRAMA_EAFIT_CODIGO": "",
                "PROGRAMA_EAFIT_NOMBRE": "",
            }
        ],
    )

    monkeypatch.setattr(h, "ARCHIVO_PROGRAMAS", programas)
    monkeypatch.setattr(h, "ARCHIVO_HISTORICO", historico)
    monkeypatch.setattr(h, "HOJA_PROGRAMAS", "Programas")
    monkeypatch.setattr(h, "HOJA_HISTORICO", "ProgramasNuevos")

    h.actualizar_historico_programas_nuevos()
    assert not historico.exists()


def test_actualizar_historico_crea_archivo(tmp_path: Path, monkeypatch):
    import etl.historicoProgramasNuevos as h

    out_dir = tmp_path / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    programas = out_dir / "Programas.xlsx"
    historico = out_dir / "HistoricoProgramasNuevos.xlsx"

    _write_programas(
        programas,
        [
            {
                "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
                "NOMBRE_INSTITUCIÓN": "x",
                "NOMBRE_DEL_PROGRAMA": "a",
                "PROGRAMA_NUEVO": "Sí",
                "ES_REFERENTE": "No",
                "PROGRAMA_EAFIT_CODIGO": "",
                "PROGRAMA_EAFIT_NOMBRE": "",
            }
        ],
    )

    monkeypatch.setattr(h, "ARCHIVO_PROGRAMAS", programas)
    monkeypatch.setattr(h, "ARCHIVO_HISTORICO", historico)
    monkeypatch.setattr(h, "HOJA_PROGRAMAS", "Programas")
    monkeypatch.setattr(h, "HOJA_HISTORICO", "ProgramasNuevos")

    h.actualizar_historico_programas_nuevos()
    assert historico.exists()

