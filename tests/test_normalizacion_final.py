from __future__ import annotations

from pathlib import Path

import pandas as pd


def _write_normalizacion_final(path: Path) -> None:
    """
    Crea un normalizacionFinal.xlsx mínimo:
    - Hoja NOMBRE_DEL_PROGRAMA: mapea 'ing sistemas' -> 'ingenieria de sistemas'
    - Hoja NOMBRE_INSTITUCIÓN: mapea CÓDIGO_INSTITUCIÓN_PADRE '1714' -> 'COLEGIO MAYOR...'
    """
    prog = pd.DataFrame({"actual": ["ing sistemas"], "normalizado": ["ingenieria de sistemas"]})
    inst = pd.DataFrame({"CÓDIGO_INSTITUCIÓN_PADRE": ["1714"], "NOMBRE_INSTITUCIÓN": ["COLEGIO MAYOR..."]})
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, mode="w", engine="openpyxl") as writer:
        prog.to_excel(writer, sheet_name="NOMBRE_DEL_PROGRAMA", index=False)
        inst.to_excel(writer, sheet_name="NOMBRE_INSTITUCIÓN", index=False)


def test_aplicar_normalizacion_final_basic(tmp_path: Path, make_programas_xlsx, monkeypatch):
    import etl.normalizacion_final as nf

    out_dir = tmp_path / "outputs"
    docs_dir = tmp_path / "docs"
    out_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)

    programas = out_dir / "Programas.xlsx"
    normalizacion = docs_dir / "normalizacionFinal.xlsx"

    make_programas_xlsx(
        programas,
        [
            {
                "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
                "CÓDIGO_INSTITUCIÓN_PADRE": "1714",
                "NOMBRE_INSTITUCIÓN": "x",
                "NOMBRE_DEL_PROGRAMA": "ing sistemas",
            }
        ],
    )
    _write_normalizacion_final(normalizacion)

    monkeypatch.setattr(nf, "ARCHIVO_PROGRAMAS", programas)
    monkeypatch.setattr(nf, "ARCHIVO_NORMALIZACION", normalizacion)
    monkeypatch.setattr(nf, "HOJA_PROGRAMAS", "Programas")

    nf.aplicar_normalizacion_final()

    df_out = pd.read_excel(programas, sheet_name="Programas")
    assert df_out.loc[0, "NOMBRE_DEL_PROGRAMA"] == "ingenieria de sistemas"
    assert df_out.loc[0, "NOMBRE_INSTITUCIÓN"] == "COLEGIO MAYOR..."

