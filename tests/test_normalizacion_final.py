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


def test_aplicar_normalizacion_final_preserva_formato_human_readable(tmp_path: Path, make_programas_xlsx, monkeypatch):
    """
    Verifica que la normalización final RESTAURA el formato (mayúsculas, tildes)
    y NO aplica lower/unidecode después del mapeo.
    Si el mapeo dice 'ingenieria' -> 'Ingeniería', el resultado debe ser 'Ingeniería'.
    """
    import etl.normalizacion_final as nf

    out_dir = tmp_path / "outputs"
    docs_dir = tmp_path / "docs"
    out_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)

    programas = out_dir / "Programas.xlsx"
    normalizacion = docs_dir / "normalizacionFinal.xlsx"

    # Datos en "estilo ML" (minúsculas, sin tildes)
    make_programas_xlsx(
        programas,
        [
            {
                "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
                "CÓDIGO_INSTITUCIÓN_PADRE": "1714",
                "NOMBRE_INSTITUCIÓN": "x",
                "NOMBRE_DEL_PROGRAMA": "ingenieria de sistemas",
            }
        ],
    )

    # Mapeo que restaura formato human-readable: mayúsculas y tildes
    prog = pd.DataFrame({
        "actual": ["ingenieria de sistemas"],
        "normalizado": ["Ingeniería de Sistemas"],
    })
    inst = pd.DataFrame({
        "CÓDIGO_INSTITUCIÓN_PADRE": ["1714"],
        "NOMBRE_INSTITUCIÓN": ["Universidad EAFIT"],
    })
    with pd.ExcelWriter(normalizacion, mode="w", engine="openpyxl") as writer:
        prog.to_excel(writer, sheet_name="NOMBRE_DEL_PROGRAMA", index=False)
        inst.to_excel(writer, sheet_name="NOMBRE_INSTITUCIÓN", index=False)

    monkeypatch.setattr(nf, "ARCHIVO_PROGRAMAS", programas)
    monkeypatch.setattr(nf, "ARCHIVO_NORMALIZACION", normalizacion)
    monkeypatch.setattr(nf, "HOJA_PROGRAMAS", "Programas")

    nf.aplicar_normalizacion_final()

    df_out = pd.read_excel(programas, sheet_name="Programas")
    # Debe quedar exactamente como en el mapeo (formato restaurado), NO en minúsculas
    assert df_out.loc[0, "NOMBRE_DEL_PROGRAMA"] == "Ingeniería de Sistemas"
    assert df_out.loc[0, "NOMBRE_INSTITUCIÓN"] == "Universidad EAFIT"

