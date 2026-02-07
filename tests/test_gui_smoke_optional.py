from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.gui
def test_manual_review_window_opens(tmp_path: Path, monkeypatch):
    """
    Smoke test GUI: crea ventana y la destruye.
    Nota: puede fallar en entornos sin GUI (por eso está marcada como gui).
    """
    import tkinter as tk

    import app.main as m

    # Patch Programas.xlsx a un tmp para evitar tocar outputs real
    out_dir = tmp_path / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    programas = out_dir / "Programas.xlsx"
    # Crear un Excel mínimo
    import pandas as pd

    df = pd.DataFrame(
        [
            {
                "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
                "NOMBRE_INSTITUCIÓN": "x",
                "NOMBRE_DEL_PROGRAMA": "a",
                "NIVEL_DE_FORMACIÓN": "universitario",
                "PROGRAMA_NUEVO": "Sí",
            }
        ]
    )
    with pd.ExcelWriter(programas, mode="w", engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Programas", index=False)

    monkeypatch.setattr(m, "ARCHIVO_PROGRAMAS", programas)

    root = tk.Tk()
    root.withdraw()
    win = m.ManualReviewWindow(root)
    win.update_idletasks()
    win.destroy()
    root.destroy()

