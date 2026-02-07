"""
Tests para los filtros en la página de ajuste manual.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


@pytest.mark.gui
@pytest.mark.slow
def test_filtro_solo_nuevos_muestra_todos_los_nuevos(tmp_path: Path):
    """
    Verifica que el filtro SOLO_NUEVOS muestra todos los programas nuevos,
    incluso con diferentes formatos de "Sí".
    """
    from app.main import ManualReviewPage
    import tkinter as tk

    # Crear archivo de programas con diferentes formatos
    programas = tmp_path / "Programas.xlsx"
    df = pd.DataFrame([
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
            "NOMBRE_DEL_PROGRAMA": "Programa 1",
            "NOMBRE_INSTITUCIÓN": "Inst 1",
            "NIVEL_DE_FORMACIÓN": "universitario",
            "PROGRAMA_NUEVO": "Sí",  # Formato correcto
            "ES_REFERENTE": "No",
        },
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "2",
            "NOMBRE_DEL_PROGRAMA": "Programa 2",
            "NOMBRE_INSTITUCIÓN": "Inst 2",
            "NIVEL_DE_FORMACIÓN": "universitario",
            "PROGRAMA_NUEVO": "Sí ",  # Con espacio
            "ES_REFERENTE": "No",
        },
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "3",
            "NOMBRE_DEL_PROGRAMA": "Programa 3",
            "NOMBRE_INSTITUCIÓN": "Inst 3",
            "NIVEL_DE_FORMACIÓN": "universitario",
            "PROGRAMA_NUEVO": " Sí",  # Con espacio al inicio
            "ES_REFERENTE": "No",
        },
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "4",
            "NOMBRE_DEL_PROGRAMA": "Programa 4",
            "NOMBRE_INSTITUCIÓN": "Inst 4",
            "NIVEL_DE_FORMACIÓN": "universitario",
            "PROGRAMA_NUEVO": "No",  # No es nuevo
            "ES_REFERENTE": "No",
        },
    ])
    
    with pd.ExcelWriter(programas, mode="w", engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Programas", index=False)

    # Crear ventana temporal para la página
    root = tk.Tk()
    root.withdraw()  # Ocultar ventana
    
    try:
        # Crear página de ajuste manual
        page = ManualReviewPage(root, on_back=None)
        
        # Patch el file_path para usar nuestro archivo temporal
        page.file_path = programas
        
        # Cargar datos
        page._load()
        
        # Verificar que se cargaron los datos
        assert page.df_view is not None
        assert len(page.df_view) == 4
        
        # Aplicar filtro SOLO_NUEVOS
        page.filter_var.set("SOLO_NUEVOS")
        page._apply_filter()
        
        # Verificar que se filtraron correctamente (debe mostrar 3 programas nuevos)
        assert page._filtered_df is not None
        assert len(page._filtered_df) == 3
        
        # Verificar que todos tienen PROGRAMA_NUEVO = "Sí" (normalizado)
        for _, row in page._filtered_df.iterrows():
            assert str(row["PROGRAMA_NUEVO"]).strip().upper() == "SÍ"
            
    finally:
        root.destroy()


@pytest.mark.gui
@pytest.mark.slow
def test_filtro_solo_referentes_funciona_correctamente(tmp_path: Path):
    """
    Verifica que el filtro SOLO_REFERENTES muestra solo los programas referentes.
    """
    from app.main import ManualReviewPage
    import tkinter as tk

    # Crear archivo de programas
    programas = tmp_path / "Programas.xlsx"
    df = pd.DataFrame([
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
            "NOMBRE_DEL_PROGRAMA": "Programa 1",
            "NOMBRE_INSTITUCIÓN": "Inst 1",
            "NIVEL_DE_FORMACIÓN": "universitario",
            "PROGRAMA_NUEVO": "Sí",
            "ES_REFERENTE": "Sí",
            "PROBABILIDAD": 0.8,
            "PROGRAMA_EAFIT_CODIGO": "E1",
            "PROGRAMA_EAFIT_NOMBRE": "Programa EAFIT",
        },
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "2",
            "NOMBRE_DEL_PROGRAMA": "Programa 2",
            "NOMBRE_INSTITUCIÓN": "Inst 2",
            "NIVEL_DE_FORMACIÓN": "universitario",
            "PROGRAMA_NUEVO": "Sí",
            "ES_REFERENTE": "No",
            "PROBABILIDAD": 0.2,
            "PROGRAMA_EAFIT_CODIGO": "",
            "PROGRAMA_EAFIT_NOMBRE": "",
        },
    ])
    
    with pd.ExcelWriter(programas, mode="w", engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Programas", index=False)

    root = tk.Tk()
    root.withdraw()
    
    try:
        page = ManualReviewPage(root, on_back=None)
        page.file_path = programas
        page._load()
        
        # Aplicar filtro SOLO_REFERENTES
        page.filter_var.set("SOLO_REFERENTES")
        page._apply_filter()
        
        # Verificar que solo muestra referentes
        assert page._filtered_df is not None
        assert len(page._filtered_df) == 1
        assert page._filtered_df.iloc[0]["ES_REFERENTE"] == "Sí"
        
    finally:
        root.destroy()


@pytest.mark.gui
@pytest.mark.slow
def test_filtro_todos_muestra_todos_los_programas(tmp_path: Path):
    """
    Verifica que el filtro TODOS muestra todos los programas sin filtrar.
    """
    from app.main import ManualReviewPage
    import tkinter as tk

    # Crear archivo de programas
    programas = tmp_path / "Programas.xlsx"
    df = pd.DataFrame([
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
            "NOMBRE_DEL_PROGRAMA": "Programa 1",
            "NOMBRE_INSTITUCIÓN": "Inst 1",
            "NIVEL_DE_FORMACIÓN": "universitario",
            "PROGRAMA_NUEVO": "Sí",
            "ES_REFERENTE": "Sí",
        },
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "2",
            "NOMBRE_DEL_PROGRAMA": "Programa 2",
            "NOMBRE_INSTITUCIÓN": "Inst 2",
            "NIVEL_DE_FORMACIÓN": "universitario",
            "PROGRAMA_NUEVO": "No",
            "ES_REFERENTE": "No",
        },
    ])
    
    with pd.ExcelWriter(programas, mode="w", engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Programas", index=False)

    root = tk.Tk()
    root.withdraw()
    
    try:
        page = ManualReviewPage(root, on_back=None)
        page.file_path = programas
        page._load()
        
        # Aplicar filtro TODOS
        page.filter_var.set("TODOS")
        page._apply_filter()
        
        # Verificar que muestra todos
        assert page._filtered_df is not None
        assert len(page._filtered_df) == 2
        
    finally:
        root.destroy()


@pytest.mark.gui
@pytest.mark.slow
def test_busqueda_por_texto_funciona(tmp_path: Path):
    """
    Verifica que la búsqueda por texto funciona correctamente.
    """
    from app.main import ManualReviewPage
    import tkinter as tk

    # Crear archivo de programas
    programas = tmp_path / "Programas.xlsx"
    df = pd.DataFrame([
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
            "NOMBRE_DEL_PROGRAMA": "Ingeniería de Sistemas",
            "NOMBRE_INSTITUCIÓN": "Universidad Test",
            "NIVEL_DE_FORMACIÓN": "universitario",
            "PROGRAMA_NUEVO": "Sí",
            "ES_REFERENTE": "No",
        },
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "2",
            "NOMBRE_DEL_PROGRAMA": "Medicina",
            "NOMBRE_INSTITUCIÓN": "Universidad Test",
            "NIVEL_DE_FORMACIÓN": "universitario",
            "PROGRAMA_NUEVO": "Sí",
            "ES_REFERENTE": "No",
        },
    ])
    
    with pd.ExcelWriter(programas, mode="w", engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Programas", index=False)

    root = tk.Tk()
    root.withdraw()
    
    try:
        page = ManualReviewPage(root, on_back=None)
        page.file_path = programas
        page._load()
        
        # Buscar por "sistemas"
        page.search_var.set("sistemas")
        page._apply_filter()
        
        # Verificar que solo muestra el programa de sistemas
        assert page._filtered_df is not None
        assert len(page._filtered_df) == 1
        assert "Sistemas" in page._filtered_df.iloc[0]["NOMBRE_DEL_PROGRAMA"]
        
    finally:
        root.destroy()
