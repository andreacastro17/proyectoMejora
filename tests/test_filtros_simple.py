"""
Tests rápidos para los filtros sin GUI (solo lógica).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest


def test_filtro_solo_nuevos_logica(tmp_path: Path):
    """
    Test rápido que verifica la lógica del filtro SOLO_NUEVOS sin GUI.
    """
    # Simular DataFrame como lo haría ManualReviewPage
    df = pd.DataFrame([
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
            "NOMBRE_DEL_PROGRAMA": "Programa 1",
            "PROGRAMA_NUEVO": "Sí",
        },
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "2",
            "NOMBRE_DEL_PROGRAMA": "Programa 2",
            "PROGRAMA_NUEVO": "Sí ",  # Con espacio
        },
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "3",
            "NOMBRE_DEL_PROGRAMA": "Programa 3",
            "PROGRAMA_NUEVO": "No",
        },
    ])
    
    # Aplicar filtro como lo hace _apply_filter
    df_filtered = df[df["PROGRAMA_NUEVO"].astype(str).str.strip().str.upper() == "SÍ"]
    
    # Verificar que solo muestra programas nuevos
    assert len(df_filtered) == 2
    assert all(df_filtered["PROGRAMA_NUEVO"].astype(str).str.strip().str.upper() == "SÍ")


def test_filtro_solo_referentes_logica(tmp_path: Path):
    """
    Test rápido que verifica la lógica del filtro SOLO_REFERENTES sin GUI.
    """
    df = pd.DataFrame([
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
            "ES_REFERENTE": "Sí",
        },
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "2",
            "ES_REFERENTE": "No",
        },
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "3",
            "ES_REFERENTE": " Sí",  # Con espacio
        },
    ])
    
    # Aplicar filtro
    df_filtered = df[df["ES_REFERENTE"].astype(str).str.strip().str.upper() == "SÍ"]
    
    # Verificar que solo muestra referentes
    assert len(df_filtered) == 2
    assert all(df_filtered["ES_REFERENTE"].astype(str).str.strip().str.upper() == "SÍ")


def test_busqueda_por_texto_logica(tmp_path: Path):
    """
    Test rápido que verifica la lógica de búsqueda sin GUI.
    """
    df = pd.DataFrame([
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "1",
            "NOMBRE_DEL_PROGRAMA": "Ingeniería de Sistemas",
            "NOMBRE_INSTITUCIÓN": "Universidad Test",
        },
        {
            "CÓDIGO_SNIES_DEL_PROGRAMA": "2",
            "NOMBRE_DEL_PROGRAMA": "Medicina",
            "NOMBRE_INSTITUCIÓN": "Universidad Test",
        },
    ])
    
    # Buscar por "sistemas"
    q = "sistemas"
    mask = (
        df["CÓDIGO_SNIES_DEL_PROGRAMA"].astype(str).str.lower().str.contains(q, na=False)
        | df["NOMBRE_DEL_PROGRAMA"].astype(str).str.lower().str.contains(q, na=False)
        | df["NOMBRE_INSTITUCIÓN"].astype(str).str.lower().str.contains(q, na=False)
    )
    df_filtered = df[mask]
    
    # Verificar que solo muestra el programa de sistemas
    assert len(df_filtered) == 1
    assert "Sistemas" in df_filtered.iloc[0]["NOMBRE_DEL_PROGRAMA"]
