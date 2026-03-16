"""
Tests unitarios para etl.scoring.apply_scoring() (Fase 4 pipeline mercado).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from etl.scoring import SCORING_CONFIG, apply_scoring


def test_pesos_suman_uno():
    """Los pesos en SCORING_CONFIG deben sumar exactamente 1.0."""
    total = sum(c["peso"] for c in SCORING_CONFIG)
    assert total == pytest.approx(1.0, abs=1e-9), f"Pesos suman {total}, se esperaba 1.0"


def _df_base() -> pd.DataFrame:
    """DataFrame mínimo con todas las columnas que usa apply_scoring."""
    return pd.DataFrame({
        "suma_matricula_2024": [0.0],
        "participacion_2024": [0.0],
        "AAGR_suma": [0.0],
        "salario_promedio_smlmv": [1.0],
        "pct_no_matriculados_2024": [0.6],
        "num_programas_2024": [60],
        "distancia_costo_pct": [-25.0],
    })


def test_score_minimo_rangos_bajos():
    """Valores en el rango más bajo de cada variable reciben score 1 (o 5 si inverse)."""
    # Variables "mayor es mejor": valor bajo → score 1
    df = _df_base()
    df["suma_matricula_2024"] = 0
    df["participacion_2024"] = 0
    df["AAGR_suma"] = -0.01
    df["salario_promedio_smlmv"] = 1
    df["distancia_costo_pct"] = -30
    # Variables "menor es mejor": valor alto → score 1
    df["pct_no_matriculados_2024"] = 0.6
    df["num_programas_2024"] = 100
    out = apply_scoring(df)
    assert out["score_matricula"].iloc[0] == 1.0
    assert out["score_participacion"].iloc[0] == 1.0
    assert out["score_AAGR"].iloc[0] == 1.0
    assert out["score_salario"].iloc[0] == 1.0
    assert out["score_pct_no_matriculados"].iloc[0] == 1.0
    assert out["score_num_programas"].iloc[0] == 1.0
    assert out["score_costo"].iloc[0] == 1.0


def test_score_maximo_rangos_altos():
    """Valores en el rango más alto de cada variable reciben score 5 (o 1 si inverse)."""
    df = pd.DataFrame({
        "suma_matricula_2024": [500],
        "participacion_2024": [0.02],
        "AAGR_suma": [0.5],
        "salario_promedio_smlmv": [8.0],
        "pct_no_matriculados_2024": [0.05],
        "num_programas_2024": [2],
        "distancia_costo_pct": [30.0],
    })
    out = apply_scoring(df)
    assert out["score_matricula"].iloc[0] == 5.0
    assert out["score_participacion"].iloc[0] == 5.0
    assert out["score_AAGR"].iloc[0] == 5.0
    assert out["score_salario"].iloc[0] == 5.0
    assert out["score_pct_no_matriculados"].iloc[0] == 5.0
    assert out["score_num_programas"].iloc[0] == 5.0
    assert out["score_costo"].iloc[0] == 5.0


def test_calificacion_final_entre_1_y_5():
    """calificacion_final está siempre entre 1.0 y 5.0."""
    # Caso mínimo
    df_min = _df_base()
    df_min.loc[0, "suma_matricula_2024"] = 0
    df_min.loc[0, "participacion_2024"] = 0
    df_min.loc[0, "AAGR_suma"] = 0
    df_min.loc[0, "salario_promedio_smlmv"] = 1
    df_min.loc[0, "pct_no_matriculados_2024"] = 0.6
    df_min.loc[0, "num_programas_2024"] = 100
    df_min.loc[0, "distancia_costo_pct"] = -30
    out_min = apply_scoring(df_min)
    assert out_min["calificacion_final"].iloc[0] >= 1.0
    assert out_min["calificacion_final"].iloc[0] <= 5.0

    # Caso máximo
    df_max = pd.DataFrame({
        "suma_matricula_2024": [1000],
        "participacion_2024": [0.05],
        "AAGR_suma": [0.4],
        "salario_promedio_smlmv": [10.0],
        "pct_no_matriculados_2024": [0.02],
        "num_programas_2024": [1],
        "distancia_costo_pct": [50.0],
    })
    out_max = apply_scoring(df_max)
    assert out_max["calificacion_final"].iloc[0] >= 1.0
    assert out_max["calificacion_final"].iloc[0] <= 5.0


def test_manejo_nulos_no_rompe():
    """NaN en alguna variable no debe romper; se asigna score 1 y se sigue."""
    df = _df_base()
    df.loc[0, "suma_matricula_2024"] = np.nan
    out = apply_scoring(df)
    assert len(out) == 1
    assert "calificacion_final" in out.columns
    assert 1.0 <= out["calificacion_final"].iloc[0] <= 5.0
    assert out["score_matricula"].iloc[0] == 1.0

    df2 = pd.DataFrame({
        "suma_matricula_2024": [np.nan],
        "participacion_2024": [np.nan],
        "AAGR_suma": [np.nan],
        "salario_promedio_smlmv": [np.nan],
        "pct_no_matriculados_2024": [np.nan],
        "num_programas_2024": [np.nan],
        "distancia_costo_pct": [np.nan],
    })
    out2 = apply_scoring(df2)
    assert len(out2) == 1
    assert out2["calificacion_final"].iloc[0] == 1.0


def test_columna_ausente_no_rompe():
    """Si falta una columna esperada, se usa score 1 para esa variable; las presentes se puntúan normal."""
    df = pd.DataFrame({"suma_matricula_2024": [100]})
    out = apply_scoring(df)
    assert "calificacion_final" in out.columns
    # 100 está en rango (50, 200] → score 4 para matrícula; el resto recibe 1 por ausencia
    assert out["score_matricula"].iloc[0] == 4.0
    assert 1.0 <= out["calificacion_final"].iloc[0] <= 5.0
