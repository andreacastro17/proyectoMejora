"""
Lógica de calificación ponderada por categoría (Fase 4 pipeline mercado).

Cada variable se mapea a score 1-5 según umbrales; calificacion_final = suma(score_i × peso_i).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from etl.config import BENCHMARK_COSTO, SMLMV

# Umbrales: lista de (límite_superior_inclusivo, score). Valores por encima del último → score 5 (o 1 si inverse).
# Para "inverse" (menor es mejor), se usa score 5 para el rango más bajo.
SCORING_CONFIG = [
    {
        "col": "suma_matricula_2024",
        "out": "score_matricula",
        "peso": 0.30,
        # Escala macro (aprox: p25=221, p50=1292, p75=6725, p90=41623)
        "thresholds": [(0, 1), (500, 2), (3000, 3), (20000, 4)],
        "inverse": False,
    },
    {
        "col": "participacion_2024",
        "out": "score_participacion",
        "peso": 0.15,
        # Escala macro (aprox: p25=0.000044, p50=0.000257, p75=0.001338)
        "thresholds": [(0, 1), (0.0002, 2), (0.001, 3), (0.005, 4)],
        "inverse": False,
    },
    {
        "col": "AAGR_suma",
        "out": "score_AAGR",
        "peso": 0.20,
        "thresholds": [(0, 1), (0.04, 2), (0.19, 3), (0.30, 4)],
        "inverse": False,
    },
    {
        "col": "salario_promedio_smlmv",
        "out": "score_salario",
        "peso": 0.15,
        "thresholds": [(2, 1), (3, 2), (4, 3), (6, 4)],
        "inverse": False,
    },  # col se rellena en run_fase4 como salario_promedio / SMLMV
    {
        "col": "pct_no_matriculados_2024",
        "out": "score_pct_no_matriculados",
        "peso": 0.10,
        "thresholds": [(0.10, 5), (0.20, 4), (0.30, 3), (0.50, 2)],
        "inverse": True,
    },
    {
        "col": "num_programas_2024",
        "out": "score_num_programas",
        "peso": 0.05,
        # Escala macro (media ~48 programas por categoría)
        "thresholds": [(10, 5), (30, 4), (80, 3), (150, 2)],
        "inverse": True,
    },
    {
        "col": "distancia_costo_pct",
        "out": "score_costo",
        "peso": 0.05,
        # Escala macro (distancia media ~ -16.9%)
        "thresholds": [(-50, 1), (-20, 2), (0, 3), (20, 4)],
        "inverse": False,
    },
]


def _value_to_score(value: float, thresholds: list[tuple[float, int]], inverse: bool) -> float:
    """
    Mapea un valor numérico a score 1-5 según umbrales.
    thresholds: list of (upper_bound_inclusive, score).
    inverse: si True, por debajo del primer umbral se asigna el score del primer umbral (mejor).
    """
    if pd.isna(value) or (isinstance(value, float) and np.isinf(value)):
        return 1.0
    if inverse:
        # Menor es mejor: score 5 para valores bajos, 1 para altos
        for bound, score in thresholds:
            if value <= bound:
                return float(score)
        return 1.0
    # Mayor es mejor: score 1 para valores bajos, 5 para altos
    for bound, score in thresholds:
        if value <= bound:
            return float(score)
    return 5.0


def apply_scoring(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica la calificación ponderada a un DataFrame con las columnas esperadas por SCORING_CONFIG.
    Añade columnas score_* y calificacion_final. NaN en alguna variable no rompe; se usa score 1.
    calificacion_final está siempre entre 1.0 y 5.0.
    """
    out = df.copy()
    total_peso = sum(c["peso"] for c in SCORING_CONFIG)
    if abs(total_peso - 1.0) > 1e-9:
        raise ValueError(f"Los pesos en SCORING_CONFIG deben sumar 1.0, suman {total_peso}")

    # Neutralizar NaN de pct_no_matriculados_2024 (categorías sin datos OLE comparables)
    # para evitar que se vayan al peor score por ausencia de dato.
    PCT_NAN_FILL = 0.20
    if "pct_no_matriculados_2024" in out.columns:
        out["pct_no_matriculados_2024"] = out["pct_no_matriculados_2024"].fillna(PCT_NAN_FILL)

    for cfg in SCORING_CONFIG:
        col = cfg["col"]
        out_col = cfg["out"]
        thresholds = cfg["thresholds"]
        inverse = cfg.get("inverse", False)
        if col not in out.columns:
            out[out_col] = 1.0
            continue
        out[out_col] = out[col].apply(
            lambda v: _value_to_score(float(v) if pd.notna(v) else np.nan, thresholds, inverse)
        )
    out["calificacion_final"] = 0.0
    for cfg in SCORING_CONFIG:
        out["calificacion_final"] += out[cfg["out"]] * cfg["peso"]
    out["calificacion_final"] = out["calificacion_final"].clip(1.0, 5.0)
    return out
