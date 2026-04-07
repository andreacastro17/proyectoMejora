"""
Lógica de calificación ponderada por categoría (Fase 4 pipeline mercado).

Cada variable se mapea a score 1-5 según umbrales; calificacion_final = suma(score_i × peso_i).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Umbrales: lista de (límite_superior_inclusivo, score). Valores por encima del último → score 5 (o 1 si inverse).
# Para "inverse" (menor es mejor), se usa score 5 para el rango más bajo.
SCORING_CONFIG = [
    {
        "col": "suma_matricula_2024",
        "out": "score_matricula",
        "peso": 0.30,
        # Distribución actual con umbrales anteriores: score2=14% score3=39% score4=41% score5=6%
        # Problema: demasiado concentrado en scores 3-4, casi nadie llega a 5.
        # Recalibrado con percentiles reales: p25=182, p50=515, p75=1350, p95=5317
        # Resultado esperado: distribución más uniforme ~20% por score
        "thresholds": [(0, 1), (200, 2), (600, 3), (2000, 4)],
        "inverse": False,
    },
    {
        "col": "participacion_2024",
        "out": "score_participacion",
        "peso": 0.15,
        # Distribución actual: score1=0% score2=6% score3=20% score4=33% score5=41%
        # Problema: 41% de categorías en score 5, umbrales demasiado bajos.
        # Recalibrado con percentiles reales: p25=0.046% p50=0.129% p75=0.339% p95=1.34%
        "thresholds": [(0, 1), (0.0002, 2), (0.0008, 3), (0.003, 4)],
        "inverse": False,
    },
    {
        "col": "AAGR_ROBUSTO",
        "out": "score_AAGR",
        "peso": 0.20,
        # Distribución actual: score1=33% score2=22% score3=34% score4=6% score5=4%
        # Problema: 33% en score 1, umbrales correctos pero el mercado colombiano
        # tiene crecimiento moderado. Umbrales se mantienen — la distribución refleja
        # la realidad: muchos mercados decrecen o crecen poco.
        # NO CAMBIAR — distribución es informativa, no un problema de calibración.
        "thresholds": [(0, 1), (0.04, 2), (0.19, 3), (0.30, 4)],
        "inverse": False,
    },
    {
        "col": "salario_promedio_smlmv",
        "out": "score_salario",
        "peso": 0.15,
        # Distribución actual: score1=1% score2=2% score3=11% score4=42% score5=44%
        # Problema crítico: 86% en scores 4-5. Mediana real = 5.74 SMLMV.
        # El umbral de score5 (>6 SMLMV) es demasiado bajo — el 44% llega ahí fácilmente.
        # Recalibrado con percentiles reales: p25=4.60, p50=5.74, p75=7.24, p90=8.89
        # Alineado también con el Excel manual (umbrales 5 y 8 SMLMV)
        "thresholds": [(2, 1), (4, 2), (5, 3), (7, 4)],
        "inverse": False,
    },
    {
        "col": "pct_no_matriculados_2024",
        "out": "score_pct_no_matriculados",
        "peso": 0.10,
        # Distribución actual: score5=79% (la mayoría)
        # Causa: mediana = 0 (muchas categorías sin datos de inscritos → fill con 0.20 → score4-5)
        # Los umbrales son correctos conceptualmente. El fill de 0.20 produce score4, no score5.
        # El 79% en score5 se debe a categorías con tasa real baja — es correcto.
        # NO CAMBIAR umbrales. Sí ajustar el fill neutral de 0.20 a 0.25 para ser más conservador
        # cuando no hay datos de inscritos (0.25 queda en score3 = neutro real).
        "thresholds": [(0.10, 5), (0.20, 4), (0.30, 3), (0.50, 2)],
        "inverse": True,
    },
    {
        "col": "num_programas_2024",
        "out": "score_num_programas",
        "peso": 0.05,
        # Distribución actual: score5=27% score4=30% score3=22% score2=15% score1=6%
        # Percentiles reales: p25=5, p50=13, p75=27, p90=43, p95=68
        # Distribución razonablemente uniforme. Ajuste menor al último umbral
        # para alinear con p95 real (68 vs 60 actual — impacto mínimo).
        "thresholds": [(5, 5), (15, 4), (30, 3), (70, 2)],
        "inverse": True,
    },
    {
        "col": "distancia_costo_pct",
        "out": "score_costo",
        "peso": 0.05,
        # Distribución actual: score1=22% score2=51% score3=7% score4=4% score5=16%
        # Problema grave: 51% en score2, distribución muy sesgada.
        # Causa: mediana real = -36.9% (mayoría de programas más baratos que el benchmark).
        # Los umbrales actuales (-50, -20, 0, 20) no reflejan esta realidad.
        # Recalibrado con percentiles reales: p25=-48% p50=-37% p75=-18% p90=+79%
        "thresholds": [(-60, 1), (-40, 2), (-15, 3), (20, 4)],
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
    PCT_NAN_FILL = 0.25  # 0.25 cae en score 3 (neutro real) en vez de score 4
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
