"""
Lógica de calificación ponderada por categoría (Fase 4 pipeline mercado).

Cada variable se mapea a score 1-5 según umbrales; calificacion_final = suma(score_i × peso_i).

score_matricula: quintiles fijos (P20/P40/P60/P80) sobre ``prom_matricula_por_programa_2024``
calibrados en el agregado **Colombia** (no por segmento regional).

score_participacion: quintiles por **cuantiles del segmento** actual (``participacion_2024``).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from etl.pipeline_logger import log_info

# Pesos de las dos métricas con lógica propia (deben coincidir con el resto de SCORING_CONFIG).
_SCORE_MATRICULA_PESO = 0.30
_SCORE_PARTICIPACION_PESO = 0.15

# score_matricula — percentiles reales de prom_primer_curso_2024 sobre Colombia (288 cats)
# Ejecución: pipeline post-primer_curso · Universos mezclados ESP+MAE+PRE
# Distribución resultante: ~58 categorías por score (quintílica perfecta)
_SCORE_MAT_P20 = 3.9
_SCORE_MAT_P40 = 8.3
_SCORE_MAT_P60 = 15.0
_SCORE_MAT_P80 = 27.6

# Umbrales: lista de (límite_superior_inclusivo, score). Valores por encima del último → score 5 (o 1 si inverse).
# Para "inverse" (menor es mejor), se usa score 5 para el rango más bajo.
SCORING_CONFIG = [
    {
        "col": "AAGR_ROBUSTO",
        "out": "score_AAGR",
        "peso": 0.20,
        # Percentiles P20/P40/P60/P80 de AAGR_ROBUSTO (primer_curso) Colombia 288 cats
        # Antes (total mat): 0% / 4% / 19% / 30%  ← calibrado para stock, no flujo
        # Ahora (primer_curso): P80 real = 16.5%  (crecimiento de flujo más volátil)
        "thresholds": [(0.00, 1), (0.038, 2), (0.084, 3), (0.165, 4)],
        "inverse": False,
    },
    {
        "col": "salario_promedio_smlmv",
        "out": "score_salario",
        "peso": 0.15,
        # Alineado con Excel referencia manual: score 5 = >= 8 SMLMV (médicos, TI senior)
        # Rango real Colombia: P20=3.2 / P40=4.7 / P60=5.5 / P80=7.0 SMLMV
        "thresholds": [(2, 1), (3, 2), (5, 3), (8, 4)],
        "inverse": False,
    },
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
        "thresholds": [(5, 5), (15, 4), (30, 3), (70, 2)],
        "inverse": True,
    },
    {
        "col": "distancia_costo_pct",
        "out": "score_costo",
        "peso": 0.05,
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
        for bound, score in thresholds:
            if value <= bound:
                return float(score)
        return 1.0
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
    total_peso = _SCORE_MATRICULA_PESO + _SCORE_PARTICIPACION_PESO + sum(c["peso"] for c in SCORING_CONFIG)
    if abs(total_peso - 1.0) > 1e-9:
        raise ValueError(f"Los pesos deben sumar 1.0, suman {total_peso}")

    # ── DIAGNÓSTICO DE DISTRIBUCIÓN ───────────────────────────────────────────
    _cols_diag = {
        "prom_primer_curso_2024": "PRIMER_CURSO",
        "prom_matricula_por_programa_2024": "MAT",
        "prom_matricula_2024": "MAT_PROM",
        "participacion_2024": "PART",
        "AAGR_ROBUSTO": "AAGR",
        "salario_promedio_smlmv": "SAL",
    }
    for _col, _lbl in _cols_diag.items():
        if _col in out.columns:
            _s = pd.to_numeric(out[_col], errors="coerce").dropna()
            if len(_s) > 0:
                _pcts = np.percentile(_s, [10, 20, 40, 60, 80, 90])
                log_info(
                    f"[Scoring diag] {_lbl} n={len(_s)} | "
                    f"P10={_pcts[0]:.4g} P20={_pcts[1]:.4g} P40={_pcts[2]:.4g} "
                    f"P60={_pcts[3]:.4g} P80={_pcts[4]:.4g} P90={_pcts[5]:.4g}"
                )

    # Con inscritos SNIES (fuente primaria desde 2025), la cobertura es >80%.
    PCT_NAN_FILL = 0.25
    if "pct_no_matriculados_2024" in out.columns:
        out["pct_no_matriculados_2024"] = out["pct_no_matriculados_2024"].fillna(PCT_NAN_FILL)

    # score_matricula — quintiles nacionales fijos (promedio por programa en la categoría, 2024)
    # Fase 7 arma filas solo con prom_matricula_2024 (a menudo ya es prom. por programa); usar fallback.
    if "prom_matricula_por_programa_2024" in out.columns:
        _s_mat = pd.to_numeric(out["prom_matricula_por_programa_2024"], errors="coerce")
    elif "prom_matricula_2024" in out.columns:
        _s_mat = pd.to_numeric(out["prom_matricula_2024"], errors="coerce")
    else:
        _s_mat = None
    if _s_mat is not None:
        _bins = [-np.inf, _SCORE_MAT_P20, _SCORE_MAT_P40, _SCORE_MAT_P60, _SCORE_MAT_P80, np.inf]
        _cat_mat = pd.cut(
            _s_mat,
            bins=_bins,
            labels=[1, 2, 3, 4, 5],
            right=True,
        )
        out["score_matricula"] = (
            pd.to_numeric(_cat_mat, errors="coerce").fillna(1.0).astype(int)
        )
    else:
        out["score_matricula"] = 1

    # score_participacion — cuantiles del segmento en curso (mercado regional relativo)
    if "participacion_2024" in out.columns:
        _part_series = pd.to_numeric(out["participacion_2024"], errors="coerce")
        _p20 = float(_part_series.quantile(0.20))
        _p40 = float(_part_series.quantile(0.40))
        _p60 = float(_part_series.quantile(0.60))
        _p80 = float(_part_series.quantile(0.80))
        if any(map(pd.isna, (_p20, _p40, _p60, _p80))):
            out["score_participacion"] = 1
        else:
            _qs = [_p20, _p40, _p60, _p80]
            for _i in range(1, len(_qs)):
                if _qs[_i] <= _qs[_i - 1]:
                    _qs[_i] = _qs[_i - 1] + 1e-15
            _p20, _p40, _p60, _p80 = _qs
            _bins_p = [-np.inf, _p20, _p40, _p60, _p80, np.inf]
            _cat_p = pd.cut(
                _part_series,
                bins=_bins_p,
                labels=[1, 2, 3, 4, 5],
                right=True,
            )
            out["score_participacion"] = (
                pd.to_numeric(_cat_p, errors="coerce").fillna(1.0).astype(int)
            )
    else:
        out["score_participacion"] = 1

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
    out["calificacion_final"] += out["score_matricula"].astype(float) * _SCORE_MATRICULA_PESO
    out["calificacion_final"] += out["score_participacion"].astype(float) * _SCORE_PARTICIPACION_PESO
    for cfg in SCORING_CONFIG:
        out["calificacion_final"] += out[cfg["out"]] * cfg["peso"]
    out["calificacion_final"] = out["calificacion_final"].clip(1.0, 5.0).round(4)

    for _sc in ["score_matricula", "score_participacion", "score_AAGR", "score_salario"]:
        if _sc in out.columns:
            _dist = out[_sc].value_counts().sort_index().to_dict()
            log_info(f"[Scoring val] {_sc}: {_dist}")

    return out
