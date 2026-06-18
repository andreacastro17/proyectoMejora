"""
Lógica de calificación ponderada por categoría (Fase 4 pipeline mercado).

Cada variable se mapea a score 1-5 según umbrales; calificacion_final = suma(score_i × peso_i).

score_matricula: modo_local=False → quintiles fijos nacionales (P20/P40/P60/P80) sobre Colombia.
                 modo_local=True  → quintiles dinámicos del segmento (regional/modal).

score_participacion: quintiles por **cuantiles del segmento** actual
(``participacion_<AÑO_FIN_DATOS>``).
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# AÑO_FIN_DATOS debe importarse ANTES de SCORING_CONFIG porque las f-strings de
# nombres de columna se evalúan al cargar el módulo.
from etl.config import AÑO_FIN_DATOS
from etl.pipeline_logger import log_info

# Pesos de las dos métricas con lógica propia (deben coincidir con el resto de SCORING_CONFIG).
_SCORE_MATRICULA_PESO = 0.30
_SCORE_PARTICIPACION_PESO = 0.15

# score_matricula — thresholds por nivel, posgrado Colombia
# Calibrados con denominador correcto (Fix 24): solo programas con primer_curso > 0.
# Fuente: percentiles reales del log post-Fix-24A, Colombia 2024.
#
# ESP (217 categorías con dato, n_total=235):
# P10=3.64  P20=9.89  P40=19.08  P60=29.90  P80=43.87  P90=63.57
# Distribución resultante: ~44/43/43/43/44 sobre las 217 con dato (quintílica).
# Las 18 sin prom_pc2024 reciben score=1 por fallback conservador.
_SCORE_MAT_ESP_P20: float = 9.89
_SCORE_MAT_ESP_P40: float = 19.08
_SCORE_MAT_ESP_P60: float = 29.90
_SCORE_MAT_ESP_P80: float = 43.87

# MAE (53 categorías con dato, n_total=53):
# P10=6.59  P20=7.93  P40=9.78  P60=12.76  P80=18.85  P90=24.29
# Distribución resultante: ~11/10/11/10/11 (quintílica perfecta).
_SCORE_MAT_MAE_P20: float = 7.93
_SCORE_MAT_MAE_P40: float = 9.78
_SCORE_MAT_MAE_P60: float = 12.76
_SCORE_MAT_MAE_P80: float = 18.85

# score_matricula — PREGRADO (prom_primer_curso_2024, ~146 cats Colombia)
# Fix 24: denominador = solo programas con primer_curso > 0 ese año.
# PENDIENTE Fase B: recalibrar con percentiles reales del output post-fix.
# Valores actuales son estimación; pueden estar subestimados ~2-3×.
_SCORE_MAT_PRE_P20: float = 35.0
_SCORE_MAT_PRE_P40: float = 65.0
_SCORE_MAT_PRE_P60: float = 100.0
_SCORE_MAT_PRE_P80: float = 160.0

# score_AAGR — thresholds por TIPO_CRECIMIENTO × nivel (Fix 25) ──────────────
#
# El valor "NORMAL" de TIPO_CRECIMIENTO fue expandido en 4 bandas de tamaño:
#   NICHO       : 30–99 est. en año inicial (15–99 MAE)
#   EMERGENTE   : 100–399
#   ESTABLECIDO : 400–1.499
#   CONSOLIDADO : ≥ 1.500
#
# Cada entrada es (P20, P40, P60, P80) calibrada sobre Colombia 2024.
# BASE_PEQUENA, CATEGORIA_NUEVA, EXTINTA y SIN_ACTIVIDAD se manejan
# aparte en _score_aagr_nivel (EXTINTA/SIN_ACTIVIDAD → score 1 directo;
# CATEGORIA_NUEVA → usa thresholds de BASE_PEQUENA).
#
# Ver comentarios inline sobre calibración por banda.
_AAGR_ESP_BANDA: dict[str, list[tuple[float, int]]] = {
    # BASE_PEQUENA n=29 | P20=-3.0%  P40=+5.8%  P60=+10.2%  P80=+13.3%
    "BASE_PEQUENA": [(-0.030, 1), (0.058, 2), (0.102, 3), (0.133, 4)],
    # NICHO        n=56 | P20=+3.9%  P40=+10.0% P60=+21.9%  P80=+36.9%
    # P60/P80 más altos que otros pools porque los nichos tienen
    # mayor volatilidad porcentual — normal para mercados pequeños.
    "NICHO":        [(0.039, 1),  (0.100, 2), (0.219, 3), (0.369, 4)],
    # EMERGENTE    n=68 | P20=+0.7%  P40=+5.5%  P60=+9.9%   P80=+22.4%
    "EMERGENTE":    [(0.007, 1),  (0.055, 2), (0.099, 3), (0.224, 4)],
    # ESTABLECIDO  n=44 | P20=+1.5%  P40=+5.9%  P60=+11.7%  P80=+16.2%
    # Banda de referencia para la tabla estandar_calificacion.
    "ESTABLECIDO":  [(0.015, 1),  (0.059, 2), (0.117, 3), (0.162, 4)],
    # CONSOLIDADO  n=19 | P20=-6.9%  P40=+3.1%  P60=+4.7%   P80=+8.3%
    # Mercados maduros: incluso un AAGR negativo puede ser score 2-3
    # si es "menos negativo" que el 20% inferior de su banda.
    "CONSOLIDADO":  [(-0.069, 1), (0.031, 2), (0.047, 3), (0.083, 4)],
}

_AAGR_MAE_BANDA: dict[str, list[tuple[float, int]]] = {
    # BASE_PEQUENA n=3  | P20=-24.0% P40=-15.8% P60=-8.7%   P80=-2.8%
    # n=3 insuficiente para calibración robusta. Se usan los percentiles reales
    # porque las 3 categorías existentes están todas en contracción severa.
    # No se hace fallback a NICHO (positivo) porque distorsionaría el score.
    "BASE_PEQUENA": [(-0.240, 1), (-0.158, 2), (-0.087, 3), (-0.028, 4)],
    # NICHO        n=16 | P20=+0.8%  P40=+3.7%  P60=+7.8%   P80=+20.1%
    "NICHO":        [(0.008, 1),  (0.037, 2), (0.078, 3), (0.201, 4)],
    # EMERGENTE    n=21 | P20=-4.0%  P40=-1.4%  P60=+0.8%   P80=+4.9%
    "EMERGENTE":    [(-0.040, 1), (-0.014, 2), (0.008, 3), (0.049, 4)],
    # ESTABLECIDO  n=10 | P20=-15.4% P40=-5.8%  P60=-0.3%   P80=+3.8%
    "ESTABLECIDO":  [(-0.154, 1), (-0.058, 2), (-0.003, 3), (0.038, 4)],
    # CONSOLIDADO  n=2  | P20=-0.6%  P40=+3.3%  P60=+7.2%   P80=+11.1%
    # n=2 insuficiente. Se usan los valores reales de los únicos 2 mercados MAE
    # consolidados de Colombia. No se hace fallback a ESTABLECIDO porque sus
    # thresholds (-15%/−6%/0%/4%) son demasiado conservadores para este nivel.
    "CONSOLIDADO":  [(-0.006, 1), (0.033, 2), (0.072, 3), (0.111, 4)],
}
# score_AAGR — PREGRADO, thresholds por banda de tamaño (extensión de Fix 25)
#
# TIPO_CRECIMIENTO ya se calcula para UNIVERSITARIO con las mismas 4 bandas que
# posgrado (NICHO/EMERGENTE/ESTABLECIDO/CONSOLIDADO), pero hasta esta versión
# scoring.py las ignoraba y usaba un único threshold plano para todas.
#
# Auditoría confirmó el mismo sesgo que motivó Fix 25 en posgrado:
# BASE_PEQUENA sobre-premiado (P80=25.6% vs threshold plano de 12.0% para score 5)
# y ESTABLECIDO castigado (P80=6.4%, la mitad del mismo threshold).
#
# Pregrado no distingue ESP/MAE — un solo diccionario banda → thresholds.
# NICHO no aparece en pregrado porque el umbral BASE_PEQUEÑA/NORMAL para
# UNIVERSITARIO es 100 (ver _umbral_base en mercado_pipeline.py), por lo que
# cualquier categoría con 30-99 estudiantes ya cae en BASE_PEQUEÑA, no en NICHO.
# Se mantiene la entrada "NICHO" como alias de BASE_PEQUEÑA por robustez,
# en caso de que el umbral cambie en el futuro y empiecen a aparecer NICHO reales.
_AAGR_PRE_BANDA: dict[str, list[tuple[float, int]]] = {
    # BASE_PEQUENA n=24 | P20=0.0%   P40=9.8%  P60=14.0%  P80=25.6%
    "BASE_PEQUENA": [(0.000, 1), (0.098, 2), (0.140, 3), (0.256, 4)],
    # NICHO: alias de BASE_PEQUENA — ver nota arriba.
    "NICHO":        [(0.000, 1), (0.098, 2), (0.140, 3), (0.256, 4)],
    # EMERGENTE    n=15 | P20=-5.7%  P40=2.3%  P60=7.5%   P80=13.0%
    "EMERGENTE":    [(-0.057, 1), (0.023, 2), (0.075, 3), (0.130, 4)],
    # ESTABLECIDO  n=24 | P20=-1.9%  P40=0.2%  P60=0.9%   P80=6.4%
    "ESTABLECIDO":  [(-0.019, 1), (0.002, 2), (0.009, 3), (0.064, 4)],
    # CONSOLIDADO  n=53 | P20=0.7%   P40=2.2%  P60=5.0%   P80=8.5%
    "CONSOLIDADO":  [(0.007, 1), (0.022, 2), (0.050, 3), (0.085, 4)],
}

# Threshold plano anterior — se mantiene solo como referencia histórica.
# Ya no se usa en SCORING_CONFIG_PREGRADO tras este cambio.
_AAGR_PRE_THRESHOLDS_LEGACY: list[tuple[float, int]] = [
    (-0.0036, 1), (0.019, 2), (0.064, 3), (0.120, 4)
]
# Alias para auditoría en mercado_pipeline.py (import existente, no modificar ese módulo).
_AAGR_PRE_THRESHOLDS = _AAGR_PRE_THRESHOLDS_LEGACY

# Umbrales: lista de (límite_superior_inclusivo, score). Valores por encima del último → score 5 (o 1 si inverse).
# Para "inverse" (menor es mejor), se usa score 5 para el rango más bajo.
SCORING_CONFIG = [
    {
        "col": "AAGR_ROBUSTO",
        "out": "score_AAGR",
        "peso": 0.20,
        # Percentiles P20/P40/P60/P80 de AAGR_ROBUSTO (primer_curso) Colombia 288 cats
        # Thresholds calibrados sobre prom_primer_curso (flujo de nuevos matriculados).
        # P80 real Colombia = 16.5% para posgrado.
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
        "col": f"pct_no_matriculados_{AÑO_FIN_DATOS}",
        "out": "score_pct_no_matriculados",
        "peso": 0.10,
        "thresholds": [(0.10, 5), (0.20, 4), (0.30, 3), (0.50, 2)],
        "inverse": True,
    },
    {
        "col": f"num_programas_{AÑO_FIN_DATOS}",
        "out": "score_num_programas",
        "peso": 0.05,
        # Percentiles reales posgrado-only (ESP+MAE, 288 cats):
        # P20=4 · P40=10 · P60=18 · P80=32 (media=21 progs/cat)
        # P60 actualizado de 25→18, P80 actualizado de 55→32
        "thresholds": [(4, 5), (10, 4), (18, 3), (32, 2)],
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


# ── Configuración de scoring para universo PREGRADO ──────────────────────────
# Todos los thresholds calibrados sobre 146 categorías con programas UNIVERSITARIO.
# Pesos idénticos al posgrado para mantener comparabilidad de estructura.
SCORING_CONFIG_PREGRADO: list[dict] = [
    {
        "col": "AAGR_ROBUSTO",
        "out": "score_AAGR",
        "peso": 0.20,
        # Fix 25 extendido a pregrado: thresholds por banda, no por este campo.
        # "thresholds" se mantiene aquí solo como fallback si TIPO_CRECIMIENTO
        # no está disponible (ver bloque de dispatch más abajo).
        "thresholds": _AAGR_PRE_THRESHOLDS_LEGACY,
        "inverse": False,
    },
    {
        "col": "salario_promedio_smlmv",
        "out": "score_salario",
        "peso": 0.15,
        # P20=2.79 P40=3.31 P60=3.94 P80=5.33 SMLMV (real pipeline, 143 cats)
        # Thresholds = percentiles exactos → distribución uniforme {29,29,29,29,29}.
        # Anterior (2.0,1) bajaba demasiado el corte inferior (P10=2.54) y juntaba
        # P40→P80 en un solo bucket (score 4 con 57 cats / 40%).
        "thresholds": [(2.79, 1), (3.31, 2), (3.94, 3), (5.33, 4)],
        "inverse": False,
    },
    {
        "col": f"pct_no_matriculados_{AÑO_FIN_DATOS}",
        "out": "score_pct_no_matriculados",
        "peso": 0.10,
        # P20=0.273 P40=0.408 P60=0.509 P80=0.623 (real pipeline, 146 cats)
        # Anterior: P80=0.48 causaba 66 cats en score 1 (45%) — umbral demasiado bajo
        "thresholds": [(0.273, 5), (0.408, 4), (0.509, 3), (0.623, 2)],
        "inverse": True,
    },
    {
        "col": f"num_programas_{AÑO_FIN_DATOS}",
        "out": "score_num_programas",
        "peso": 0.05,
        # P20=1 P40=5 P60=19 P80=55 (real pipeline, 146 cats)
        # Anterior: P20=2 causaba 43 cats en score 5 (29%) — 20% ideal
        "thresholds": [(1, 5), (5, 4), (19, 3), (55, 2)],
        "inverse": True,
    },
    {
        "col": "distancia_costo_pct",
        "out": "score_costo",
        "peso": 0.05,
        # Mismo threshold: distancia relativa ya ajusta por nivel via BENCHMARK_COSTO_PREGRADO
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


def apply_scoring(
    df: pd.DataFrame,
    modo_local: bool = False,
    universo: str = "posgrado",
) -> pd.DataFrame:
    """
    Aplica la calificación ponderada a un DataFrame con las columnas esperadas por SCORING_CONFIG.
    Añade columnas score_* y calificacion_final. NaN en alguna variable no rompe; se usa score 1.
    calificacion_final está siempre entre 1.0 y 5.0.

    Args:
        df: DataFrame con las columnas de métricas agregadas por categoría.
        modo_local: Si True, score_matricula usa quintiles calculados sobre el propio
                    DataFrame (para segmentos regionales/modales). Si False (default),
                    usa thresholds nacionales fijos por nivel ESP/MAE (posgrado) o
                    _SCORE_MAT_PRE_* (pregrado), garantizando comparabilidad cross-segmento.
        universo:   "posgrado" (default) usa SCORING_CONFIG y thresholds de matrícula
                    posgrado. "pregrado" usa SCORING_CONFIG_PREGRADO y thresholds de
                    matrícula pregrado (~10× mayor). En posgrado, score_AAGR aplica
                    árbol de decisión por NIVEL_MAYORIT (ESP vs MAE).
    """
    out = df.copy()
    scoring_config = SCORING_CONFIG_PREGRADO if universo == "pregrado" else SCORING_CONFIG
    total_peso = (
        _SCORE_MATRICULA_PESO
        + _SCORE_PARTICIPACION_PESO
        + sum(c["peso"] for c in scoring_config)
    )
    if abs(total_peso - 1.0) > 1e-9:
        raise ValueError(
            f"Los pesos deben sumar 1.0, suman {total_peso} (universo={universo})"
        )

    # ── DIAGNÓSTICO DE DISTRIBUCIÓN ───────────────────────────────────────────
    _cols_diag = {
        f"prom_primer_curso_{AÑO_FIN_DATOS}": "PRIMER_CURSO",
        f"prom_matricula_por_programa_{AÑO_FIN_DATOS}": "MAT",
        f"prom_matricula_{AÑO_FIN_DATOS}": "MAT_PROM",
        f"participacion_{AÑO_FIN_DATOS}": "PART",
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

    if "NIVEL_MAYORIT" in out.columns and f"prom_primer_curso_{AÑO_FIN_DATOS}" in out.columns:
        _prom_col = pd.to_numeric(out[f"prom_primer_curso_{AÑO_FIN_DATOS}"], errors="coerce")
        _ESP_MASK = out["NIVEL_MAYORIT"].astype(str).str.upper().isin({
            "ESPECIALIZACIÓN", "ESPECIALIZACIÓN MÉDICO QUIRÚRGICA",
            "ESPECIALIZACIÓN TECNOLÓGICA", "ESPECIALIZACIÓN TÉCNICO PROFESIONAL",
        })
        _MAE_MASK = out["NIVEL_MAYORIT"].astype(str).str.upper() == "MAESTRÍA"
        for _lbl, _mask in [("ESP", _ESP_MASK), ("MAE", _MAE_MASK)]:
            _s = _prom_col[_mask].dropna()
            if len(_s) > 0:
                _pcts = [float(np.percentile(_s, p)) for p in [10, 20, 40, 60, 80, 90]]
                log_info(
                    f"[Scoring diag Fix24] prom_pc_{AÑO_FIN_DATOS} {_lbl} "
                    f"n={len(_s)} | "
                    f"P10={_pcts[0]:.2f} P20={_pcts[1]:.2f} P40={_pcts[2]:.2f} "
                    f"P60={_pcts[3]:.2f} P80={_pcts[4]:.2f} P90={_pcts[5]:.2f}"
                )

    # Con inscritos SNIES (fuente primaria), la cobertura es >80%.
    PCT_NAN_FILL = 0.25
    _col_pct_nm = f"pct_no_matriculados_{AÑO_FIN_DATOS}"
    if _col_pct_nm in out.columns:
        out[_col_pct_nm] = out[_col_pct_nm].fillna(PCT_NAN_FILL)

    # score_matricula — thresholds nacionales fijos (modo_local=False)
    #                   o quintiles locales dinámicos (modo_local=True).
    if f"prom_matricula_por_programa_{AÑO_FIN_DATOS}" in out.columns:
        _s_mat = pd.to_numeric(out[f"prom_matricula_por_programa_{AÑO_FIN_DATOS}"], errors="coerce")
    elif f"prom_matricula_{AÑO_FIN_DATOS}" in out.columns:
        _s_mat = pd.to_numeric(out[f"prom_matricula_{AÑO_FIN_DATOS}"], errors="coerce")
    else:
        _s_mat = None

    if _s_mat is not None:
        if modo_local:
            # Quintiles dinámicos sobre el segmento — misma lógica que score_participacion
            _mat_valid = _s_mat.dropna()
            _lp20 = float(_mat_valid.quantile(0.20))
            _lp40 = float(_mat_valid.quantile(0.40))
            _lp60 = float(_mat_valid.quantile(0.60))
            _lp80 = float(_mat_valid.quantile(0.80))
            # Guard contra bins duplicados cuando el segmento es pequeño
            _lqs = [_lp20, _lp40, _lp60, _lp80]
            for _i in range(1, len(_lqs)):
                if _lqs[_i] <= _lqs[_i - 1]:
                    _lqs[_i] = _lqs[_i - 1] + 1e-9
            _lp20, _lp40, _lp60, _lp80 = _lqs
            _bins = [-np.inf, _lp20, _lp40, _lp60, _lp80, np.inf]
            log_info(
                f"[Scoring] score_matricula LOCAL → "
                f"P20={_lp20:.2f} P40={_lp40:.2f} P60={_lp60:.2f} P80={_lp80:.2f}"
            )
            _cat_mat = pd.cut(
                _s_mat,
                bins=_bins,
                labels=[1, 2, 3, 4, 5],
                right=True,
            )
            out["score_matricula"] = (
                pd.to_numeric(_cat_mat, errors="coerce").fillna(1.0).astype(int)
            )
        elif universo == "pregrado":
            _p20, _p40, _p60, _p80 = (
                _SCORE_MAT_PRE_P20,
                _SCORE_MAT_PRE_P40,
                _SCORE_MAT_PRE_P60,
                _SCORE_MAT_PRE_P80,
            )
            log_info(
                f"[Scoring] score_matricula PREGRADO → "
                f"P20={_p20} P40={_p40} P60={_p60} P80={_p80}"
            )
            _bins = [-np.inf, _p20, _p40, _p60, _p80, np.inf]
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
            # POSGRADO — árbol de decisión ESP vs MAE (igual que score_AAGR).
            if "NIVEL_MAYORIT" not in out.columns:
                _p20, _p40, _p60, _p80 = (
                    _SCORE_MAT_ESP_P20,
                    _SCORE_MAT_ESP_P40,
                    _SCORE_MAT_ESP_P60,
                    _SCORE_MAT_ESP_P80,
                )
                _bins = [-np.inf, _p20, _p40, _p60, _p80, np.inf]
                _cat_mat = pd.cut(
                    _s_mat,
                    bins=_bins,
                    labels=[1, 2, 3, 4, 5],
                    right=True,
                )
                out["score_matricula"] = (
                    pd.to_numeric(_cat_mat, errors="coerce").fillna(1.0).astype(int)
                )
                log_info(
                    f"[Scoring] score_matricula POSGRADO (fallback ESP) → "
                    f"P20={_p20} P40={_p40} P60={_p60} P80={_p80}"
                )
            else:
                _col_mat = f"prom_matricula_por_programa_{AÑO_FIN_DATOS}"

                def _score_mat_nivel(
                    row,
                    _esp_p20=_SCORE_MAT_ESP_P20,
                    _esp_p40=_SCORE_MAT_ESP_P40,
                    _esp_p60=_SCORE_MAT_ESP_P60,
                    _esp_p80=_SCORE_MAT_ESP_P80,
                    _mae_p20=_SCORE_MAT_MAE_P20,
                    _mae_p40=_SCORE_MAT_MAE_P40,
                    _mae_p60=_SCORE_MAT_MAE_P60,
                    _mae_p80=_SCORE_MAT_MAE_P80,
                    _col=_col_mat,
                ):
                    nivel = str(row.get("NIVEL_MAYORIT", "")).strip().upper()
                    val = row.get(_col, np.nan)
                    v = float(val) if pd.notna(val) else np.nan
                    if pd.isna(v):
                        return 1.0
                    if "MAEST" in nivel:
                        if v <= _mae_p20:
                            return 1.0
                        if v <= _mae_p40:
                            return 2.0
                        if v <= _mae_p60:
                            return 3.0
                        if v <= _mae_p80:
                            return 4.0
                        return 5.0
                    if v <= _esp_p20:
                        return 1.0
                    if v <= _esp_p40:
                        return 2.0
                    if v <= _esp_p60:
                        return 3.0
                    if v <= _esp_p80:
                        return 4.0
                    return 5.0

                out["score_matricula"] = out.apply(_score_mat_nivel, axis=1).astype(int)
                log_info(
                    f"[Scoring] score_matricula POSGRADO ESP/MAE → "
                    f"ESP: P20={_SCORE_MAT_ESP_P20} P40={_SCORE_MAT_ESP_P40} "
                    f"P60={_SCORE_MAT_ESP_P60} P80={_SCORE_MAT_ESP_P80} | "
                    f"MAE: P20={_SCORE_MAT_MAE_P20} P40={_SCORE_MAT_MAE_P40} "
                    f"P60={_SCORE_MAT_MAE_P60} P80={_SCORE_MAT_MAE_P80}"
                )
    else:
        out["score_matricula"] = 1

    # score_participacion — cuantiles del segmento en curso (mercado regional relativo)
    # NOTA: el denominador de participacion_2024 es el total del mercado nacional completo
    # (ESP+MAE, todos los sectores, ~146K en 2024), NO el subconjunto del manual de referencia
    # (~15K). Los quintiles internos hacen que el cambio de denominador no afecte los scores.
    if f"participacion_{AÑO_FIN_DATOS}" in out.columns:
        _part_series = pd.to_numeric(out[f"participacion_{AÑO_FIN_DATOS}"], errors="coerce")
        _n_validos = _part_series.notna().sum()
        _n_total = len(_part_series)

        if _n_total and _n_validos < _n_total * 0.20:
            log_info(
                f"[Scoring] ALERTA: participacion_{AÑO_FIN_DATOS} tiene solo {_n_validos}/{_n_total} "
                f"valores válidos ({_n_validos / _n_total * 100:.0f}%). "
                f"score_participacion asignado en 1 para todos. "
                f"Verificar que primer_curso_{AÑO_FIN_DATOS}.xlsx exista en ref/backup/."
            )
            out["score_participacion"] = 1
        else:
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

    for cfg in scoring_config:
        col = cfg["col"]
        out_col = cfg["out"]
        thresholds = cfg["thresholds"]
        inverse = cfg.get("inverse", False)

        if col not in out.columns:
            out[out_col] = 1.0
            continue

        # Árbol de decisión AAGR por banda de tamaño — posgrado (ESP/MAE) y pregrado.
        # Fix 25 extendido: ambos universos usan TIPO_CRECIMIENTO para elegir el
        # pool de percentiles correcto, en lugar de un único threshold plano.
        if (
            col == "AAGR_ROBUSTO"
            and universo == "posgrado"
            and "NIVEL_MAYORIT" in out.columns
        ):
            def _score_aagr_nivel(
                row,
                _esp_banda=_AAGR_ESP_BANDA,
                _mae_banda=_AAGR_MAE_BANDA,
            ):
                nivel = str(row.get("NIVEL_MAYORIT", "")).upper()
                tipo  = str(row.get("TIPO_CRECIMIENTO", "NICHO")).strip()
                val   = row.get("AAGR_ROBUSTO", np.nan)
                v = float(val) if pd.notna(val) else np.nan

                if tipo in ("EXTINTA", "SIN_ACTIVIDAD"):
                    return 1.0

                scoring_tipo = "BASE_PEQUENA" if tipo == "CATEGORIA_NUEVA" else tipo

                if "MAEST" in nivel:
                    thrs = _mae_banda.get(scoring_tipo, _mae_banda["NICHO"])
                else:
                    thrs = _esp_banda.get(scoring_tipo, _esp_banda["NICHO"])

                return _value_to_score(v, thrs, inverse=False)

            out[out_col] = out.apply(_score_aagr_nivel, axis=1)

        elif (
            col == "AAGR_ROBUSTO"
            and universo == "pregrado"
            and "TIPO_CRECIMIENTO" in out.columns
        ):
            def _score_aagr_pregrado(
                row,
                _pre_banda=_AAGR_PRE_BANDA,
                _fallback=_AAGR_PRE_THRESHOLDS_LEGACY,
            ):
                tipo = str(row.get("TIPO_CRECIMIENTO", "")).strip()
                val  = row.get("AAGR_ROBUSTO", np.nan)
                v = float(val) if pd.notna(val) else np.nan

                if tipo in ("EXTINTA", "SIN_ACTIVIDAD"):
                    return 1.0

                scoring_tipo = "BASE_PEQUENA" if tipo == "CATEGORIA_NUEVA" else tipo
                thrs = _pre_banda.get(scoring_tipo)

                # Si TIPO_CRECIMIENTO viene vacío o con un valor inesperado,
                # usar el threshold plano legacy como red de seguridad.
                if thrs is None:
                    return _value_to_score(v, _fallback, inverse=False)

                return _value_to_score(v, thrs, inverse=False)

            out[out_col] = out.apply(_score_aagr_pregrado, axis=1)

        else:
            out[out_col] = out[col].apply(
                lambda v: _value_to_score(float(v) if pd.notna(v) else np.nan, thresholds, inverse)
            )

    out["calificacion_final"] = 0.0
    out["calificacion_final"] += out["score_matricula"].astype(float) * _SCORE_MATRICULA_PESO
    out["calificacion_final"] += out["score_participacion"].astype(float) * _SCORE_PARTICIPACION_PESO
    for cfg in scoring_config:
        out["calificacion_final"] += out[cfg["out"]] * cfg["peso"]
    out["calificacion_final"] = out["calificacion_final"].clip(1.0, 5.0).round(4)

    for _sc in ["score_matricula", "score_participacion", "score_AAGR", "score_salario"]:
        if _sc in out.columns:
            _dist = out[_sc].value_counts().sort_index().to_dict()
            log_info(f"[Scoring val/{universo}] {_sc}: {_dist}")

    return out
