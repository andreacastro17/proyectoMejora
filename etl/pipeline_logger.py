"""
Módulo de logging centralizado para el pipeline SNIES.

Registra todos los eventos del pipeline en logs/pipeline.log con formato:
[YYYY-MM-DD HH:MM:SS] LEVEL: MENSAJE

Soporta niveles (INFO, WARNING, ERROR, DEBUG) y rotación por tamaño (configurable en config.json).
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

from etl.config import LOGS_DIR, LOG_LEVEL, LOG_MAX_BYTES

LOG_FILE = LOGS_DIR / "pipeline.log"
LOG_FILE.parent.mkdir(exist_ok=True)

# Logger con rotación por tamaño (evita que el archivo crezca sin límite)
_LOGGER: logging.Logger | None = None
_BACKUP_COUNT = 3


def _get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER is None:
        _LOGGER = logging.getLogger("pipeline_snies")
        _LOGGER.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
        handler = logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_MAX_BYTES,
            backupCount=_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        _LOGGER.addHandler(handler)
        _LOGGER.propagate = False
    return _LOGGER


def log_inicio() -> None:
    """Registra el inicio de ejecución del pipeline."""
    _get_logger().info("INICIO: ejecución del pipeline.")


def log_fin(duracion_minutos: float | None = None) -> None:
    """
    Registra el fin de ejecución del pipeline.
    
    Args:
        duracion_minutos: Duración de la ejecución en minutos (opcional)
    """
    if duracion_minutos is not None:
        if duracion_minutos == int(duracion_minutos):
            duracion_str = f"{int(duracion_minutos)}m"
        else:
            duracion_str = f"{duracion_minutos:.1f}m"
        _get_logger().info("FIN: ejecución completa (duración: %s).", duracion_str)
    else:
        _get_logger().info("FIN: ejecución completa.")


def log_etapa_iniciada(nombre_etapa: str) -> None:
    """Registra el inicio de una etapa del pipeline."""
    _get_logger().info("%s iniciada...", nombre_etapa)


def log_etapa_completada(nombre_etapa: str, detalles: str | None = None) -> None:
    """Registra la finalización exitosa de una etapa."""
    if detalles:
        _get_logger().info("%s finalizada: %s", nombre_etapa, detalles)
    else:
        _get_logger().info("%s finalizada.", nombre_etapa)


def log_error(mensaje_error: str) -> None:
    """Registra un error."""
    _get_logger().error("%s", mensaje_error)


def log_warning(mensaje_warning: str) -> None:
    """Registra una advertencia."""
    _get_logger().warning("%s", mensaje_warning)


def log_info(mensaje_info: str) -> None:
    """Registra información general."""
    _get_logger().info("%s", mensaje_info)


def log_resultado(mensaje_resultado: str) -> None:
    """Registra un resultado o KPI."""
    _get_logger().info("%s", mensaje_resultado)


def log_exception(exc: BaseException) -> None:
    """Registra el traceback completo en el log (útil para depuración)."""
    import traceback
    _get_logger().error("Traceback: %s", traceback.format_exc())

