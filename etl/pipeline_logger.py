"""
Módulo de logging centralizado para el pipeline SNIES.

Registra todos los eventos del pipeline en logs/pipeline.log con formato:
[YYYY-MM-DD HH:MM:SS] MENSAJE
"""

from __future__ import annotations

import datetime
from pathlib import Path


# Importar configuración centralizada
from etl.config import LOGS_DIR

# Ruta del archivo de log
LOG_FILE = LOGS_DIR / "pipeline.log"

# Asegurar que el directorio logs existe
LOG_FILE.parent.mkdir(exist_ok=True)


def _formatear_timestamp() -> str:
    """
    Formatea el timestamp actual en formato [YYYY-MM-DD HH:MM:SS].
    
    Returns:
        String con el timestamp formateado
    """
    return datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def log_inicio() -> None:
    """Registra el inicio de ejecución del pipeline."""
    mensaje = f"{_formatear_timestamp()} INICIO: ejecución del pipeline."
    _escribir_log(mensaje)


def log_fin(duracion_minutos: float | None = None) -> None:
    """
    Registra el fin de ejecución del pipeline.
    
    Args:
        duracion_minutos: Duración de la ejecución en minutos (opcional)
    """
    if duracion_minutos is not None:
        # Formatear duración: mostrar como entero si es un número entero, sino con 1 decimal
        if duracion_minutos == int(duracion_minutos):
            duracion_str = f"{int(duracion_minutos)}m"
        else:
            duracion_str = f"{duracion_minutos:.1f}m"
        mensaje = f"{_formatear_timestamp()} FIN: ejecución completa (duración: {duracion_str})."
    else:
        mensaje = f"{_formatear_timestamp()} FIN: ejecución completa."
    _escribir_log(mensaje)


def log_etapa_iniciada(nombre_etapa: str) -> None:
    """
    Registra el inicio de una etapa del pipeline.
    
    Args:
        nombre_etapa: Nombre de la etapa (ej: "Descarga SNIES", "Normalización")
    """
    mensaje = f"{_formatear_timestamp()} {nombre_etapa} iniciada..."
    _escribir_log(mensaje)


def log_etapa_completada(nombre_etapa: str, detalles: str | None = None) -> None:
    """
    Registra la finalización exitosa de una etapa.
    
    Args:
        nombre_etapa: Nombre de la etapa
        detalles: Información adicional opcional (ej: nombre de archivo generado)
    """
    if detalles:
        mensaje = f"{_formatear_timestamp()} {nombre_etapa} finalizada: {detalles}"
    else:
        mensaje = f"{_formatear_timestamp()} {nombre_etapa} finalizada."
    _escribir_log(mensaje)


def log_error(mensaje_error: str) -> None:
    """
    Registra un error.
    
    Args:
        mensaje_error: Mensaje de error descriptivo
    """
    mensaje = f"{_formatear_timestamp()} ERROR: {mensaje_error}"
    _escribir_log(mensaje)


def log_warning(mensaje_warning: str) -> None:
    """
    Registra una advertencia.
    
    Args:
        mensaje_warning: Mensaje de advertencia
    """
    mensaje = f"{_formatear_timestamp()} WARNING: {mensaje_warning}"
    _escribir_log(mensaje)


def log_info(mensaje_info: str) -> None:
    """
    Registra información general.
    
    Args:
        mensaje_info: Mensaje informativo
    """
    mensaje = f"{_formatear_timestamp()} {mensaje_info}"
    _escribir_log(mensaje)


def log_resultado(mensaje_resultado: str) -> None:
    """
    Registra un resultado o KPI.
    
    Args:
        mensaje_resultado: Mensaje con el resultado (ej: "Nuevos programas detectados: 3")
    """
    mensaje = f"{_formatear_timestamp()} {mensaje_resultado}"
    _escribir_log(mensaje)


def _escribir_log(mensaje: str) -> None:
    """
    Escribe un mensaje en el archivo de log.
    
    Args:
        mensaje: Mensaje a escribir
    """
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(mensaje + "\n")
    except Exception as e:
        # Si falla el logging, imprimir en consola como fallback
        print(f"[ERROR al escribir log] {e}")
        print(f"[Mensaje que se intentó registrar] {mensaje}")

