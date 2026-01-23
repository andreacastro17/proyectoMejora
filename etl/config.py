"""
Módulo de configuración centralizado para manejar rutas de archivos.

Este módulo detecta automáticamente si el código se está ejecutando como:
- Script de Python (desarrollo)
- Ejecutable .EXE (distribución)

Y configura las rutas de manera apropiada para cada caso.

También soporta configurar una carpeta raíz del usuario mediante config.json.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def _get_default_base_path() -> Path:
    """
    Obtiene la ruta base por defecto del proyecto.
    
    Si se ejecuta como .EXE, usa la carpeta del ejecutable.
    Si se ejecuta como script, usa la carpeta del proyecto.
    """
    if getattr(sys, 'frozen', False):
        # Ejecutándose como .EXE (PyInstaller)
        # sys.executable es la ruta del .EXE
        base_path = Path(sys.executable).parent
    else:
        # Ejecutándose como script de Python
        # Usar la carpeta del proyecto (dos niveles arriba desde etl/)
        base_path = Path(__file__).resolve().parents[1]
    
    return base_path


def _get_config_file_path() -> Path:
    """Obtiene la ruta del archivo config.json."""
    default_base = _get_default_base_path()
    return default_base / "config.json"


def _load_config() -> dict:
    """
    Carga la configuración desde config.json si existe.
    
    Returns:
        Diccionario con la configuración, o diccionario vacío si no existe.
    """
    config_file = _get_config_file_path()
    
    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"[WARN] No se pudo cargar config.json: {e}. Usando valores por defecto.")
            return {}
    return {}


def _save_config(config: dict) -> bool:
    """
    Guarda la configuración en config.json.
    
    Args:
        config: Diccionario con la configuración a guardar
        
    Returns:
        True si se guardó correctamente, False en caso contrario
    """
    config_file = _get_config_file_path()
    try:
        config_file.parent.mkdir(parents=True, exist_ok=True)
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"[ERROR] No se pudo guardar config.json: {e}")
        return False


def get_base_dir() -> Path:
    """
    Obtiene el directorio base del proyecto.
    
    Si existe 'base_dir' en config.json, usa esa ruta.
    Si no, usa la ruta por defecto (carpeta del ejecutable o del proyecto).
    """
    config = _load_config()
    base_dir_str = config.get("base_dir", "").strip()
    
    if base_dir_str:
        base_dir = Path(base_dir_str)
        if base_dir.exists() and base_dir.is_dir():
            return base_dir
        else:
            print(f"[WARN] El base_dir configurado no existe: {base_dir_str}")
            print(f"[WARN] Usando ruta por defecto.")
    
    return _get_default_base_path()


def set_base_dir(base_dir: Path) -> bool:
    """
    Establece el directorio base del proyecto y lo guarda en config.json.
    
    Args:
        base_dir: Path del directorio base a establecer
        
    Returns:
        True si se guardó correctamente, False en caso contrario
    """
    if not base_dir.exists() or not base_dir.is_dir():
        return False
    
    config = _load_config()
    config["base_dir"] = str(base_dir.resolve())
    return _save_config(config)


# Ruta base del proyecto (se obtiene dinámicamente)
_BASE_PATH = None

def _refresh_base_path():
    """Actualiza la ruta base del proyecto."""
    global _BASE_PATH
    _BASE_PATH = get_base_dir()

_refresh_base_path()

# ========= RUTAS DE DIRECTORIOS =========
# Todas las rutas son relativas a BASE_PATH (que puede ser configurada por el usuario)
def _get_path(config_key: str, default_relative: str) -> Path:
    """
    Obtiene una ruta desde config o usa la ruta relativa por defecto.
    
    Si existe una ruta absoluta en config.json, la usa.
    Si no, construye la ruta relativa a BASE_PATH.
    """
    config = _load_config()
    config_value = config.get(config_key, "").strip()
    if config_value:
        return Path(config_value)
    return _BASE_PATH / default_relative

OUTPUTS_DIR = _get_path("outputs_dir", "outputs")
HISTORIC_DIR = OUTPUTS_DIR / "historico"
REF_DIR = _get_path("ref_dir", "ref")
MODELS_DIR = _get_path("models_dir", "models")
DOCS_DIR = _get_path("docs_dir", "docs")
LOGS_DIR = _get_path("logs_dir", "logs")

# Crear directorios si no existen
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
HISTORIC_DIR.mkdir(parents=True, exist_ok=True)
REF_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# ========= RUTAS DE ARCHIVOS =========
ARCHIVO_PROGRAMAS = OUTPUTS_DIR / "Programas.xlsx"
ARCHIVO_HISTORICO = OUTPUTS_DIR / "HistoricoProgramasNuevos.xlsx"
ARCHIVO_REFERENTES = REF_DIR / "referentesUnificados.xlsx"
ARCHIVO_CATALOGO_EAFIT = REF_DIR / "catalogoOfertasEAFIT.xlsx"
ARCHIVO_NORMALIZACION = DOCS_DIR / "normalizacionFinal.xlsx"

# ========= CONFIGURACIÓN DE DESCARGA SNIES =========
SNIES_URL = "https://hecaa.mineducacion.gov.co/consultaspublicas/programas"
DOWNLOAD_DIR = OUTPUTS_DIR  # Usar el mismo directorio de outputs
RENAME_TO = "Programas"
_CONFIG = _load_config()  # Cargar configuración
HEADLESS = _CONFIG.get("headless", False)
MAX_WAIT_DOWNLOAD_SEC = _CONFIG.get("max_wait_download_sec", 180)

# ========= CONFIGURACIÓN DE HOJAS =========
HOJA_PROGRAMAS = "Programas"
HOJA_HISTORICO = "ProgramasNuevos"

# ========= FUNCIÓN PARA ACTUALIZAR RUTAS =========
def update_paths_for_base_dir(base_dir: Path) -> None:
    """
    Actualiza todas las rutas para usar un nuevo directorio base.
    
    Esta función debe llamarse antes de ejecutar el pipeline si se quiere
    cambiar el directorio base en tiempo de ejecución.
    
    Args:
        base_dir: Nuevo directorio base a usar
    """
    global _BASE_PATH, OUTPUTS_DIR, HISTORIC_DIR, REF_DIR, MODELS_DIR, DOCS_DIR, LOGS_DIR
    global ARCHIVO_PROGRAMAS, ARCHIVO_HISTORICO, ARCHIVO_REFERENTES, ARCHIVO_CATALOGO_EAFIT, ARCHIVO_NORMALIZACION
    
    if not set_base_dir(base_dir):
        raise ValueError(f"No se pudo establecer el directorio base: {base_dir}")
    
    _BASE_PATH = base_dir
    
    # Recalcular todas las rutas
    OUTPUTS_DIR = _get_path("outputs_dir", "outputs")
    HISTORIC_DIR = OUTPUTS_DIR / "historico"
    REF_DIR = _get_path("ref_dir", "ref")
    MODELS_DIR = _get_path("models_dir", "models")
    DOCS_DIR = _get_path("docs_dir", "docs")
    LOGS_DIR = _get_path("logs_dir", "logs")
    
    # Recalcular rutas de archivos
    ARCHIVO_PROGRAMAS = OUTPUTS_DIR / "Programas.xlsx"
    ARCHIVO_HISTORICO = OUTPUTS_DIR / "HistoricoProgramasNuevos.xlsx"
    ARCHIVO_REFERENTES = REF_DIR / "referentesUnificados.xlsx"
    ARCHIVO_CATALOGO_EAFIT = REF_DIR / "catalogoOfertasEAFIT.xlsx"
    ARCHIVO_NORMALIZACION = DOCS_DIR / "normalizacionFinal.xlsx"
    
    # Crear directorios si no existen
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    HISTORIC_DIR.mkdir(parents=True, exist_ok=True)
    REF_DIR.mkdir(parents=True, exist_ok=True)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


# ========= INFORMACIÓN DE DEBUG =========
def print_config_info() -> None:
    """Imprime información de configuración para debugging."""
    print("=== CONFIGURACIÓN DE RUTAS ===")
    print(f"Base Path: {_BASE_PATH}")
    print(f"Outputs Dir: {OUTPUTS_DIR}")
    print(f"Historic Dir: {HISTORIC_DIR}")
    print(f"Ref Dir: {REF_DIR}")
    print(f"Models Dir: {MODELS_DIR}")
    print(f"Docs Dir: {DOCS_DIR}")
    print(f"Logs Dir: {LOGS_DIR}")
    print(f"Archivo Programas: {ARCHIVO_PROGRAMAS}")
    print(f"Ejecutándose como .EXE: {getattr(sys, 'frozen', False)}")
    print("=============================")


if __name__ == "__main__":
    # Ejecutar este script directamente para ver la configuración
    print_config_info()

