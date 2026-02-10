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

import pandas as pd


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


# Caché de configuración para evitar lecturas repetidas del disco
_config_cache: dict | None = None
_config_cache_mtime: float | None = None


def _load_config() -> dict:
    """
    Carga la configuración desde config.json si existe.
    Usa caché en memoria; se invalida al guardar o si cambia la fecha del archivo.
    
    Returns:
        Diccionario con la configuración, o diccionario vacío si no existe.
    """
    global _config_cache, _config_cache_mtime
    config_file = _get_config_file_path()
    if not config_file.exists():
        _config_cache = {}
        _config_cache_mtime = None
        return {}
    try:
        mtime = config_file.stat().st_mtime
        if _config_cache is not None and _config_cache_mtime == mtime:
            return _config_cache
    except OSError:
        pass
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            _config_cache = json.load(f)
            _config_cache_mtime = config_file.stat().st_mtime if config_file.exists() else None
            return _config_cache
    except (json.JSONDecodeError, IOError) as e:
        print(f"[WARN] No se pudo cargar config.json: {e}. Usando valores por defecto.")
        _config_cache = {}
        _config_cache_mtime = None
        return {}


def _save_config(config: dict) -> bool:
    """
    Guarda la configuración en config.json.
    Invalida la caché para que la próxima lectura use el archivo actualizado.
    
    Args:
        config: Diccionario con la configuración a guardar
        
    Returns:
        True si se guardó correctamente, False en caso contrario
    """
    global _config_cache, _config_cache_mtime
    config_file = _get_config_file_path()
    try:
        config_file.parent.mkdir(parents=True, exist_ok=True)
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        _config_cache = None
        _config_cache_mtime = None
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
ARCHIVO_HISTORICO = OUTPUTS_DIR / "HistoricoProgramasNuevos .xlsx"  # Con espacio al final (archivo principal con todos los históricos)


def _resolve_referencia_path(ref_dir: Path, nombre_base: str) -> Path:
    """Resuelve ruta a referentesUnificados o catalogoOfertasEAFIT (.xlsx o .csv).
    Busca en ref_dir y, si no existe, en ref_dir/backup (común cuando los archivos están en ref/backup/).
    """
    for carpeta in (ref_dir, ref_dir / "backup"):
        if not carpeta.exists():
            continue
        for ext in [".xlsx", ".csv"]:
            p = carpeta / f"{nombre_base}{ext}"
            if p.exists():
                return p
    return ref_dir / f"{nombre_base}.xlsx"


ARCHIVO_REFERENTES = _resolve_referencia_path(REF_DIR, "referentesUnificados")
ARCHIVO_CATALOGO_EAFIT = _resolve_referencia_path(REF_DIR, "catalogoOfertasEAFIT")
ARCHIVO_NORMALIZACION = DOCS_DIR / "normalizacionFinal.xlsx"

# ========= CONFIGURACIÓN DE DESCARGA SNIES =========
SNIES_URL = "https://hecaa.mineducacion.gov.co/consultaspublicas/programas"
DOWNLOAD_DIR = OUTPUTS_DIR  # Usar el mismo directorio de outputs
RENAME_TO = "Programas"
_CONFIG = _load_config()  # Cargar configuración
HEADLESS = _CONFIG.get("headless", False)
# Validación de config con valores por defecto y advertencias
_raw_max_wait = _CONFIG.get("max_wait_download_sec", 180)
MAX_WAIT_DOWNLOAD_SEC = max(60, int(_raw_max_wait)) if isinstance(_raw_max_wait, (int, float)) else 180
if isinstance(_raw_max_wait, (int, float)) and int(_raw_max_wait) < 60:
    print("[WARN] config: max_wait_download_sec debe ser >= 60. Usando 60.")
_raw_log = str(_CONFIG.get("log_level", "INFO")).upper()
LOG_LEVEL = _raw_log if _raw_log in ("DEBUG", "INFO", "WARNING", "ERROR") else "INFO"
if _raw_log != LOG_LEVEL:
    print("[WARN] config: log_level inválido. Usando INFO.")
LOG_MAX_BYTES = max(1024 * 1024, int(_CONFIG.get("log_max_bytes", 2 * 1024 * 1024)))
DOWNLOAD_RETRIES = max(1, int(_CONFIG.get("download_retries", 2)))
_raw_timeout = _CONFIG.get("selenium_page_load_timeout_sec", 120)
SELENIUM_PAGE_LOAD_TIMEOUT_SEC = max(30, int(_raw_timeout)) if isinstance(_raw_timeout, (int, float)) else 120
try:
    _raw_umbral = float(_CONFIG.get("umbral_referente", 0.70))
    UMBRAL_REFERENTE = max(0.0, min(1.0, _raw_umbral))
except (TypeError, ValueError):
    UMBRAL_REFERENTE = 0.70

# Limpieza de históricos: umbral configurable
MAX_ARCHIVOS_HISTORICOS = max(5, int(_CONFIG.get("max_archivos_historicos", 20)))

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
    ARCHIVO_HISTORICO = OUTPUTS_DIR / "HistoricoProgramasNuevos .xlsx"  # Con espacio al final (archivo principal con todos los históricos)
    ARCHIVO_NORMALIZACION = DOCS_DIR / "normalizacionFinal.xlsx"
    
    # Funciones para detección automática de formato en archivos de referencia
    def _cargar_archivo_referencia(base_path: Path, nombre_base: str) -> Path:
        """Busca .xlsx o .csv en base_path y, si no existe, en base_path/backup."""
        for carpeta in (base_path, base_path / "backup"):
            if not carpeta.exists():
                continue
            for ext in ['.xlsx', '.csv']:
                archivo = carpeta / f"{nombre_base}{ext}"
                if archivo.exists():
                    return archivo
        raise FileNotFoundError(
            f"No se encontró {nombre_base}.xlsx ni {nombre_base}.csv en {base_path} ni en {base_path / 'backup'}"
        )
    
    def _leer_datos_flexible(ruta: Path, **kwargs) -> pd.DataFrame:
        """
        Lee Excel o CSV automáticamente con manejo robusto.
        """
        if not ruta.exists():
            raise FileNotFoundError(f"No existe el archivo: {ruta}")
        
        suffix = ruta.suffix.lower()
        
        if suffix == '.csv':
            # Manejo robusto de CSV
            encodings = kwargs.pop('encoding', ['utf-8', 'latin-1', 'cp1252'])
            separators = kwargs.pop('sep', [',', ';'])
            
            if isinstance(encodings, str):
                encodings = [encodings]
            if isinstance(separators, str):
                separators = [separators]
            
            # Intentar diferentes combinaciones
            for encoding in encodings:
                for sep in separators:
                    try:
                        return pd.read_csv(ruta, encoding=encoding, sep=sep, **kwargs)
                    except Exception:
                        continue
            
            # Último intento con defaults de pandas
            return pd.read_csv(ruta, **kwargs)
        
        elif suffix in ['.xlsx', '.xls']:
            return pd.read_excel(ruta, **kwargs)
        
        else:
            raise ValueError(f"Formato no soportado: {suffix}")
    
    # Rutas dinámicas para archivos de referencia
    try:
        ARCHIVO_REFERENTES = _cargar_archivo_referencia(REF_DIR, "referentesUnificados")
        ARCHIVO_CATALOGO_EAFIT = _cargar_archivo_referencia(REF_DIR, "catalogoOfertasEAFIT")
    except FileNotFoundError as e:
        print(f"[ERROR] {e}")
        # Fallback a rutas por defecto (para compatibilidad)
        ARCHIVO_REFERENTES = REF_DIR / "referentesUnificados.xlsx"
        ARCHIVO_CATALOGO_EAFIT = REF_DIR / "catalogoOfertasEAFIT.xlsx"
    
    # Crear directorios si no existen
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    HISTORIC_DIR.mkdir(parents=True, exist_ok=True)
    REF_DIR.mkdir(parents=True, exist_ok=True)
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


# Exponer funciones de utilidad para uso en otros módulos
def cargar_archivo_referencia(base_path: Path, nombre_base: str) -> Path:
    """Busca .xlsx o .csv en base_path y, si no existe, en base_path/backup."""
    for carpeta in (base_path, base_path / "backup"):
        if not carpeta.exists():
            continue
        for ext in ['.xlsx', '.csv']:
            archivo = carpeta / f"{nombre_base}{ext}"
            if archivo.exists():
                return archivo
    raise FileNotFoundError(
        f"No se encontró {nombre_base}.xlsx ni {nombre_base}.csv en {base_path} ni en {base_path / 'backup'}"
    )

def leer_datos_flexible(ruta: Path, **kwargs) -> pd.DataFrame:
    """Lee Excel o CSV automáticamente con manejo robusto."""
    if not ruta.exists():
        raise FileNotFoundError(f"No existe el archivo: {ruta}")
    
    suffix = ruta.suffix.lower()
    
    if suffix == '.csv':
        # Manejo robusto de CSV
        encodings = kwargs.pop('encoding', ['utf-8', 'latin-1', 'cp1252'])
        separators = kwargs.pop('sep', [',', ';'])
        
        if isinstance(encodings, str):
            encodings = [encodings]
        if isinstance(separators, str):
            separators = [separators]
        
        # Intentar diferentes combinaciones
        for encoding in encodings:
            for sep in separators:
                try:
                    return pd.read_csv(ruta, encoding=encoding, sep=sep, **kwargs)
                except Exception:
                    continue
        
        # Último intento con defaults de pandas
        return pd.read_csv(ruta, **kwargs)
    
    elif suffix in ['.xlsx', '.xls']:
        return pd.read_excel(ruta, **kwargs)
    
    else:
        raise ValueError(f"Formato no soportado: {suffix}")

# Funciones actualizadas para obtener rutas dinámicas
def get_archivo_referentes() -> Path:
    """Obtiene la ruta al archivo de referentes (xlsx o csv)."""
    return cargar_archivo_referencia(REF_DIR, "referentesUnificados")

def get_archivo_catalogo_eafit() -> Path:
    """Obtiene la ruta al archivo del catálogo EAFIT (xlsx o csv)."""
    return cargar_archivo_referencia(REF_DIR, "catalogoOfertasEAFIT")


# Exponer rutas de config para la GUI (evitar duplicar lógica exe/script)
def get_config_file_path() -> Path:
    """Ruta del archivo config.json (según se ejecute como script o .EXE)."""
    return _get_config_file_path()

def get_default_base_path() -> Path:
    """Ruta base por defecto (carpeta del .exe o del proyecto)."""
    return _get_default_base_path()


# Última ejecución exitosa (guardada en config.json para la GUI)
def get_last_success() -> tuple[str | None, float | None]:
    """
    Obtiene fecha y duración de la última ejecución exitosa del pipeline.
    Returns:
        (iso_timestamp o None, duracion_minutos o None)
    """
    c = _load_config()
    return (c.get("last_success_iso"), c.get("last_success_duration_min"))

def set_last_success(iso_timestamp: str, duration_minutes: float) -> bool:
    """Guarda en config.json la última ejecución exitosa."""
    c = _load_config()
    c["last_success_iso"] = iso_timestamp
    c["last_success_duration_min"] = round(duration_minutes, 2)
    return _save_config(c)

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

