"""
Módulo de exportación de datos para Power BI.

Prepara los datos de Programas.xlsx en formato optimizado para visualización en Power BI,
incluyendo métricas calculadas y datos detallados de programas nuevos y referentes.

Arquitectura preparada para conexión directa a Power BI cuando se configure el link.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

# Import lazy de pandas (solo cuando se necesita)
if TYPE_CHECKING:
    import pandas as pd

from etl.config import OUTPUTS_DIR, ARCHIVO_PROGRAMAS, HOJA_PROGRAMAS
from etl.exceptions_helpers import leer_excel_con_reintentos
from etl.pipeline_logger import log_info, log_error, log_warning, log_resultado


# Archivo de salida para Power BI
ARCHIVO_POWER_BI = OUTPUTS_DIR / "Programas_PowerBI.xlsx"


def preparar_datos_powerbi(
    archivo_programas: Path | None = None,
    df_programas: "pd.DataFrame | None" = None,
    log_callback=None
) -> tuple["pd.DataFrame", dict]:
    """
    Prepara los datos de Programas.xlsx para Power BI.
    
    Genera:
    1. DataFrame con programas nuevos filtrados y columnas optimizadas
    2. Diccionario con métricas calculadas
    
    Args:
        archivo_programas: Ruta al archivo Programas.xlsx (opcional si se pasa df_programas)
        df_programas: DataFrame con los datos (opcional si se pasa archivo_programas)
        log_callback: Función para logging (opcional)
        
    Returns:
        Tupla (df_powerbi, metricas) donde:
        - df_powerbi: DataFrame preparado para Power BI
        - metricas: Diccionario con métricas calculadas
        
    Raises:
        FileNotFoundError: Si no se puede leer el archivo
        ValueError: Si faltan columnas requeridas
    """
    def log(msg: str):
        if log_callback:
            log_callback(msg)
        else:
            log_info(msg)
    
    # Import pandas aquí (lazy import)
    import pandas as pd
    
    # Cargar datos
    if df_programas is not None:
        df = df_programas.copy()
        log("Usando DataFrame proporcionado en memoria")
    else:
        archivo = archivo_programas or ARCHIVO_PROGRAMAS
        if not archivo.exists():
            raise FileNotFoundError(f"No se encontró el archivo: {archivo}")
        
        log(f"Leyendo datos desde {archivo.name}...")
        df = leer_excel_con_reintentos(archivo, sheet_name=HOJA_PROGRAMAS)
    
    # Validar columnas requeridas
    columnas_requeridas = [
        "CÓDIGO_SNIES_DEL_PROGRAMA",
        "NOMBRE_INSTITUCIÓN",
        "NOMBRE_DEL_PROGRAMA",
        "PROGRAMA_NUEVO"
    ]
    faltantes = [c for c in columnas_requeridas if c not in df.columns]
    if faltantes:
        raise ValueError(f"Faltan columnas requeridas: {faltantes}")
    
    # Filtrar solo programas nuevos
    mask_nuevos = df["PROGRAMA_NUEVO"].astype(str).str.strip().str.upper() == "SÍ"
    df_nuevos = df[mask_nuevos].copy()
    
    log(f"Programas nuevos encontrados: {len(df_nuevos)}")
    
    # Preparar DataFrame para Power BI con columnas renombradas y optimizadas
    df_powerbi = pd.DataFrame()
    
    # Columnas básicas (renombradas para Power BI)
    df_powerbi["Codigo SNIES"] = df_nuevos["CÓDIGO_SNIES_DEL_PROGRAMA"].astype(str)
    df_powerbi["NOMBRE_INSTITUCIÓN"] = df_nuevos["NOMBRE_INSTITUCIÓN"].fillna("").astype(str)
    df_powerbi["Programa IE"] = df_nuevos["NOMBRE_DEL_PROGRAMA"].fillna("").astype(str)
    
    # Columna Es Referente? (formato Sí/No para Power BI)
    if "ES_REFERENTE" in df_nuevos.columns:
        df_powerbi["Es Referente?"] = df_nuevos["ES_REFERENTE"].fillna("No").astype(str)
        # Normalizar valores
        df_powerbi["Es Referente?"] = df_powerbi["Es Referente?"].str.strip().str.upper()
        df_powerbi["Es Referente?"] = df_powerbi["Es Referente?"].map({
            "SÍ": "Sí",
            "SI": "Sí",
            "YES": "Sí",
            "TRUE": "Sí",
            "1": "Sí"
        }).fillna("No")
    else:
        df_powerbi["Es Referente?"] = "No"
        log_warning("Columna ES_REFERENTE no encontrada, todos los valores serán 'No'")
    
    # Columna Programa EAFIT (para filtro en Power BI)
    if "PROGRAMA_EAFIT_NOMBRE" in df_nuevos.columns:
        df_powerbi["Programa EAFIT"] = df_nuevos["PROGRAMA_EAFIT_NOMBRE"].fillna("").astype(str)
    else:
        df_powerbi["Programa EAFIT"] = ""
        log_warning("Columna PROGRAMA_EAFIT_NOMBRE no encontrada")
    
    # Columnas adicionales útiles para análisis (opcionales)
    columnas_adicionales = {
        "PROBABILIDAD": "Probabilidad",
        "NIVEL_DE_FORMACIÓN": "Nivel Formación",
        "CINE_F_2013_AC_CAMPO_AMPLIO": "Campo Amplio",
        "PROGRAMA_EAFIT_CODIGO": "Codigo EAFIT",
        "SIMILITUD_EMBEDDING": "Similitud Embedding",
        "AJUSTE_MANUAL": "Ajuste Manual"
    }
    
    for col_origen, col_destino in columnas_adicionales.items():
        if col_origen in df_nuevos.columns:
            df_powerbi[col_destino] = df_nuevos[col_origen]
        else:
            # Si no existe, crear columna vacía del tipo apropiado
            if col_origen == "PROBABILIDAD" or col_origen == "SIMILITUD_EMBEDDING":
                df_powerbi[col_destino] = 0.0
            elif col_origen == "AJUSTE_MANUAL":
                df_powerbi[col_destino] = False
            else:
                df_powerbi[col_destino] = ""
    
    # Calcular métricas
    total_programas_nuevos = len(df_powerbi)
    total_referentes = len(df_powerbi[df_powerbi["Es Referente?"] == "Sí"])
    
    metricas = {
        "Programas Nuevos Detectados": total_programas_nuevos,
        "Referentes Nuevos Detectados": total_referentes,
        "Porcentaje Referentes": (total_referentes / total_programas_nuevos * 100) if total_programas_nuevos > 0 else 0.0,
        "Fecha Exportación": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    
    log(f"Métricas calculadas:")
    log(f"  - Programas Nuevos Detectados: {total_programas_nuevos}")
    log(f"  - Referentes Nuevos Detectados: {total_referentes}")
    
    return df_powerbi, metricas


def exportar_a_powerbi(
    archivo_programas: Path | None = None,
    df_programas: "pd.DataFrame | None" = None,
    archivo_salida: Path | None = None,
    log_callback=None
) -> Path:
    """
    Deprecado: La integración con Power BI se realiza ahora vía Dataflows,
    que se conectan directamente al archivo maestro Programas.xlsx.
    Esta función se mantiene por compatibilidad de imports; no genera archivos Excel.
    """
    # Integración ahora vía Power BI Dataflows leyendo Programas.xlsx directamente.
    pass
    return ARCHIVO_PROGRAMAS


def conectar_powerbi_directo(
    archivo_programas: Path | None = None,
    df_programas: "pd.DataFrame | None" = None,
    powerbi_url: str | None = None,
    log_callback=None
) -> bool:
    """
    Conecta directamente con Power BI y actualiza el dataset.
    
    NOTA: Esta función está preparada pero requiere configuración del link de Power BI.
    Para habilitarla:
    1. Instalar: pip install powerbiclient
    2. Configurar autenticación (Service Principal o OAuth)
    3. Obtener el link del dataset de Power BI
    4. Configurar en config.json: "powerbi_dataset_id" y "powerbi_workspace_id"
    
    Args:
        archivo_programas: Ruta al archivo Programas.xlsx (opcional si se pasa df_programas)
        df_programas: DataFrame con los datos (opcional si se pasa archivo_programas)
        powerbi_url: URL del dataset de Power BI (opcional, se lee de config si no se proporciona)
        log_callback: Función para logging (opcional)
        
    Returns:
        True si la conexión fue exitosa, False en caso contrario
        
    Raises:
        NotImplementedError: Si la funcionalidad aún no está configurada
    """
    def log(msg: str):
        if log_callback:
            log_callback(msg)
        else:
            log_info(msg)
    
    # Por ahora, esta funcionalidad no está implementada
    # Se deja como placeholder para futura implementación
    
    log("[INFO] Conexión directa a Power BI no configurada aún.")
    log("[INFO] Los datos se exportan a outputs/Programas_PowerBI.xlsx")
    log("[INFO] Puedes cargar este archivo manualmente en Power BI o configurar la conexión directa.")
    
    # TODO: Implementar cuando se tenga el link de Power BI
    # Ejemplo de implementación futura:
    # 
    # try:
    #     from powerbiclient import Report, models
    #     from etl.config import _load_config
    #     
    #     config = _load_config()
    #     dataset_id = powerbi_url or config.get("powerbi_dataset_id")
    #     workspace_id = config.get("powerbi_workspace_id")
    #     
    #     if not dataset_id:
    #         log("[WARN] No se configuró powerbi_dataset_id en config.json")
    #         return False
    #     
    #     # Preparar datos
    #     df_powerbi, metricas = preparar_datos_powerbi(
    #         archivo_programas=archivo_programas,
    #         df_programas=df_programas,
    #         log_callback=log
    #     )
    #     
    #     # Conectar y actualizar dataset
    #     # ... código de conexión ...
    #     
    #     log("✓ Datos actualizados en Power BI")
    #     return True
    #     
    # except ImportError:
    #     log("[ERROR] powerbiclient no está instalado. Instala con: pip install powerbiclient")
    #     return False
    # except Exception as e:
    #     log(f"[ERROR] Error al conectar con Power BI: {e}")
    #     return False
    
    return False


def main(log_callback=None) -> Path | None:
    """
    Función principal para ejecutar la exportación a Power BI.
    
    Args:
        log_callback: Función para logging (opcional)
        
    Returns:
        Path del archivo generado, o None si falló
    """
    try:
        archivo_generado = exportar_a_powerbi(log_callback=log_callback)
        return archivo_generado
    except Exception as e:
        if log_callback:
            log_callback(f"[ERROR] Exportación Power BI falló: {e}")
        log_error(f"Exportación Power BI falló: {e}")
        return None


if __name__ == "__main__":
    # Ejecución directa del módulo
    resultado = main()
    if resultado:
        print(f"✓ Exportación completada: {resultado}")
    else:
        print("✗ Exportación falló. Revisa los logs.")
