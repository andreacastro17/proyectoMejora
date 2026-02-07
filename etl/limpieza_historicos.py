"""
Módulo para consolidar y limpiar archivos históricos en outputs/historico/.

Cuando hay muchos archivos históricos individuales, se consolidan en
HistoricoProgramasNuevos.xlsx y se eliminan los archivos individuales para
evitar que la carpeta se llene de archivos.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# Agregar el directorio raíz al path para importar módulos
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from etl.pipeline_logger import log_error, log_info, log_resultado
from etl.config import (
    HISTORIC_DIR,
    ARCHIVO_HISTORICO,
    HOJA_PROGRAMAS,
    HOJA_HISTORICO,
    MAX_ARCHIVOS_HISTORICOS,
)

# Umbral: si hay más de N archivos históricos, se consolidan (configurable en config.json)
UMBRAL_CONSOLIDACION = MAX_ARCHIVOS_HISTORICOS


def consolidar_historicos(umbral: int = UMBRAL_CONSOLIDACION) -> tuple[int, int]:
    """
    Consolida archivos históricos en HistoricoProgramasNuevos.xlsx y elimina los archivos consolidados.
    
    Args:
        umbral: Número mínimo de archivos históricos para activar la consolidación.
                Si hay menos archivos, no se hace nada.
    
    Returns:
        Tupla (archivos_consolidados, registros_agregados)
        - archivos_consolidados: Número de archivos históricos consolidados
        - registros_agregados: Número de registros agregados al histórico consolidado
    """
    if not HISTORIC_DIR.exists():
        log_info("No existe el directorio histórico. No hay nada que consolidar.")
        return (0, 0)
    
    # Obtener todos los archivos .xlsx en el directorio histórico
    archivos_historicos = sorted(
        [f for f in HISTORIC_DIR.glob("*.xlsx") if f.is_file()],
        key=lambda x: x.stat().st_mtime  # Ordenar por fecha de modificación (más antiguos primero)
    )
    
    total_archivos = len(archivos_historicos)
    
    if total_archivos < umbral:
        log_info(
            f"Hay {total_archivos} archivos históricos. "
            f"No se requiere consolidación (umbral: {umbral})."
        )
        return (0, 0)
    
    print(f"=== Consolidación de archivos históricos ===")
    print(f"Archivos históricos encontrados: {total_archivos}")
    print(f"Umbral de consolidación: {umbral}")
    print(f"Consolidando archivos...")
    
    log_info(f"Iniciando consolidación de {total_archivos} archivos históricos")
    
    # Leer todos los archivos históricos y consolidar
    registros_consolidados = []
    archivos_procesados = 0
    archivos_con_error = 0
    
    for archivo_historico in archivos_historicos:
        try:
            # Intentar leer con hoja "Programas" (nombre estándar)
            try:
                from etl.exceptions_helpers import leer_excel_con_reintentos
                df = leer_excel_con_reintentos(archivo_historico, sheet_name=HOJA_PROGRAMAS)
            except Exception:
                # Si no tiene hoja "Programas", intentar leer la primera hoja
                try:
                    from etl.exceptions_helpers import leer_excel_con_reintentos
                    df = leer_excel_con_reintentos(archivo_historico, sheet_name=0)
                except Exception as e:
                    print(f"[WARN] No se pudo leer {archivo_historico.name}: {e}")
                    log_error(f"Error al leer {archivo_historico.name}: {e}")
                    archivos_con_error += 1
                    continue
            
            # Verificar que tiene la columna mínima requerida
            if "CÓDIGO_SNIES_DEL_PROGRAMA" not in df.columns:
                print(f"[WARN] {archivo_historico.name} no tiene columna CÓDIGO_SNIES_DEL_PROGRAMA. Se omite.")
                log_error(f"{archivo_historico.name} no tiene estructura válida. Se omite.")
                archivos_con_error += 1
                continue
            
            # Agregar columna con fecha del archivo (basada en nombre o fecha de modificación)
            fecha_mod = archivo_historico.stat().st_mtime
            import time
            fecha_str = time.strftime("%Y-%m-%d", time.localtime(fecha_mod))
            df["FECHA_ARCHIVO_HISTORICO"] = fecha_str
            df["ARCHIVO_ORIGEN"] = archivo_historico.name
            
            registros_consolidados.append(df)
            archivos_procesados += 1
            
        except Exception as e:
            print(f"[ERROR] Error al procesar {archivo_historico.name}: {e}")
            log_error(f"Error al procesar {archivo_historico.name}: {e}")
            archivos_con_error += 1
            continue
    
    if not registros_consolidados:
        print("No se pudieron leer archivos históricos válidos.")
        log_error("No se pudieron leer archivos históricos válidos para consolidar.")
        return (0, 0)
    
    # Consolidar todos los DataFrames
    print(f"Consolidando {len(registros_consolidados)} archivos...")
    df_consolidado = pd.concat(registros_consolidados, ignore_index=True)
    
    # Eliminar duplicados basados en CÓDIGO_SNIES_DEL_PROGRAMA (mantener el más reciente)
    # Si hay FECHA_ARCHIVO_HISTORICO, ordenar por fecha descendente
    if "FECHA_ARCHIVO_HISTORICO" in df_consolidado.columns:
        df_consolidado = df_consolidado.sort_values(
            by="FECHA_ARCHIVO_HISTORICO", ascending=False, na_position="last"
        )
    
    # Eliminar duplicados (mantener el primero, que es el más reciente)
    columnas_unicas = ["CÓDIGO_SNIES_DEL_PROGRAMA"]
    antes_dedup = len(df_consolidado)
    df_consolidado = df_consolidado.drop_duplicates(
        subset=columnas_unicas, keep="first"
    )
    despues_dedup = len(df_consolidado)
    duplicados_eliminados = antes_dedup - despues_dedup
    
    if duplicados_eliminados > 0:
        print(f"Eliminados {duplicados_eliminados} registros duplicados.")
    
    # Leer el histórico consolidado existente (si existe)
    if ARCHIVO_HISTORICO.exists():
        try:
            from etl.exceptions_helpers import leer_excel_con_reintentos
            df_historico_existente = leer_excel_con_reintentos(
                ARCHIVO_HISTORICO, sheet_name=HOJA_HISTORICO
            )
            print(f"Archivo histórico consolidado existente: {len(df_historico_existente)} registros")
            
            # Combinar con el histórico existente (evitar duplicados)
            # Si el histórico existente tiene FECHA, usar esa para ordenar
            df_combinado = pd.concat([df_historico_existente, df_consolidado], ignore_index=True)
            
            # Eliminar duplicados nuevamente (mantener el más reciente)
            if "FECHA" in df_combinado.columns or "FECHA_ARCHIVO_HISTORICO" in df_combinado.columns:
                col_fecha = "FECHA" if "FECHA" in df_combinado.columns else "FECHA_ARCHIVO_HISTORICO"
                df_combinado = df_combinado.sort_values(
                    by=col_fecha, ascending=False, na_position="last"
                )
            
            antes_final = len(df_combinado)
            df_final = df_combinado.drop_duplicates(
                subset=columnas_unicas, keep="first"
            )
            despues_final = len(df_final)
            
            print(f"Combinado con histórico existente. Total único: {despues_final} registros")
            
        except Exception as e:
            print(f"[WARN] Error al leer histórico consolidado existente: {e}. Creando nuevo.")
            log_error(f"Error al leer histórico consolidado: {e}")
            df_final = df_consolidado
    else:
        print("No existe histórico consolidado. Creando nuevo.")
        df_final = df_consolidado
    
    # Guardar el histórico consolidado
    print(f"Guardando histórico consolidado: {ARCHIVO_HISTORICO}")
    try:
        ARCHIVO_HISTORICO.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(
            ARCHIVO_HISTORICO,
            mode="w",
            engine="openpyxl",
        ) as writer:
            df_final.to_excel(
                writer, sheet_name=HOJA_HISTORICO, index=False
            )
        print(f"✓ Histórico consolidado guardado: {len(df_final)} registros")
        log_info(f"Histórico consolidado guardado: {ARCHIVO_HISTORICO.name} ({len(df_final)} registros)")
    except Exception as e:
        error_msg = f"Error al guardar histórico consolidado: {e}"
        print(f"[ERROR] {error_msg}")
        log_error(error_msg)
        raise
    
    # Eliminar los archivos históricos consolidados
    archivos_eliminados = 0
    for archivo_historico in archivos_historicos[:archivos_procesados]:
        try:
            archivo_historico.unlink()
            archivos_eliminados += 1
        except Exception as e:
            print(f"[WARN] No se pudo eliminar {archivo_historico.name}: {e}")
            log_error(f"Error al eliminar {archivo_historico.name}: {e}")
    
    registros_nuevos = len(df_consolidado)
    
    print(f"\n=== Consolidación completada ===")
    print(f"Archivos procesados: {archivos_procesados}")
    print(f"Archivos con error: {archivos_con_error}")
    print(f"Archivos eliminados: {archivos_eliminados}")
    print(f"Registros consolidados: {registros_nuevos}")
    print(f"Total en histórico consolidado: {len(df_final)} registros")
    
    log_resultado(f"Consolidación completada: {archivos_eliminados} archivos eliminados, {registros_nuevos} registros agregados")
    
    return (archivos_eliminados, registros_nuevos)


def limpiar_historicos_automatico(umbral: int = UMBRAL_CONSOLIDACION) -> bool:
    """
    Ejecuta la consolidación automática si se cumple el umbral.
    
    Args:
        umbral: Número mínimo de archivos para activar la consolidación.
    
    Returns:
        True si se ejecutó la consolidación, False si no era necesario.
    """
    archivos_eliminados, registros_agregados = consolidar_historicos(umbral)
    return archivos_eliminados > 0


if __name__ == "__main__":
    # Ejecutar consolidación manual
    print("Ejecutando consolidación manual de archivos históricos...")
    archivos, registros = consolidar_historicos()
    print(f"\nResultado: {archivos} archivos consolidados, {registros} registros agregados.")
