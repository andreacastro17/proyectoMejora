"""
Script para comparar el archivo actual de Programas.xlsx con el último archivo histórico
y marcar los programas nuevos mediante la columna CÓDIGO_SNIES_DEL_PROGRAMA.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from etl.pipeline_logger import log_error, log_info, log_resultado, log_warning
from etl.config import ARCHIVO_PROGRAMAS, HISTORIC_DIR, HOJA_PROGRAMAS
from etl.exceptions_helpers import (
    leer_excel_con_reintentos,
    escribir_excel_con_reintentos,
    validar_excel_basico,
    explicar_error_archivo_abierto,
)

COLUMNA_ID = "CÓDIGO_SNIES_DEL_PROGRAMA"
COLUMNA_NUEVO = "PROGRAMA_NUEVO"


def obtener_ultimo_archivo_historico(directorio: Path) -> Path | None:
    """
    Obtiene el archivo más reciente en el directorio histórico basado en la fecha de modificación.
    
    Args:
        directorio: Ruta al directorio que contiene los archivos históricos
        
    Returns:
        Path al archivo más reciente o None si no hay archivos
    """
    if not directorio.exists():
        return None
    
    archivos_xlsx = list(directorio.glob("*.xlsx"))
    if not archivos_xlsx:
        return None
    
    # Ordenar por fecha de modificación (más reciente primero)
    archivos_xlsx.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return archivos_xlsx[0]


def procesar_programas_nuevos(df: pd.DataFrame | None = None, archivo: Path | None = None) -> pd.DataFrame:
    """
    Compara el archivo actual de Programas.xlsx con el último archivo histórico
    y agrega una columna indicando si el programa es nuevo o no.
    
    Args:
        df: DataFrame opcional. Si se proporciona, se procesa en memoria sin leer/escribir archivo.
        archivo: Archivo opcional. Si df es None, se lee desde este archivo (o ARCHIVO_PROGRAMAS por defecto).
        
    Returns:
        DataFrame procesado
        
    Si df es None, lee desde archivo y escribe de vuelta.
    Si df se proporciona, solo procesa y retorna (sin I/O).
    """
    # Si se proporciona DataFrame, trabajar en memoria
    if df is not None:
        df_actual = df.copy()
        log_info(f"Procesando DataFrame en memoria ({len(df_actual)} filas)")
    else:
        # Modo tradicional: leer desde archivo
        archivo = archivo or ARCHIVO_PROGRAMAS
        if not archivo.exists():
            error_msg = f"No se encontró el archivo: {archivo}"
            log_error(error_msg)
            raise FileNotFoundError(error_msg)
        
        # Validar y leer el archivo actual con manejo robusto
        print(f"Leyendo archivo actual: {archivo}")
        es_valido, msg_error = validar_excel_basico(archivo)
        if not es_valido:
            log_error(f"Validación fallida del archivo actual: {msg_error}")
            raise ValueError(msg_error)
        
        try:
            df_actual = leer_excel_con_reintentos(archivo, sheet_name=HOJA_PROGRAMAS)
            log_info(f"Archivo actual cargado: {archivo.name}")
        except PermissionError as e:
            error_msg = explicar_error_archivo_abierto(archivo, "leer")
            log_error(error_msg)
            raise PermissionError(error_msg) from e
    
    # Eliminar filas vacías (donde todas las columnas son nulas)
    filas_antes = len(df_actual)
    df_actual = df_actual.dropna(how='all')
    filas_despues = len(df_actual)
    if filas_antes != filas_despues:
        print(f"Se eliminaron {filas_antes - filas_despues} filas vacías del archivo actual.")
    
    # Verificar que existe la columna de ID
    if COLUMNA_ID not in df_actual.columns:
        raise ValueError(
            f"No se encontró la columna '{COLUMNA_ID}' en el archivo actual"
        )
    
    # Eliminar filas donde CÓDIGO_SNIES_DEL_PROGRAMA está vacío (incluye notas y filas sin código)
    filas_antes_codigo = len(df_actual)
    df_actual = df_actual.dropna(subset=[COLUMNA_ID])
    filas_despues_codigo = len(df_actual)
    if filas_antes_codigo != filas_despues_codigo:
        print(
            f"Se eliminaron {filas_antes_codigo - filas_despues_codigo} filas sin código SNIES "
            "(incluyendo notas y filas vacías en la columna de identificación)."
        )

    # No crear ni mantener columnas deprecadas (FUENTE_DATOS, MATCH_SCORE, COINCIDE_HISTORICO, REQUIERE_VALIDACION)
    columnas_deprecadas = ["FUENTE_DATOS", "MATCH_SCORE", "COINCIDE_HISTORICO", "REQUIERE_VALIDACION"]
    presentes = [c for c in columnas_deprecadas if c in df_actual.columns]
    if presentes:
        df_actual = df_actual.drop(columns=presentes)

    # OPTIMIZACIÓN: Normalizar código usando operaciones vectorizadas en lugar de .apply()
    # Normalizar: convertir a string, eliminar espacios, mayúsculas, y quitar .0 de números
    codigos_str = df_actual[COLUMNA_ID].fillna("").astype(str)
    codigos_normalizados = codigos_str.str.strip().str.upper()
    # Remover .0 al final usando operación vectorizada
    codigos_normalizados = codigos_normalizados.str.replace(r'\.0$', '', regex=True)
    df_actual['_codigo_normalizado'] = codigos_normalizados
 
    # Obtener el último archivo histórico
    archivo_historico = obtener_ultimo_archivo_historico(HISTORIC_DIR)

    if archivo_historico is None:
        df_actual[COLUMNA_NUEVO] = "Sí"
        df_actual = df_actual.drop(columns=['_codigo_normalizado'])

        total = len(df_actual)
        log_resultado(f"Filas procesadas: {total} (sin histórico)")
        # Modo memoria: el pipeline escribe después; retornar DataFrame
        if df is not None:
            return df_actual
        print(f"Guardando archivo actualizado: {ARCHIVO_PROGRAMAS}")
        try:
            escribir_excel_con_reintentos(ARCHIVO_PROGRAMAS, df_actual, sheet_name=HOJA_PROGRAMAS)
            log_info(f"Archivo actualizado guardado: {ARCHIVO_PROGRAMAS.name}")
            print("Procesamiento completado exitosamente.")
        except PermissionError as e:
            error_msg = explicar_error_archivo_abierto(ARCHIVO_PROGRAMAS, "escribir")
            log_error(error_msg)
            raise PermissionError(error_msg) from e
        return
    
    print(f"Leyendo archivo histórico: {archivo_historico.name}")
    print(f"Ruta completa: {archivo_historico}")
    log_info(f"Archivo histórico cargado: {archivo_historico.name}")
    # Validar y leer el archivo histórico con manejo robusto
    es_valido_hist, msg_error_hist = validar_excel_basico(archivo_historico)
    if not es_valido_hist:
        log_warning(f"El archivo histórico {archivo_historico.name} no es válido: {msg_error_hist}")
        log_warning("Marcando todos los programas como nuevos (sin histórico válido).")
        df_actual[COLUMNA_NUEVO] = "Sí"
        df_actual = df_actual.drop(columns=['_codigo_normalizado'])
        if df is not None:
            return df_actual
        try:
            escribir_excel_con_reintentos(ARCHIVO_PROGRAMAS, df_actual, sheet_name=HOJA_PROGRAMAS)
            log_info(f"Archivo actualizado guardado: {ARCHIVO_PROGRAMAS.name}")
        except PermissionError as e:
            error_msg = explicar_error_archivo_abierto(ARCHIVO_PROGRAMAS, "escribir")
            log_error(error_msg)
            raise PermissionError(error_msg) from e
        return
    
    try:
        df_historico = leer_excel_con_reintentos(archivo_historico, sheet_name=HOJA_PROGRAMAS)
    except PermissionError:
        log_warning(f"El archivo histórico {archivo_historico.name} está abierto. Marcando todos como nuevos.")
        df_actual[COLUMNA_NUEVO] = "Sí"
        df_actual = df_actual.drop(columns=['_codigo_normalizado'])
        if df is not None:
            return df_actual
        try:
            escribir_excel_con_reintentos(ARCHIVO_PROGRAMAS, df_actual, sheet_name=HOJA_PROGRAMAS)
            log_info(f"Archivo actualizado guardado: {ARCHIVO_PROGRAMAS.name}")
        except PermissionError as e:
            error_msg = explicar_error_archivo_abierto(ARCHIVO_PROGRAMAS, "escribir")
            log_error(error_msg)
            raise PermissionError(error_msg) from e
        return
    
    # Eliminar filas vacías (donde todas las columnas son nulas)
    filas_antes_hist = len(df_historico)
    df_historico = df_historico.dropna(how='all')
    filas_despues_hist = len(df_historico)
    if filas_antes_hist != filas_despues_hist:
        print(f"Se eliminaron {filas_antes_hist - filas_despues_hist} filas vacías del archivo histórico.")
    
    # Verificar que existe la columna de ID en el histórico
    if COLUMNA_ID not in df_historico.columns:
        df_actual[COLUMNA_NUEVO] = "Sí"
        df_actual = df_actual.drop(columns=['_codigo_normalizado'])

        total = len(df_actual)
        log_resultado(f"Filas procesadas: {total} (histórico sin columna de ID)")
        
        # Si se está trabajando en memoria (df proporcionado), retornar el DataFrame
        if df is not None:
            return df_actual
        
        # Si se está trabajando con archivo, escribir y retornar None (no se usa)
        print(f"Guardando archivo actualizado: {ARCHIVO_PROGRAMAS}")
        try:
            escribir_excel_con_reintentos(ARCHIVO_PROGRAMAS, df_actual, sheet_name=HOJA_PROGRAMAS)
            log_info(f"Archivo actualizado guardado: {ARCHIVO_PROGRAMAS.name}")
            print("Procesamiento completado exitosamente.")
        except PermissionError as e:
            error_msg = explicar_error_archivo_abierto(ARCHIVO_PROGRAMAS, "escribir")
            log_error(error_msg)
            raise PermissionError(error_msg) from e
        return None
    
    # Eliminar filas donde CÓDIGO_SNIES_DEL_PROGRAMA está vacío en el histórico
    df_historico = df_historico.dropna(subset=[COLUMNA_ID])
    
    # OPTIMIZACIÓN: Normalizar códigos históricos usando operaciones vectorizadas
    # Obtener el conjunto de IDs del archivo histórico
    codigos_hist_str = df_historico[COLUMNA_ID].dropna().astype(str)
    ids_historicos_raw = codigos_hist_str.str.strip().str.upper().str.replace(r'\.0$', '', regex=True)
    # Filtrar strings "nan", "none", etc. que pueden aparecer de conversiones
    ids_historicos = {
        codigo for codigo in ids_historicos_raw 
        if codigo and codigo not in ('NAN', 'NONE', 'NULL', '') and not codigo.isspace()
    }
    
    print(f"Total de códigos SNIES en el archivo histórico: {len(ids_historicos)}")
    if len(ids_historicos) > 0:
        print(f"Ejemplo de códigos históricos (primeros 5): {list(ids_historicos)[:5]}")
 
    # OPTIMIZACIÓN: Usar operación vectorizada en lugar de .apply()
    # Crear máscara vectorizada para programas nuevos
    codigos_norm = df_actual['_codigo_normalizado']
    mask_valido = (
        codigos_norm.notna() & 
        (codigos_norm != '') & 
        ~codigos_norm.isin(['NAN', 'NONE', 'NULL']) &
        ~codigos_norm.str.isspace()
    )
    mask_nuevo = ~codigos_norm.isin(ids_historicos)
    df_actual[COLUMNA_NUEVO] = (mask_valido & mask_nuevo).map({True: "Sí", False: "No"})

    # Eliminar la columna temporal de normalización
    df_actual = df_actual.drop(columns=['_codigo_normalizado'])
    
    # Contar programas nuevos
    nuevos = (df_actual[COLUMNA_NUEVO] == "Sí").sum()
    existentes = (df_actual[COLUMNA_NUEVO] == "No").sum()
    total = len(df_actual)
    print(f"Total de programas procesados: {total}")
    print(f"Programas nuevos: {nuevos}")
    print(f"Programas existentes: {existentes}")

    log_resultado(f"Total de programas procesados: {total}")
    log_resultado(f"Nuevos programas detectados: {nuevos}")
    log_resultado(f"Programas existentes: {existentes}")

    # Debug: mostrar algunos ejemplos de códigos nuevos si hay
    if nuevos > 0:
        nuevos_codigos = df_actual[df_actual[COLUMNA_NUEVO] == "Sí"][COLUMNA_ID].head(5).tolist()
        print(f"Ejemplos de códigos marcados como nuevos (primeros 5): {nuevos_codigos}")
    
    # Si se está trabajando en memoria (df proporcionado), retornar el DataFrame sin escribir
    if df is not None:
        return df_actual
    
    # Si se está trabajando con archivo, escribir y no retornar nada (comportamiento legacy)
    print(f"Guardando archivo actualizado: {ARCHIVO_PROGRAMAS}")
    try:
        escribir_excel_con_reintentos(ARCHIVO_PROGRAMAS, df_actual, sheet_name=HOJA_PROGRAMAS)
        log_info(f"Archivo actualizado guardado: {ARCHIVO_PROGRAMAS.name}")
        print("Procesamiento completado exitosamente.")
    except PermissionError as e:
        error_msg = explicar_error_archivo_abierto(ARCHIVO_PROGRAMAS, "escribir")
        log_error(error_msg)
        raise PermissionError(error_msg) from e


if __name__ == "__main__":
    procesar_programas_nuevos()

