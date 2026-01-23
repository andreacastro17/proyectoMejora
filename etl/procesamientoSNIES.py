"""
Script para comparar el archivo actual de Programas.xlsx con el último archivo histórico
y marcar los programas nuevos mediante la columna CÓDIGO_SNIES_DEL_PROGRAMA.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from etl.pipeline_logger import log_error, log_info, log_resultado
from etl.config import ARCHIVO_PROGRAMAS, HISTORIC_DIR, HOJA_PROGRAMAS

# Rutas de configuración (usando alias para compatibilidad con código existente)
ARCHIVO_PROGRAMAS_ACTUAL = ARCHIVO_PROGRAMAS
DIRECTORIO_HISTORICO = HISTORIC_DIR
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


def procesar_programas_nuevos() -> None:
    """
    Compara el archivo actual de Programas.xlsx con el último archivo histórico
    y agrega una columna indicando si el programa es nuevo o no.
    """
    # Verificar que existe el archivo actual
    if not ARCHIVO_PROGRAMAS_ACTUAL.exists():
        error_msg = f"No se encontró el archivo: {ARCHIVO_PROGRAMAS_ACTUAL}"
        log_error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # Leer el archivo actual
    print(f"Leyendo archivo actual: {ARCHIVO_PROGRAMAS_ACTUAL}")
    df_actual = pd.read_excel(ARCHIVO_PROGRAMAS_ACTUAL, sheet_name=HOJA_PROGRAMAS)
    log_info(f"Archivo actual cargado: {ARCHIVO_PROGRAMAS_ACTUAL.name}")
    
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
    
    # Obtener el último archivo histórico
    archivo_historico = obtener_ultimo_archivo_historico(DIRECTORIO_HISTORICO)
    
    if archivo_historico is None:
        warning_msg = "No se encontró ningún archivo histórico. No se realizará ningún procesamiento."
        print(warning_msg)
        log_info(warning_msg)
        return
    
    print(f"Leyendo archivo histórico: {archivo_historico.name}")
    print(f"Ruta completa: {archivo_historico}")
    log_info(f"Archivo histórico cargado: {archivo_historico.name}")
    # Leer el archivo histórico
    df_historico = pd.read_excel(archivo_historico, sheet_name=HOJA_PROGRAMAS)
    
    # Eliminar filas vacías (donde todas las columnas son nulas)
    filas_antes_hist = len(df_historico)
    df_historico = df_historico.dropna(how='all')
    filas_despues_hist = len(df_historico)
    if filas_antes_hist != filas_despues_hist:
        print(f"Se eliminaron {filas_antes_hist - filas_despues_hist} filas vacías del archivo histórico.")
    
    # Verificar que existe la columna de ID en el histórico
    if COLUMNA_ID not in df_historico.columns:
        print(
            f"Advertencia: No se encontró la columna '{COLUMNA_ID}' en el archivo histórico. "
            "No se realizará ningún procesamiento."
        )
        return
    
    # Eliminar filas donde CÓDIGO_SNIES_DEL_PROGRAMA está vacío en el histórico
    df_historico = df_historico.dropna(subset=[COLUMNA_ID])
    
    # Obtener el conjunto de IDs del archivo histórico
    # Normalizar: convertir a string, eliminar espacios, mayúsculas, y quitar .0 de números
    def normalizar_codigo(valor) -> str:
        """Normaliza un código SNIES para comparación."""
        if pd.isna(valor):
            return ""
        # Convertir a string y limpiar
        codigo = str(valor).strip().upper()
        # Si termina en .0 (de conversión float), quitarlo
        if codigo.endswith('.0'):
            codigo = codigo[:-2]
        return codigo
    
    ids_historicos_raw = df_historico[COLUMNA_ID].dropna().apply(normalizar_codigo)
    # Filtrar strings "nan", "none", etc. que pueden aparecer de conversiones
    ids_historicos = {
        codigo for codigo in ids_historicos_raw 
        if codigo and codigo not in ('NAN', 'NONE', 'NULL', '') and not codigo.isspace()
    }
    
    print(f"Total de códigos SNIES en el archivo histórico: {len(ids_historicos)}")
    if len(ids_historicos) > 0:
        print(f"Ejemplo de códigos históricos (primeros 5): {list(ids_historicos)[:5]}")
    
    # Normalizar los códigos del archivo actual de la misma manera para comparar
    df_actual['_codigo_normalizado'] = df_actual[COLUMNA_ID].apply(normalizar_codigo)
    
    # Crear la columna de programas nuevos
    # Si el ID normalizado no está en el histórico, es nuevo (Sí), si está, no es nuevo (No)
    # Filtrar también valores inválidos como "nan", "none", etc.
    def es_valido_y_nuevo(codigo: str) -> str:
        if not codigo or codigo in ('NAN', 'NONE', 'NULL', '') or codigo.isspace():
            return "No"  # Códigos inválidos no se consideran nuevos
        return "Sí" if codigo not in ids_historicos else "No"
    
    df_actual[COLUMNA_NUEVO] = df_actual['_codigo_normalizado'].apply(es_valido_y_nuevo)
    
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
    
    # Guardar el archivo actualizado
    print(f"Guardando archivo actualizado: {ARCHIVO_PROGRAMAS_ACTUAL}")
    with pd.ExcelWriter(
        ARCHIVO_PROGRAMAS_ACTUAL,
        mode="a",
        if_sheet_exists="replace",
        engine="openpyxl",
    ) as writer:
        df_actual.to_excel(writer, sheet_name=HOJA_PROGRAMAS, index=False)
    
    log_info(f"Archivo actualizado guardado: {ARCHIVO_PROGRAMAS_ACTUAL.name}")
    print("Procesamiento completado exitosamente.")


if __name__ == "__main__":
    procesar_programas_nuevos()

