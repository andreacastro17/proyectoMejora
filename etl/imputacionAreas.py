"""
Módulo para imputar valores faltantes en columnas categóricas usando KNN con embeddings semánticos.

Este módulo utiliza SentenceTransformer para generar embeddings de los nombres de programas
y KNN (K-Nearest Neighbors) para encontrar programas similares que tienen valores asignados,
asignando esos valores a los programas con valores faltantes.

Actualmente implementado para:
- ÁREA_DE_CONOCIMIENTO

Diseñado para ser extensible a otras columnas en el futuro (ej. NÚCLEO_BÁSICO_DEL_CONOCIMIENTO).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from sklearn.neighbors import KNeighborsClassifier

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer

from etl.pipeline_logger import log_error, log_info, log_warning
from etl.config import ARCHIVO_PROGRAMAS, HOJA_PROGRAMAS
from etl.exceptions_helpers import (
    leer_excel_con_reintentos,
    escribir_excel_con_reintentos,
    explicar_error_archivo_abierto,
)
from etl.normalizacion import limpiar_texto
from etl.clasificacionProgramas import _get_sentence_transformer, MODELO_EMBEDDINGS


def _es_valor_faltante(valor: object) -> bool:
    """
    Determina si un valor se considera faltante para imputación.
    
    Args:
        valor: Valor a evaluar
        
    Returns:
        True si el valor se considera faltante, False en caso contrario
    """
    if pd.isna(valor):
        return True
    
    valor_str = str(valor).strip().lower()
    valores_faltantes = ["", "sin clasificar", "sin clasificacion", "n/a", "na", "none", "null"]
    
    return valor_str in valores_faltantes


def imputar_columna(
    df: pd.DataFrame,
    columna_target: str,
    columna_nombre: str,
    modelo_embeddings: "SentenceTransformer",
    n_neighbors: int = 5,
    batch_size: int = 32,
) -> tuple[pd.DataFrame, int]:
    """
    Imputa valores faltantes en una columna categórica usando KNN con embeddings semánticos.
    
    Esta función es genérica y puede usarse para cualquier columna categórica que se quiera
    imputar basándose en la similitud semántica de los nombres de programas.
    
    Args:
        df: DataFrame con los datos
        columna_target: Nombre de la columna a imputar (ej. "ÁREA_DE_CONOCIMIENTO")
        columna_nombre: Nombre de la columna que contiene los nombres de programas
                       (usualmente "NOMBRE_DEL_PROGRAMA")
        modelo_embeddings: Modelo SentenceTransformer ya cargado
        n_neighbors: Número de vecinos más cercanos a considerar (default: 5)
        batch_size: Tamaño de lote para generar embeddings (default: 32)
        
    Returns:
        Tupla (DataFrame con valores imputados, número de valores imputados)
        
    Raises:
        ValueError: Si la columna target o nombre no existen en el DataFrame
        ValueError: Si no hay suficientes registros con valores asignados para hacer KNN
    """
    # Validar que las columnas existan
    if columna_target not in df.columns:
        raise ValueError(f"La columna '{columna_target}' no existe en el DataFrame")
    
    if columna_nombre not in df.columns:
        raise ValueError(f"La columna '{columna_nombre}' no existe en el DataFrame")
    
    # Crear copia para no modificar el original
    df_resultado = df.copy()
    
    # Identificar filas con valores faltantes
    mask_faltantes = df_resultado[columna_target].apply(_es_valor_faltante)
    filas_faltantes = df_resultado[mask_faltantes]
    
    if len(filas_faltantes) == 0:
        log_info(f"No hay valores faltantes en la columna '{columna_target}'")
        return df_resultado, 0
    
    # Identificar filas con valores asignados (no faltantes)
    filas_con_valor = df_resultado[~mask_faltantes]
    
    if len(filas_con_valor) == 0:
        log_warning(
            f"No hay registros con valores asignados en '{columna_target}'. "
            "No se puede realizar imputación."
        )
        return df_resultado, 0
    
    if len(filas_con_valor) < n_neighbors:
        log_warning(
            f"Solo hay {len(filas_con_valor)} registros con valores asignados en '{columna_target}', "
            f"pero se requieren al menos {n_neighbors} para KNN. "
            f"Reduciendo n_neighbors a {len(filas_con_valor)}."
        )
        n_neighbors = len(filas_con_valor)
    
    log_info(
        f"Imputando '{columna_target}': {len(filas_faltantes)} valores faltantes, "
        f"{len(filas_con_valor)} valores de referencia disponibles"
    )
    
    # Preparar textos para embeddings: limpiar y normalizar
    textos_con_valor = filas_con_valor[columna_nombre].apply(
        lambda x: str(x) if pd.notna(x) else ""
    ).tolist()
    
    textos_faltantes = filas_faltantes[columna_nombre].apply(
        lambda x: str(x) if pd.notna(x) else ""
    ).tolist()
    
    # Generar embeddings para programas con valores asignados
    log_info(f"Generando embeddings para {len(textos_con_valor)} programas de referencia...")
    try:
        embeddings_con_valor = modelo_embeddings.encode(
            textos_con_valor,
            show_progress_bar=True,
            batch_size=batch_size,
            convert_to_numpy=True,
        )
    except Exception as e:
        error_msg = f"Error al generar embeddings para programas de referencia: {e}"
        log_error(error_msg)
        raise RuntimeError(error_msg) from e
    
    # Generar embeddings para programas con valores faltantes
    log_info(f"Generando embeddings para {len(textos_faltantes)} programas con valores faltantes...")
    try:
        embeddings_faltantes = modelo_embeddings.encode(
            textos_faltantes,
            show_progress_bar=True,
            batch_size=batch_size,
            convert_to_numpy=True,
        )
    except Exception as e:
        error_msg = f"Error al generar embeddings para programas faltantes: {e}"
        log_error(error_msg)
        raise RuntimeError(error_msg) from e
    
    # Preparar datos para KNN: usar los valores asignados como "etiquetas"
    valores_asignados = filas_con_valor[columna_target].astype(str).tolist()
    
    # Entrenar KNN con los programas que tienen valores asignados
    log_info(f"Entrenando KNN con {len(embeddings_con_valor)} muestras de entrenamiento...")
    try:
        knn = KNeighborsClassifier(
            n_neighbors=n_neighbors,
            weights='distance',  # Los vecinos más cercanos tienen más peso
            algorithm='auto',
            metric='cosine',  # Usar distancia coseno para embeddings semánticos
        )
        knn.fit(embeddings_con_valor, valores_asignados)
    except Exception as e:
        error_msg = f"Error al entrenar KNN: {e}"
        log_error(error_msg)
        raise RuntimeError(error_msg) from e
    
    # Predecir valores para los programas faltantes
    log_info(f"Prediciendo valores para {len(embeddings_faltantes)} programas faltantes...")
    try:
        valores_imputados = knn.predict(embeddings_faltantes)
        # Obtener probabilidades para logging
        probabilidades = knn.predict_proba(embeddings_faltantes)
        confianza_promedio = np.mean(np.max(probabilidades, axis=1))
        log_info(f"Confianza promedio de las predicciones: {confianza_promedio:.2%}")
    except Exception as e:
        error_msg = f"Error al predecir valores con KNN: {e}"
        log_error(error_msg)
        raise RuntimeError(error_msg) from e
    
    # Asignar valores imputados al DataFrame
    indices_faltantes = filas_faltantes.index
    df_resultado.loc[indices_faltantes, columna_target] = valores_imputados
    
    # Log de resumen
    valores_unicos_imputados = pd.Series(valores_imputados).value_counts()
    log_info(f"Valores imputados por categoría:")
    for valor, cantidad in valores_unicos_imputados.items():
        log_info(f"  - {valor}: {cantidad} programas")
    
    return df_resultado, len(filas_faltantes)


def ejecutar_imputacion_areas(
    df: pd.DataFrame | None = None,
    archivo: Path | None = None,
) -> pd.DataFrame:
    """
    Ejecuta la imputación de valores faltantes en la columna ÁREA_DE_CONOCIMIENTO.
    
    Esta función carga el modelo de embeddings una sola vez y lo reutiliza para
    todas las imputaciones, optimizando el uso de memoria.
    
    Args:
        df: DataFrame opcional. Si se proporciona, se procesa en memoria sin leer/escribir archivo.
        archivo: Archivo opcional. Si df es None, se lee desde este archivo (o ARCHIVO_PROGRAMAS por defecto).
        
    Returns:
        DataFrame con valores imputados en ÁREA_DE_CONOCIMIENTO
        
    Si df es None, lee desde archivo y escribe de vuelta.
    Si df se proporciona, solo imputa y retorna (sin I/O).
    """
    # Cargar datos
    if df is not None:
        df_procesar = df.copy()
        log_info(f"Imputando ÁREA_DE_CONOCIMIENTO en DataFrame en memoria ({len(df_procesar)} filas)")
    else:
        archivo = archivo or ARCHIVO_PROGRAMAS
        if not archivo.exists():
            error_msg = f"No se encontró el archivo: {archivo}"
            log_error(error_msg)
            raise FileNotFoundError(error_msg)
        
        try:
            df_procesar = leer_excel_con_reintentos(archivo, sheet_name=HOJA_PROGRAMAS)
            log_info(f"Archivo cargado para imputación: {archivo.name} ({len(df_procesar)} filas)")
        except PermissionError as e:
            error_msg = explicar_error_archivo_abierto(archivo, "leer")
            log_error(error_msg)
            raise PermissionError(error_msg) from e
    
    # Verificar que existe la columna ÁREA_DE_CONOCIMIENTO
    columna_target = "ÁREA_DE_CONOCIMIENTO"
    if columna_target not in df_procesar.columns:
        log_warning(
            f"La columna '{columna_target}' no existe en el DataFrame. "
            "No se puede realizar imputación."
        )
        return df_procesar
    
    # Verificar que existe la columna NOMBRE_DEL_PROGRAMA
    columna_nombre = "NOMBRE_DEL_PROGRAMA"
    if columna_nombre not in df_procesar.columns:
        error_msg = (
            f"La columna '{columna_nombre}' no existe en el DataFrame. "
            "Es necesaria para generar embeddings semánticos."
        )
        log_error(error_msg)
        raise ValueError(error_msg)
    
    # Contar valores faltantes antes de imputar
    mask_faltantes_antes = df_procesar[columna_target].apply(_es_valor_faltante)
    cantidad_faltantes = mask_faltantes_antes.sum()
    
    if cantidad_faltantes == 0:
        log_info(
            f"No hay valores faltantes en '{columna_target}'. "
            "No se requiere imputación."
        )
        return df_procesar
    
    log_info(f"Iniciando imputación de '{columna_target}': {cantidad_faltantes} valores faltantes detectados")
    
    # Cargar modelo de embeddings una sola vez (reutilizable)
    log_info(f"Cargando modelo de embeddings: {MODELO_EMBEDDINGS}")
    try:
        SentenceTransformer = _get_sentence_transformer()
        modelo_embeddings = SentenceTransformer(MODELO_EMBEDDINGS)
        log_info("Modelo de embeddings cargado exitosamente")
    except ImportError as e:
        error_msg = (
            f"No se pudo cargar el modelo de embeddings: {e}\n\n"
            "Asegúrate de que sentence-transformers esté instalado correctamente."
        )
        log_error(error_msg)
        raise ImportError(error_msg) from e
    except Exception as e:
        error_msg = f"Error al cargar modelo de embeddings: {e}"
        log_error(error_msg)
        raise RuntimeError(error_msg) from e
    
    # Ejecutar imputación usando la función genérica
    try:
        df_resultado, valores_imputados = imputar_columna(
            df=df_procesar,
            columna_target=columna_target,
            columna_nombre=columna_nombre,
            modelo_embeddings=modelo_embeddings,
            n_neighbors=5,
            batch_size=32,
        )
        
        log_info(f"Imputación completada: {valores_imputados} valores imputados en '{columna_target}'")
        
    except Exception as e:
        error_msg = f"Error durante la imputación de '{columna_target}': {e}"
        log_error(error_msg)
        raise RuntimeError(error_msg) from e
    
    # Si se está trabajando con archivo (no en memoria), guardar cambios
    if df is None:
        log_info(f"Guardando archivo con valores imputados: {archivo}")
        try:
            escribir_excel_con_reintentos(archivo, df_resultado, sheet_name=HOJA_PROGRAMAS)
            log_info(f"Archivo guardado exitosamente: {archivo.name}")
        except PermissionError as e:
            error_msg = explicar_error_archivo_abierto(archivo, "escribir")
            log_error(error_msg)
            raise PermissionError(error_msg) from e
        except Exception as e:
            error_msg = f"Error al guardar archivo con valores imputados: {e}"
            log_error(error_msg)
            raise RuntimeError(error_msg) from e
    
    return df_resultado


if __name__ == "__main__":
    # Ejecutar imputación desde línea de comandos
    ejecutar_imputacion_areas()
