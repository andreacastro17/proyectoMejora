"""
Modelo de clasificación supervisado para identificar programas nuevos que son referentes
de EAFIT o competencia directa.

El modelo se entrena con referentesUnificados.xlsx y clasifica programas nuevos del
archivo Programas.xlsx que tienen PROGRAMA_NUEVO = Sí.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

# Importación diferida de sentence_transformers para evitar errores al importar el módulo
_SentenceTransformer = None

def _get_sentence_transformer():
    """
    Obtiene SentenceTransformer con importación diferida.
    
    Returns:
        Clase SentenceTransformer
        
    Raises:
        ImportError: Si sentence_transformers no está disponible o está corrupto
    """
    global _SentenceTransformer
    if _SentenceTransformer is None:
        try:
            from sentence_transformers import SentenceTransformer
            _SentenceTransformer = SentenceTransformer
        except ImportError as e:
            error_msg = (
                f"No se pudo importar sentence_transformers. "
                f"Esto puede deberse a una instalación corrupta. "
                f"Error: {e}. "
                f"Intente reinstalar: pip install --no-cache-dir sentence-transformers"
            )
            log_error(error_msg)
            raise ImportError(error_msg) from e
    return _SentenceTransformer

from etl.pipeline_logger import log_error, log_info, log_resultado
from etl.config import (
    REF_DIR,
    MODELS_DIR,
    ARCHIVO_REFERENTES,
    ARCHIVO_CATALOGO_EAFIT,
    ARCHIVO_PROGRAMAS,
    HOJA_PROGRAMAS,
)

# ========= CONFIG =========
# Modelo de embeddings (multilingüe para español)
MODELO_EMBEDDINGS = "paraphrase-multilingual-MiniLM-L12-v2"

# Rutas de modelos guardados
MODELO_CLASIFICADOR = MODELS_DIR / "clasificador_referentes.pkl"
MODELO_EMBEDDINGS_OBJ = MODELS_DIR / "modelo_embeddings.pkl"
ENCODER_PROGRAMAS_EAFIT = MODELS_DIR / "encoder_programas_eafit.pkl"
# =========================


def normalizar_texto(texto: str) -> str:
    """
    Normaliza texto para comparación: minúsculas, sin tildes, sin caracteres especiales.
    
    Args:
        texto: Texto a normalizar
        
    Returns:
        Texto normalizado
    """
    if pd.isna(texto):
        return ""
    
    texto = str(texto).lower().strip()
    # Eliminar espacios múltiples
    texto = " ".join(texto.split())
    return texto


def normalizar_nivel_formacion(nivel: str) -> str:
    """
    Normaliza el nivel de formación a uno de los 4 valores canónicos:
    - 'universitario' (pregrado, universitario)
    - 'maestria' (maestría, magíster)
    - 'doctorado' (doctorado, phd)
    - 'especializacion universitaria' (especialización, especialidad)
    
    Args:
        nivel: Nivel de formación a normalizar
        
    Returns:
        Nivel normalizado a uno de los 4 valores canónicos
    """
    if pd.isna(nivel):
        return ""
    
    nivel_norm = normalizar_texto(str(nivel))
    
    # Universitario / Pregrado
    if any(x in nivel_norm for x in ['universit', 'pregrad', 'pregra']):
        return 'universitario'
    
    # Maestría
    if any(x in nivel_norm for x in ['maestr', 'magist', 'master']):
        return 'maestria'
    
    # Doctorado
    if any(x in nivel_norm for x in ['doctor', 'phd']):
        return 'doctorado'
    
    # Especialización universitaria
    if any(x in nivel_norm for x in ['especial', 'especializ']):
        return 'especializacion universitaria'
    
    # Si no coincide con ningún patrón, retornar vacío (se considerará inválido)
    return ""


def niveles_coinciden(nivel1: str, nivel2: str) -> bool:
    """
    Verifica si dos niveles de formación coinciden.
    
    Args:
        nivel1: Nivel 1 (normalizado)
        nivel2: Nivel 2 (normalizado)
        
    Returns:
        True si coinciden, False en caso contrario
    """
    nivel1_norm = normalizar_nivel_formacion(nivel1)
    nivel2_norm = normalizar_nivel_formacion(nivel2)
    
    if not nivel1_norm or not nivel2_norm:
        return False
    
    return nivel1_norm == nivel2_norm


def cargar_referentes(archivo: Path = ARCHIVO_REFERENTES) -> pd.DataFrame:
    """
    Carga el archivo de referentes unificados y prepara los datos de entrenamiento.
    
    Args:
        archivo: Ruta al archivo Excel de referentes
        
    Returns:
        DataFrame con los referentes preparados
    """
    print(f"Cargando referentes desde: {archivo}")
    df = pd.read_excel(archivo, engine='openpyxl')
    
    # Filtrar solo los que tienen label=1 (son referentes confirmados)
    df = df[df['label'] == 1].copy()
    
    # Limpiar datos - eliminar filas sin nombre de programa o programa EAFIT
    df = df.dropna(subset=['NOMBRE_DEL_PROGRAMA', 'NombrePrograma EAFIT'])
    
    # Normalizar textos
    df['NOMBRE_DEL_PROGRAMA_norm'] = df['NOMBRE_DEL_PROGRAMA'].apply(normalizar_texto)
    df['NombrePrograma EAFIT_norm'] = df['NombrePrograma EAFIT'].apply(normalizar_texto)
    
    # Normalizar campos amplios
    df['CAMPO_AMPLIO_norm'] = df['CAMPO_AMPLIO'].fillna('').apply(normalizar_texto)
    df['CAMPO_AMPLIO_EAFIT_norm'] = df['CAMPO_AMPLIO_EAFIT'].fillna('').apply(normalizar_texto)
    
    # Normalizar niveles de formación
    if 'NIVEL_DE_FORMACIÓN' in df.columns:
        df['NIVEL_DE_FORMACIÓN_norm'] = df['NIVEL_DE_FORMACIÓN'].fillna('').apply(normalizar_nivel_formacion)
    else:
        print("ADVERTENCIA: No se encontró la columna 'NIVEL_DE_FORMACIÓN' en referentes")
        df['NIVEL_DE_FORMACIÓN_norm'] = ''
    
    if 'NIVEL_DE_FORMACIÓN EAFIT' in df.columns:
        df['NIVEL_DE_FORMACIÓN_EAFIT_norm'] = df['NIVEL_DE_FORMACIÓN EAFIT'].fillna('').apply(normalizar_nivel_formacion)
    else:
        print("ADVERTENCIA: No se encontró la columna 'NIVEL_DE_FORMACIÓN EAFIT' en referentes")
        df['NIVEL_DE_FORMACIÓN_EAFIT_norm'] = ''
    
    # FILTRAR: Solo entrenar con referentes donde los niveles coinciden
    # CRÍTICO: Si los niveles no coinciden, no son referentes válidos
    antes_filtro = len(df)
    df = df[
        (df['NIVEL_DE_FORMACIÓN_norm'] != '') & 
        (df['NIVEL_DE_FORMACIÓN_EAFIT_norm'] != '') &
        (df['NIVEL_DE_FORMACIÓN_norm'] == df['NIVEL_DE_FORMACIÓN_EAFIT_norm'])
    ].copy()
    despues_filtro = len(df)
    
    if antes_filtro != despues_filtro:
        print(f"Filtrados {antes_filtro - despues_filtro} referentes donde los niveles no coinciden")
    
    print(f"Total de referentes con label=1 y niveles coincidentes: {len(df)}")
    return df


def cargar_catalogo_eafit(archivo: Path = ARCHIVO_CATALOGO_EAFIT) -> pd.DataFrame:
    """
    Carga el catálogo de programas EAFIT.
    
    Args:
        archivo: Ruta al archivo Excel del catálogo
        
    Returns:
        DataFrame con programas EAFIT
    """
    print(f"Cargando catálogo EAFIT desde: {archivo}")
    df = pd.read_excel(archivo, engine='openpyxl')
    
    # Normalizar nombres
    df['Nombre Programa EAFIT_norm'] = df['Nombre Programa EAFIT'].apply(normalizar_texto)
    df['CAMPO_AMPLIO_norm'] = df['CAMPO_AMPLIO'].fillna('').apply(normalizar_texto)
    
    # Normalizar niveles de formación
    # El catálogo EAFIT tiene la columna 'NIVEL_DE_FORMACIÓN'
    if 'NIVEL_DE_FORMACIÓN' in df.columns:
        df['NIVEL_DE_FORMACIÓN_norm'] = df['NIVEL_DE_FORMACIÓN'].fillna('').apply(normalizar_nivel_formacion)
    elif 'Nivel Programas' in df.columns:
        # Fallback: si no está NIVEL_DE_FORMACIÓN, usar Nivel Programas
        df['NIVEL_DE_FORMACIÓN_norm'] = df['Nivel Programas'].fillna('').apply(normalizar_nivel_formacion)
    else:
        print("ADVERTENCIA: No se encontró columna de nivel en catálogo EAFIT")
        df['NIVEL_DE_FORMACIÓN_norm'] = ''
    
    return df


def generar_features_embeddings(
    df: pd.DataFrame,
    modelo: Any,  # SentenceTransformer
    columna_nombre: str = 'NOMBRE_DEL_PROGRAMA_norm'
) -> np.ndarray:
    """
    Genera embeddings semánticos para los nombres de programas.
    
    Args:
        df: DataFrame con los programas
        modelo: Modelo de sentence-transformers
        columna_nombre: Nombre de la columna con los nombres de programas
        
    Returns:
        Array con embeddings (n_samples, embedding_dim)
    """
    print(f"Generando embeddings para {len(df)} programas...")
    textos = df[columna_nombre].astype(str).tolist()
    
    embeddings = modelo.encode(
        textos,
        show_progress_bar=True,
        batch_size=32,
        convert_to_numpy=True
    )
    
    return embeddings


def calcular_similitud_campo_amplio(
    campo1: str,
    campo2: str
) -> float:
    """
    Calcula similitud entre campos amplios (1 si son iguales, 0 si no).
    
    Args:
        campo1: Campo amplio 1
        campo2: Campo amplio 2
        
    Returns:
        Similitud (0.0 o 1.0)
    """
    if not campo1 or not campo2:
        return 0.0
    
    return 1.0 if campo1 == campo2 else 0.0


def calcular_similitud_nivel(
    nivel1: str,
    nivel2: str
) -> float:
    """
    Calcula similitud entre niveles de formación (1 si coinciden, 0 si no).
    CRÍTICO: Si los niveles no coinciden, no pueden ser referentes.
    
    Args:
        nivel1: Nivel 1
        nivel2: Nivel 2
        
    Returns:
        Similitud (0.0 o 1.0)
    """
    return 1.0 if niveles_coinciden(nivel1, nivel2) else 0.0


def preparar_features_entrenamiento(
    df_referentes: pd.DataFrame,
    modelo_embeddings: SentenceTransformer
) -> tuple[np.ndarray, np.ndarray, LabelEncoder]:
    """
    Prepara las features para entrenamiento del modelo.
    
    Features incluyen:
    - Embeddings del nombre del programa externo
    - Embeddings del nombre del programa EAFIT
    - Similitud coseno entre embeddings
    - Similitud de campo amplio (binaria)
    
    Args:
        df_referentes: DataFrame con referentes
        modelo_embeddings: Modelo de embeddings
        
    Returns:
        Tupla con (features, labels, encoder_programas_eafit)
    """
    print("Preparando features de entrenamiento...")
    
    # Generar embeddings para programas externos
    embeddings_externos = generar_features_embeddings(
        df_referentes,
        modelo_embeddings,
        'NOMBRE_DEL_PROGRAMA_norm'
    )
    
    # Generar embeddings para programas EAFIT
    embeddings_eafit = generar_features_embeddings(
        df_referentes,
        modelo_embeddings,
        'NombrePrograma EAFIT_norm'
    )
    
    # Calcular similitud coseno entre embeddings
    from sklearn.metrics.pairwise import cosine_similarity
    similitudes_embeddings = np.array([
        cosine_similarity([emb_ext], [emb_eaf])[0][0]
        for emb_ext, emb_eaf in zip(embeddings_externos, embeddings_eafit)
    ])
    
    # Calcular similitud de campo amplio
    similitudes_campo = np.array([
        calcular_similitud_campo_amplio(row['CAMPO_AMPLIO_norm'], row['CAMPO_AMPLIO_EAFIT_norm'])
        for _, row in df_referentes.iterrows()
    ])
    
    # Calcular similitud de nivel de formación
    similitudes_nivel = np.array([
        calcular_similitud_nivel(row['NIVEL_DE_FORMACIÓN_norm'], row['NIVEL_DE_FORMACIÓN_EAFIT_norm'])
        for _, row in df_referentes.iterrows()
    ])
    
    # Combinar features: embeddings externos + similitud coseno + similitud campo + similitud nivel
    # Usamos solo los embeddings del programa externo y las similitudes como features adicionales
    embedding_dim = embeddings_externos.shape[1]
    
    # Features: embeddings del programa externo + similitud coseno + similitud campo + similitud nivel
    features = np.hstack([
        embeddings_externos,  # embedding_dim features
        similitudes_embeddings.reshape(-1, 1),  # 1 feature
        similitudes_campo.reshape(-1, 1),  # 1 feature
        similitudes_nivel.reshape(-1, 1)  # 1 feature
    ])
    
    # Labels: código del programa EAFIT (usaremos el nombre normalizado como label)
    encoder = LabelEncoder()
    labels = encoder.fit_transform(df_referentes['NombrePrograma EAFIT_norm'])
    
    print(f"Features shape: {features.shape}")
    print(f"Labels shape: {labels.shape}")
    print(f"Total de programas EAFIT únicos: {len(encoder.classes_)}")
    
    return features, labels, encoder


def entrenar_modelo(
    features: np.ndarray,
    labels: np.ndarray,
    test_size: float = 0.2,
    random_state: int = 42
) -> tuple[RandomForestClassifier, dict]:
    """
    Entrena un modelo RandomForest para clasificar programas.
    
    Args:
        features: Array de features (n_samples, n_features)
        labels: Array de labels (n_samples,)
        test_size: Proporción de datos para test
        random_state: Semilla aleatoria
        
    Returns:
        Tupla con (modelo entrenado, métricas)
    """
    print("Dividiendo datos en entrenamiento y prueba...")

    # Si alguna clase tiene solo 1 muestra, stratify falla. En ese caso usamos split sin estratificar.
    counts_por_clase = np.bincount(labels)
    min_count = counts_por_clase.min()
    usar_stratify = min_count >= 2

    if not usar_stratify:
        print(
            f"[WARN] Al menos una clase tiene solo {min_count} muestra. "
            "Se realizará el split sin estratificar para evitar errores."
        )

    X_train, X_test, y_train, y_test = train_test_split(
        features,
        labels,
        test_size=test_size,
        random_state=random_state,
        stratify=labels if usar_stratify else None,
    )
    
    print(f"Entrenamiento: {len(X_train)} muestras")
    print(f"Prueba: {len(X_test)} muestras")
    
    print("Entrenando modelo RandomForest...")
    modelo = RandomForestClassifier(
        n_estimators=200,
        max_depth=30,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=random_state,
        n_jobs=-1,
        verbose=1
    )
    
    modelo.fit(X_train, y_train)
    
    # Evaluar modelo
    print("\nEvaluando modelo...")
    y_pred = modelo.predict(X_test)
    
    print("\nReporte de clasificación:")
    print(classification_report(y_test, y_pred))
    
    print("\nMatriz de confusión:")
    print(confusion_matrix(y_test, y_pred))
    
    # Calcular accuracy
    accuracy = modelo.score(X_test, y_test)
    print(f"\nAccuracy: {accuracy:.4f}")
    
    metricas = {
        'accuracy': accuracy,
        'n_train': len(X_train),
        'n_test': len(X_test)
    }
    
    return modelo, metricas


def guardar_modelos(
    modelo_clasificador: RandomForestClassifier,
    modelo_embeddings: SentenceTransformer,
    encoder: LabelEncoder
) -> None:
    """
    Guarda los modelos entrenados en disco.
    
    Args:
        modelo_clasificador: Modelo RandomForest entrenado
        modelo_embeddings: Modelo de embeddings
        encoder: Encoder de labels
    """
    print(f"Guardando modelos en {MODELS_DIR}...")
    
    with open(MODELO_CLASIFICADOR, 'wb') as f:
        pickle.dump(modelo_clasificador, f)
    
    with open(MODELO_EMBEDDINGS_OBJ, 'wb') as f:
        pickle.dump(modelo_embeddings, f)
    
    with open(ENCODER_PROGRAMAS_EAFIT, 'wb') as f:
        pickle.dump(encoder, f)
    
    print("Modelos guardados exitosamente.")


def cargar_modelos() -> tuple[RandomForestClassifier, Any, LabelEncoder]:
    """
    Carga los modelos entrenados desde disco.
    
    Returns:
        Tupla con (modelo_clasificador, modelo_embeddings, encoder)
    """
    print("Cargando modelos desde disco...")
    
    if not MODELO_CLASIFICADOR.exists():
        raise FileNotFoundError(
            f"No se encontró el modelo. Ejecute primero el entrenamiento. "
            f"Ruta esperada: {MODELO_CLASIFICADOR}"
        )
    
    with open(MODELO_CLASIFICADOR, 'rb') as f:
        modelo_clasificador = pickle.load(f)
    
    with open(MODELO_EMBEDDINGS_OBJ, 'rb') as f:
        modelo_embeddings = pickle.load(f)
    
    with open(ENCODER_PROGRAMAS_EAFIT, 'rb') as f:
        encoder = pickle.load(f)
    
    print("Modelos cargados exitosamente.")
    return modelo_clasificador, modelo_embeddings, encoder


def clasificar_programa_nuevo(
    nombre_programa: str,
    campo_amplio: str | None,
    nivel_formacion: str | None,
    modelo_clasificador: RandomForestClassifier,
    modelo_embeddings: Any,  # SentenceTransformer
    encoder: LabelEncoder,
    df_catalogo_eafit: pd.DataFrame,
    top_k_candidatos: int = 20
) -> dict[str, Any]:
    """
    Clasifica un programa nuevo y determina si es referente y a qué programa EAFIT pertenece.
    
    Estrategia:
    1. Normaliza el nivel de formación del programa nuevo
    2. FILTRA el catálogo EAFIT solo a programas con el mismo nivel (CRÍTICO)
    3. Genera embedding del programa nuevo
    4. Encuentra los top K candidatos por similitud de embedding (solo entre los del mismo nivel)
    5. Para cada candidato, calcula features completas y usa el modelo para predecir
    6. Selecciona el mejor match
    
    Args:
        nombre_programa: Nombre del programa nuevo
        campo_amplio: Campo amplio del programa (opcional)
        nivel_formacion: Nivel de formación del programa nuevo (CRÍTICO)
        modelo_clasificador: Modelo RandomForest entrenado
        modelo_embeddings: Modelo de embeddings
        encoder: Encoder de labels
        df_catalogo_eafit: DataFrame con catálogo EAFIT
        top_k_candidatos: Número de candidatos a evaluar con el modelo completo
        
    Returns:
        Diccionario con resultados de clasificación
    """
    from sklearn.metrics.pairwise import cosine_similarity
    
    # Normalizar nombre y nivel
    nombre_norm = normalizar_texto(nombre_programa)
    campo_norm = normalizar_texto(campo_amplio) if campo_amplio else ""
    nivel_norm = normalizar_nivel_formacion(nivel_formacion) if nivel_formacion else ""
    
    # CRÍTICO: Filtrar catálogo EAFIT por nivel de formación
    # Si los niveles no coinciden, NO pueden ser referentes
    if nivel_norm:
        df_candidatos = df_catalogo_eafit[
            df_catalogo_eafit['NIVEL_DE_FORMACIÓN_norm'] == nivel_norm
        ].copy()
        
        if len(df_candidatos) == 0:
            # No hay ningún programa EAFIT con el mismo nivel
            return {
                'es_referente': False,
                'probabilidad': 0.0,
                'programa_eafit_codigo': None,
                'programa_eafit_nombre': None,
                'similitud_embedding': 0.0,
                'similitud_campo': 0.0,
                'similitud_nivel': 0.0,
                'razon_no_referente': f'Nivel de formación "{nivel_norm}" no coincide con ningún programa EAFIT',
                'top_5_matches': []
            }
    else:
        # Si no se proporciona nivel, usar todos (pero esto no debería pasar)
        print("ADVERTENCIA: No se proporcionó nivel de formación. Evaluando todos los candidatos.")
        df_candidatos = df_catalogo_eafit.copy()
    
    # Generar embedding del programa nuevo
    embedding_programa = modelo_embeddings.encode(
        [nombre_norm],
        convert_to_numpy=True
    )
    
    # Pre-calcular embeddings de programas EAFIT candidatos (solo los del mismo nivel)
    nombres_eafit = df_candidatos['Nombre Programa EAFIT_norm'].tolist()
    embeddings_eafit = modelo_embeddings.encode(
        nombres_eafit,
        convert_to_numpy=True,
        show_progress_bar=False
    )
    
    # Calcular similitudes coseno con candidatos EAFIT
    similitudes = cosine_similarity(embedding_programa, embeddings_eafit)[0]
    
    # Obtener índices de los top K candidatos por similitud
    top_k_indices = np.argsort(similitudes)[::-1][:top_k_candidatos]
    
    mejores_matches = []
    
    # Evaluar cada candidato con el modelo completo
    for idx in top_k_indices:
        row_eafit = df_candidatos.iloc[idx]
        nombre_eafit_norm = row_eafit['Nombre Programa EAFIT_norm']
        campo_eafit_norm = row_eafit['CAMPO_AMPLIO_norm']
        nivel_eafit_norm = row_eafit['NIVEL_DE_FORMACIÓN_norm']
        embedding_eafit = embeddings_eafit[idx]
        similitud_emb = similitudes[idx]
        
        # Calcular similitud de campo amplio
        similitud_campo = calcular_similitud_campo_amplio(campo_norm, campo_eafit_norm)
        
        # Calcular similitud de nivel (debe ser 1.0 porque ya filtramos por nivel)
        similitud_nivel = calcular_similitud_nivel(nivel_norm, nivel_eafit_norm)
        
        # Preparar features para el modelo (igual que en entrenamiento)
        features = np.hstack([
            embedding_programa[0],  # embedding del programa nuevo
            np.array([similitud_emb]),  # similitud coseno
            np.array([similitud_campo]),  # similitud campo
            np.array([similitud_nivel])  # similitud nivel
        ]).reshape(1, -1)
        
        # Predecir con el modelo
        # El modelo predice qué programa EAFIT corresponde
        try:
            # Verificar si este programa EAFIT está en el encoder (fue visto en entrenamiento)
            if nombre_eafit_norm in encoder.classes_:
                label_eafit = encoder.transform([nombre_eafit_norm])[0]
                proba = modelo_clasificador.predict_proba(features)[0]
                probabilidad_modelo = proba[label_eafit] if label_eafit < len(proba) else 0.0
            else:
                # Si no está en el entrenamiento, usar solo similitud de embedding
                probabilidad_modelo = similitud_emb * 0.7  # Penalizar ligeramente
        except Exception:
            probabilidad_modelo = similitud_emb * 0.7
        
        # Combinar similitud de embedding y probabilidad del modelo
        # Peso: 60% modelo, 40% similitud directa
        score_final = 0.6 * probabilidad_modelo + 0.4 * similitud_emb
        
        mejores_matches.append({
            'Codigo EAFIT': row_eafit['Codigo EAFIT'],
            'NombrePrograma EAFIT': row_eafit['Nombre Programa EAFIT'],
            'Nivel EAFIT': nivel_eafit_norm,
            'probabilidad': score_final,
            'probabilidad_modelo': probabilidad_modelo,
            'similitud_embedding': similitud_emb,
            'similitud_campo': similitud_campo,
            'similitud_nivel': similitud_nivel
        })
    
    # Ordenar por score final descendente
    mejores_matches.sort(key=lambda x: x['probabilidad'], reverse=True)
    
    mejor_match = mejores_matches[0] if mejores_matches else None
    
    # Determinar si es referente (umbral ajustable)
    # VALIDACIÓN CRÍTICA: El nivel DEBE coincidir obligatoriamente
    # Si el nivel no coincide, automáticamente NO es referente, sin importar la probabilidad
    umbral_referente = 0.70  # Ajustable según calibración
    
    if mejor_match:
        nivel_coincide = mejor_match.get('similitud_nivel', 0.0) == 1.0
        probabilidad_suficiente = mejor_match['probabilidad'] >= umbral_referente
        
        # VALIDACIÓN ESTRICTA: Si el nivel NO coincide, automáticamente NO es referente
        if not nivel_coincide:
            es_referente = False
        else:
            # Solo si el nivel coincide, verificar la probabilidad
            es_referente = probabilidad_suficiente
    else:
        es_referente = False
    
    return {
        'es_referente': es_referente,
        'probabilidad': mejor_match['probabilidad'] if mejor_match else 0.0,
        'programa_eafit_codigo': mejor_match['Codigo EAFIT'] if mejor_match else None,
        'programa_eafit_nombre': mejor_match['NombrePrograma EAFIT'] if mejor_match else None,
        'similitud_embedding': mejor_match['similitud_embedding'] if mejor_match else 0.0,
        'similitud_campo': mejor_match['similitud_campo'] if mejor_match else 0.0,
        'similitud_nivel': mejor_match.get('similitud_nivel', 0.0) if mejor_match else 0.0,
        'top_5_matches': mejores_matches[:5]
    }


def clasificar_programas_nuevos(
    archivo_programas: Path = ARCHIVO_PROGRAMAS,
    hoja: str = HOJA_PROGRAMAS
) -> pd.DataFrame:
    """
    Clasifica todos los programas nuevos del archivo Programas.xlsx.
    
    Args:
        archivo_programas: Ruta al archivo Excel con programas
        hoja: Nombre de la hoja
        
    Returns:
        DataFrame con resultados de clasificación
    """
    print(f"Cargando programas desde: {archivo_programas}")
    df_programas = pd.read_excel(archivo_programas, sheet_name=hoja)
    log_info(f"Archivo de programas cargado: {archivo_programas.name}")
    
    # Filtrar solo programas nuevos
    if 'PROGRAMA_NUEVO' not in df_programas.columns:
        error_msg = (
            "No se encontró la columna 'PROGRAMA_NUEVO'. "
            "Ejecute primero procesamientoSNIES.py"
        )
        log_error(error_msg)
        raise ValueError(error_msg)
    
    df_nuevos = df_programas[df_programas['PROGRAMA_NUEVO'] == 'Sí'].copy()
    
    if len(df_nuevos) == 0:
        info_msg = "No hay programas nuevos para clasificar."
        print(info_msg)
        log_info(info_msg)
        return pd.DataFrame()
    
    print(f"Clasificando {len(df_nuevos)} programas nuevos...")
    log_info(f"Iniciando clasificación de {len(df_nuevos)} programas nuevos")
    
    # Cargar modelos
    modelo_clasificador, modelo_embeddings, encoder = cargar_modelos()
    
    # Cargar catálogo EAFIT
    df_catalogo_eafit = cargar_catalogo_eafit()
    
    # Clasificar cada programa nuevo
    resultados = []
    
    for idx, row in df_nuevos.iterrows():
        nombre_programa = row.get('NOMBRE_DEL_PROGRAMA', '')
        campo_amplio = row.get('CINE_F_2013_AC_CAMPO_AMPLIO', None)
        nivel_formacion = row.get('NIVEL_DE_FORMACIÓN', None)
        
        if pd.isna(nombre_programa) or not nombre_programa:
            continue
        
        resultado = clasificar_programa_nuevo(
            nombre_programa,
            campo_amplio,
            nivel_formacion,
            modelo_clasificador,
            modelo_embeddings,
            encoder,
            df_catalogo_eafit
        )
        
        # VALIDACIÓN CRÍTICA: Comparar directamente los campos NIVEL_DE_FORMACIÓN
        # Regla de negocio: Si el NIVEL_DE_FORMACIÓN del programa nuevo (Programas.xlsx) 
        # NO coincide exactamente con el NIVEL_DE_FORMACIÓN del programa EAFIT (catalogoOfertasEAFIT.xlsx),
        # automáticamente NO son referentes (ES_REFERENTE = "No")
        
        # Normalizar el nivel del programa nuevo desde Programas.xlsx
        nivel_programa_nuevo_norm = normalizar_nivel_formacion(nivel_formacion) if nivel_formacion else ""
        
        # Inicializar como NO referente si no hay nivel válido
        es_referente_final = False
        
        # Solo puede ser referente si:
        # 1. Hay un programa EAFIT asignado
        # 2. El nivel del programa nuevo es válido
        # 3. El nivel del programa EAFIT es válido
        # 4. Ambos niveles coinciden exactamente
        if resultado['programa_eafit_codigo'] is not None and nivel_programa_nuevo_norm:
            # Obtener el nivel del programa EAFIT desde catalogoOfertasEAFIT.xlsx
            programa_eafit_codigo = resultado['programa_eafit_codigo']
            programa_eafit_info = df_catalogo_eafit[
                df_catalogo_eafit['Codigo EAFIT'] == programa_eafit_codigo
            ]
            
            if not programa_eafit_info.empty:
                nivel_eafit_norm = programa_eafit_info.iloc[0].get('NIVEL_DE_FORMACIÓN_norm', '')
                
                # VALIDACIÓN DIRECTA: Comparar los niveles normalizados
                if nivel_eafit_norm:
                    if nivel_programa_nuevo_norm == nivel_eafit_norm:
                        # Los niveles coinciden → puede ser referente (depende de probabilidad)
                        es_referente_final = resultado['es_referente']
                    else:
                        # Los niveles NO coinciden → NO es referente
                        es_referente_final = False
                        print(
                            f"VALIDACIÓN NIVEL: Programa '{nombre_programa}' "
                            f"(NIVEL_DE_FORMACIÓN: '{nivel_formacion}' → '{nivel_programa_nuevo_norm}') "
                            f"NO es referente de '{resultado['programa_eafit_nombre']}' "
                            f"(NIVEL_DE_FORMACIÓN: '{nivel_eafit_norm}') - Niveles diferentes"
                        )
                else:
                    # El programa EAFIT no tiene nivel válido
                    es_referente_final = False
                    print(
                        f"VALIDACIÓN NIVEL: Programa EAFIT '{resultado['programa_eafit_nombre']}' "
                        f"no tiene NIVEL_DE_FORMACIÓN válido"
                    )
            else:
                # No se encontró el programa EAFIT en el catálogo
                es_referente_final = False
        elif not nivel_programa_nuevo_norm:
            # El programa nuevo no tiene nivel válido → NO puede ser referente
            es_referente_final = False
            if nivel_formacion:
                print(
                    f"VALIDACIÓN NIVEL: Programa '{nombre_programa}' "
                    f"tiene NIVEL_DE_FORMACIÓN inválido: '{nivel_formacion}'"
                )
            else:
                print(
                    f"VALIDACIÓN NIVEL: Programa '{nombre_programa}' "
                    f"no tiene NIVEL_DE_FORMACIÓN"
                )
        
        resultados.append({
            'CÓDIGO_SNIES_DEL_PROGRAMA': row.get('CÓDIGO_SNIES_DEL_PROGRAMA'),
            'NOMBRE_DEL_PROGRAMA': nombre_programa,
            'NIVEL_FORMACION': nivel_formacion,
            'ES_REFERENTE': 'Sí' if es_referente_final else 'No',
            'PROBABILIDAD': resultado['probabilidad'],
            'PROGRAMA_EAFIT_CODIGO': resultado['programa_eafit_codigo'],
            'PROGRAMA_EAFIT_NOMBRE': resultado['programa_eafit_nombre'],
            'SIMILITUD_EMBEDDING': resultado['similitud_embedding'],
            'SIMILITUD_CAMPO': resultado['similitud_campo'],
            'SIMILITUD_NIVEL': resultado.get('similitud_nivel', 0.0)
        })
        
        if (len(resultados) % 10) == 0:
            print(f"Procesados {len(resultados)}/{len(df_nuevos)} programas...")
    
    df_resultados = pd.DataFrame(resultados)
    
    # Agregar columnas al archivo original
    print("Agregando resultados al archivo original...")
    for col in ['ES_REFERENTE', 'PROBABILIDAD', 'PROGRAMA_EAFIT_CODIGO', 
                'PROGRAMA_EAFIT_NOMBRE', 'SIMILITUD_EMBEDDING', 'SIMILITUD_CAMPO', 'SIMILITUD_NIVEL']:
        df_programas[col] = None
    
    # Mapear resultados al DataFrame original
    for resultado in resultados:
        codigo_snies = resultado['CÓDIGO_SNIES_DEL_PROGRAMA']
        mask = df_programas['CÓDIGO_SNIES_DEL_PROGRAMA'] == codigo_snies
        
        if mask.any():
            df_programas.loc[mask, 'ES_REFERENTE'] = resultado['ES_REFERENTE']
            df_programas.loc[mask, 'PROBABILIDAD'] = resultado['PROBABILIDAD']
            df_programas.loc[mask, 'PROGRAMA_EAFIT_CODIGO'] = resultado['PROGRAMA_EAFIT_CODIGO']
            df_programas.loc[mask, 'PROGRAMA_EAFIT_NOMBRE'] = resultado['PROGRAMA_EAFIT_NOMBRE']
            df_programas.loc[mask, 'SIMILITUD_EMBEDDING'] = resultado['SIMILITUD_EMBEDDING']
            df_programas.loc[mask, 'SIMILITUD_CAMPO'] = resultado['SIMILITUD_CAMPO']
            df_programas.loc[mask, 'SIMILITUD_NIVEL'] = resultado.get('SIMILITUD_NIVEL', 0.0)
    
    # Guardar archivo actualizado
    print(f"Guardando resultados en {archivo_programas}...")
    with pd.ExcelWriter(
        archivo_programas,
        mode="a",
        if_sheet_exists="replace",
        engine="openpyxl",
    ) as writer:
        df_programas.to_excel(writer, sheet_name=hoja, index=False)
    
    total_nuevos = len(df_nuevos)
    referentes = df_resultados['ES_REFERENTE'].value_counts().get('Sí', 0)
    no_referentes = df_resultados['ES_REFERENTE'].value_counts().get('No', 0)
    
    print(f"\nClasificación completada:")
    print(f"Total de programas nuevos: {total_nuevos}")
    print(f"Programas clasificados como referentes: {referentes}")
    print(f"Programas no referentes: {no_referentes}")
    
    log_resultado(f"Total de programas nuevos clasificados: {total_nuevos}")
    log_resultado(f"Programas clasificados como referentes: {referentes}")
    log_resultado(f"Programas no referentes: {no_referentes}")
    
    return df_resultados


def entrenar_y_guardar_modelo() -> None:
    """
    Función principal para entrenar el modelo desde cero.
    """
    print("=== ENTRENAMIENTO DEL MODELO DE CLASIFICACIÓN ===\n")
    
    # Cargar datos de entrenamiento
    df_referentes = cargar_referentes()
    
    # Cargar modelo de embeddings
    print(f"\nCargando modelo de embeddings: {MODELO_EMBEDDINGS}")
    SentenceTransformer = _get_sentence_transformer()
    modelo_embeddings = SentenceTransformer(MODELO_EMBEDDINGS)
    
    # Preparar features
    features, labels, encoder = preparar_features_entrenamiento(
        df_referentes,
        modelo_embeddings
    )
    
    # Entrenar modelo
    modelo_clasificador, metricas = entrenar_modelo(features, labels)
    
    # Guardar modelos
    guardar_modelos(modelo_clasificador, modelo_embeddings, encoder)
    
    print("\n=== ENTRENAMIENTO COMPLETADO ===")
    print(f"Accuracy: {metricas['accuracy']:.4f}")
    print(f"Modelos guardados en: {MODELS_DIR}")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "entrenar":
        # Entrenar modelo
        entrenar_y_guardar_modelo()
    else:
        # Clasificar programas nuevos
        clasificar_programas_nuevos()

