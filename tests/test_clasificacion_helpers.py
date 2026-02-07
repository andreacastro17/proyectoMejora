from __future__ import annotations

import numpy as np
import pandas as pd


def test_normalizar_nivel_formacion():
    from etl.clasificacionProgramas import normalizar_nivel_formacion

    assert normalizar_nivel_formacion("Pregrado") == "universitario"
    assert normalizar_nivel_formacion("Universitaria") == "universitario"
    assert normalizar_nivel_formacion("Maestría") == "maestria"
    assert normalizar_nivel_formacion("Doctorado") == "doctorado"
    assert normalizar_nivel_formacion("Especialización") == "especializacion universitaria"


def test_niveles_coinciden():
    from etl.clasificacionProgramas import niveles_coinciden

    assert niveles_coinciden("Pregrado", "universitario") is True
    assert niveles_coinciden("Doctorado", "Maestría") is False


def test_clasificar_programa_nuevo_uses_umbral(monkeypatch):
    """
    Prueba controlada sin SentenceTransformer real:
    - 1 candidato EAFIT
    - embeddings constantes
    - modelo devuelve probabilidad alta para el label correcto
    """
    import etl.clasificacionProgramas as c

    class DummyEmb:
        def encode(self, texts, **kwargs):
            # vector 2D constante
            n = len(texts)
            return np.tile(np.array([[1.0, 0.0]]), (n, 1))

    class DummyModel:
        def predict_proba(self, X):
            # una sola clase (label 0) con prob 0.9
            return np.array([[0.9]])

    class DummyEncoder:
        classes_ = np.array(["ingenieria"])

        def transform(self, items):
            return np.array([0])

    df_catalogo = pd.DataFrame(
        [
            {
                "Codigo EAFIT": "E1",
                "Nombre Programa EAFIT": "Ingenieria",
                "Nombre Programa EAFIT_norm": "ingenieria",
                "CAMPO_AMPLIO_norm": "",
                "NIVEL_DE_FORMACIÓN_norm": "universitario",
            }
        ]
    )

    # Forzar umbral alto => False
    monkeypatch.setattr(c, "UMBRAL_REFERENTE", 0.95)
    r = c.clasificar_programa_nuevo(
        nombre_programa="Ingenieria de sistemas",
        campo_amplio=None,
        nivel_formacion="Pregrado",
        modelo_clasificador=DummyModel(),
        modelo_embeddings=DummyEmb(),
        encoder=DummyEncoder(),
        df_catalogo_eafit=df_catalogo,
        top_k_candidatos=1,
    )
    assert r["similitud_nivel"] == 1.0
    assert bool(r["es_referente"]) is False

    # Umbral bajo => True
    monkeypatch.setattr(c, "UMBRAL_REFERENTE", 0.1)
    r2 = c.clasificar_programa_nuevo(
        nombre_programa="Ingenieria de sistemas",
        campo_amplio=None,
        nivel_formacion="Pregrado",
        modelo_clasificador=DummyModel(),
        modelo_embeddings=DummyEmb(),
        encoder=DummyEncoder(),
        df_catalogo_eafit=df_catalogo,
        top_k_candidatos=1,
    )
    assert bool(r2["es_referente"]) is True

