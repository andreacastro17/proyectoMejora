"""
Scraper de matrículas e inscritos históricos SNIES (Fase 2 pipeline mercado).

Reemplaza scraping web por consumo directo de API REST oficial del MEN.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import requests
from io import BytesIO
import unicodedata
import re
from urllib.parse import urljoin

from etl.config import RAW_HISTORIC_DIR
from etl.pipeline_logger import log_info, log_warning


def _empty_matriculados() -> pd.DataFrame:
    return pd.DataFrame(columns=["CÓDIGO_SNIES_DEL_PROGRAMA", "MATRICULADOS", "SEMESTRE"])


def _empty_inscritos() -> pd.DataFrame:
    return pd.DataFrame(columns=["CÓDIGO_SNIES_DEL_PROGRAMA", "INSCRITOS", "SEMESTRE"])


class SNIESMatriculasScraper:
    """
    Descarga matrículas e inscritos desde API REST de Bases Consolidadas del MEN.

    Endpoints:
      - Lista:     POST https://snies.mineducacion.gov.co/api/BasesConsolidadas/ListaArchivos con json={}
      - Descarga:  POST https://snies.mineducacion.gov.co/api/BasesConsolidadas/DescargarArchivo con json={"idDirectorio": "<id>"}

    Nota: Los archivos descargados son Excel anuales. Este scraper:
      - Descarga el Excel anual 1 sola vez por tipo (matriculados / inscritos) y año
      - Divide por SEMESTRE en memoria
      - Guarda inmediatamente matriculados_{year}_1.csv y matriculados_{year}_2.csv (o inscritos_*)
    """

    LISTA_URL = "https://snies.mineducacion.gov.co/api/BasesConsolidadas/ListaArchivos"
    DESCARGA_URL = "https://snies.mineducacion.gov.co/api/BasesConsolidadas/DescargarArchivo"
    PORTAL_LISTADO_URL = "https://snies.mineducacion.gov.co/portal/ESTADISTICAS/Bases-consolidadas/"
    TIMEOUT_SEC = 180

    def __init__(self, raw_dir: Path | None = None) -> None:
        self.raw_dir = raw_dir or RAW_HISTORIC_DIR
        self.raw_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _norm_col_name(name: str) -> str:
        """
        Normaliza nombres de columna para comparación:
        - upper
        - sin tildes/diacríticos
        - colapsa espacios
        """
        s = str(name).replace("\n", " ").replace("\r", " ").strip()
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = " ".join(s.split())
        return s.upper()

    @classmethod
    def _normalize_cached_columns(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Acepta variaciones de encabezados (ej. CODIGO vs CÓDIGO o caracteres dañados por encoding)
        y fuerza el nombre estándar: CÓDIGO_SNIES_DEL_PROGRAMA.
        """
        if df is None or len(df.columns) == 0:
            return df
        rename_map = {}
        for c in df.columns:
            cn = cls._norm_col_name(c)
            if "SNIES" in cn and "PROGRAMA" in cn:
                rename_map[c] = "CÓDIGO_SNIES_DEL_PROGRAMA"
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def _load_cached(self, path: Path, required_cols: set[str]) -> pd.DataFrame | None:
        if not path.exists():
            return None
        try:
            df = pd.read_csv(path, dtype={"CÓDIGO_SNIES_DEL_PROGRAMA": str}, encoding="utf-8-sig")
            df = self._normalize_cached_columns(df)
            # required_cols puede incluir CÓDIGO_SNIES_DEL_PROGRAMA; tolerar que venga como CODIGO... y ya haya sido normalizado.
            if required_cols.issubset(set(df.columns)):
                return df
        except Exception as e:
            log_warning(f"[Fase 2] Error leyendo {path.name}: {e}. Se intentará reconstruir desde API.")
        return None

    @staticmethod
    def _find_first_id(obj) -> str | None:
        """
        Busca recursivamente un identificador en JSON inestable.
        Prueba llaves comunes: idDirectorio, idArchivo, id, Id.
        """
        if isinstance(obj, dict):
            for k in ("idDirectorio", "idArchivo", "id", "Id"):
                v = obj.get(k)
                if v is not None and str(v).strip():
                    return str(v).strip()
            for v in obj.values():
                found = SNIESMatriculasScraper._find_first_id(v)
                if found:
                    return found
        elif isinstance(obj, list):
            for it in obj:
                found = SNIESMatriculasScraper._find_first_id(it)
                if found:
                    return found
        return None

    @staticmethod
    def _find_best_match_in_listing(listing_json, year: int, name_contains: list[str]) -> str | None:
        """
        Recorre el JSON de ListaArchivos buscando items cuyo nombre contenga alguno de name_contains y el año.
        Devuelve el id encontrado (idDirectorio/idArchivo/id/Id).
        """
        year_str = str(year)

        def _iter_items(o):
            if isinstance(o, dict):
                yield o
                for v in o.values():
                    yield from _iter_items(v)
            elif isinstance(o, list):
                for it in o:
                    yield from _iter_items(it)

        for item in _iter_items(listing_json):
            if not isinstance(item, dict):
                continue
            # Nombre puede venir con varias llaves
            name = None
            for nk in ("nombre", "Nombre", "name", "Name", "archivo", "Archivo", "nombreArchivo", "NombreArchivo"):
                if nk in item and item[nk] is not None:
                    name = str(item[nk])
                    break
            if not name:
                continue
            name_norm = name.strip().lower()
            if year_str not in name_norm:
                continue
            if not any(tok.lower() in name_norm for tok in name_contains):
                continue
            return SNIESMatriculasScraper._find_first_id(item)

        return None

    @staticmethod
    def _normalize_cols(df: pd.DataFrame) -> tuple[pd.DataFrame, str, str]:
        """
        Encuentra columnas de SNIES y SEMESTRE de forma flexible y renombra:
          - CÓDIGO_SNIES_DEL_PROGRAMA
          - SEMESTRE
        Retorna (df, col_snies, col_semestre)
        """
        cols = [str(c) for c in df.columns]
        cols_norm = {c: str(c).strip().replace("\n", " ").replace("\r", " ").lower() for c in cols}

        col_snies = None
        for c, cn in cols_norm.items():
            if "snies" in cn and "program" in cn:
                col_snies = c
                break
        if col_snies is None:
            for c, cn in cols_norm.items():
                if "snies" in cn:
                    col_snies = c
                    break
        col_sem = None
        for c, cn in cols_norm.items():
            if "semestre" in cn:
                col_sem = c
                break

        if not col_snies or not col_sem:
            raise ValueError(f"No se pudieron detectar columnas SNIES/SEMESTRE. Columnas: {list(df.columns)[:15]}")

        df = df.rename(columns={col_snies: "CÓDIGO_SNIES_DEL_PROGRAMA", col_sem: "SEMESTRE"})
        df["CÓDIGO_SNIES_DEL_PROGRAMA"] = df["CÓDIGO_SNIES_DEL_PROGRAMA"].astype(str).str.strip()
        df["SEMESTRE"] = pd.to_numeric(df["SEMESTRE"], errors="coerce").astype("Int64")
        return df, "CÓDIGO_SNIES_DEL_PROGRAMA", "SEMESTRE"

    def _descargar_excel_anual(self, year: int, name_contains: list[str]) -> pd.DataFrame:
        """
        Descarga el Excel anual desde:
          1) API REST (si está disponible)
          2) Fallback: portal público Bases-consolidadas (GET HTML + regex de enlaces)
        """
        content: BytesIO | None = None

        # 1) Intento por API REST (puede responder 404 en algunos entornos)
        try:
            listing = requests.post(self.LISTA_URL, json={}, timeout=self.TIMEOUT_SEC)
            listing.raise_for_status()
            listing_json = listing.json()

            file_id = self._find_best_match_in_listing(listing_json, year=year, name_contains=name_contains)
            if file_id:
                resp = requests.post(self.DESCARGA_URL, json={"idDirectorio": file_id}, timeout=self.TIMEOUT_SEC)
                resp.raise_for_status()
                content = BytesIO(resp.content)
        except Exception as e:
            # No abortar: si la API está caída/404, intentar portal HTML.
            log_warning(f"[Fase 2] API BasesConsolidadas no disponible para {year} ({name_contains}): {e}. Usando portal HTML.")
            content = None

        # 2) Fallback: portal HTML con enlaces a articles-*_recurso.xlsx
        if content is None:
            url_xlsx = self._find_xlsx_url_from_portal(year=year, name_contains=name_contains)
            if not url_xlsx:
                raise RuntimeError(f"No se encontró enlace XLSX en portal para año {year} con tokens {name_contains}.")
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                "Referer": self.PORTAL_LISTADO_URL,
            }
            r = requests.get(url_xlsx, headers=headers, timeout=self.TIMEOUT_SEC)
            r.raise_for_status()
            content = BytesIO(r.content)

        # Lectura Excel con skiprows=5 y fallback de hoja
        try:
            df = pd.read_excel(content, skiprows=5, sheet_name="1.")
        except Exception:
            content.seek(0)
            df = pd.read_excel(content, skiprows=5, sheet_name=0)
        if df is None or len(df) == 0:
            raise RuntimeError(f"Excel anual {year} descargado pero sin datos.")
        return df

    def _find_xlsx_url_from_portal(self, year: int, name_contains: list[str]) -> str | None:
        """
        Extrae del portal público el enlace al archivo Excel anual correspondiente.

        Restricciones:
        - Sin Selenium
        - Sin BeautifulSoup
        - Solo requests + regex sobre HTML

        El portal expone enlaces relativos tipo "articles-401908_recurso.xlsx".
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        }
        r = requests.get(self.PORTAL_LISTADO_URL, headers=headers, timeout=self.TIMEOUT_SEC)
        r.raise_for_status()
        html = r.text

        year_str = str(year)
        tokens = [t.lower().strip() for t in name_contains if str(t).strip()]

        # Capturar href + texto visible del link
        # Ejemplo: <a href="articles-401908_recurso.xlsx" ...>Estudiantes matriculados 2019</a>
        pat = re.compile(r'<a[^>]+href="(?P<href>[^"]+_recurso\.(?:xlsx|xlsb))"[^>]*>(?P<text>.*?)</a>', re.IGNORECASE | re.DOTALL)

        candidates: list[tuple[str, str]] = []
        for m in pat.finditer(html):
            href = (m.group("href") or "").strip()
            text = (m.group("text") or "").strip()
            # limpiar texto de tags internos si los hubiera
            text_clean = re.sub(r"<[^>]+>", " ", text)
            text_norm = " ".join(text_clean.split()).lower()
            if year_str not in text_norm:
                continue
            if tokens and not any(tok in text_norm for tok in tokens):
                continue
            candidates.append((href, text_norm))

        if not candidates:
            return None

        # Priorizar por tokens en texto (más coincidencias)
        def _score(t: str) -> int:
            return sum(1 for tok in tokens if tok in t) + (1 if year_str in t else 0)

        candidates.sort(key=lambda x: _score(x[1]), reverse=True)
        best_href = candidates[0][0]
        return urljoin(self.PORTAL_LISTADO_URL, best_href)

    def _split_y_cache_por_semestre(
        self,
        df: pd.DataFrame,
        year: int,
        prefix: str,
        value_col_candidates: list[str],
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Normaliza columnas, encuentra la columna de valor (MATRICULADOS/INSCRITOS) y guarda semestre 1 y 2.
        Retorna (df_sem1, df_sem2) con columnas [CÓDIGO_SNIES_DEL_PROGRAMA, VALUE, SEMESTRE]
        """
        df, _, _ = self._normalize_cols(df)

        # Encontrar columna de valor por contains (ej. 'MATRICULADOS', 'INSCRITOS', etc.)
        value_col = None
        cols_norm = {c: str(c).strip().replace("\n", " ").replace("\r", " ").lower() for c in df.columns}
        for cand in value_col_candidates:
            cand_l = cand.lower()
            for c, cn in cols_norm.items():
                if cand_l in cn:
                    value_col = c
                    break
            if value_col:
                break
        if not value_col:
            # Fallback: tomar primera columna numérica distinta a SEMESTRE
            numeric_cols = [c for c in df.columns if c not in ("CÓDIGO_SNIES_DEL_PROGRAMA", "SEMESTRE")]
            for c in numeric_cols:
                try:
                    s = pd.to_numeric(df[c], errors="coerce")
                    if s.notna().sum() > 0:
                        value_col = c
                        break
                except Exception:
                    continue
        if not value_col:
            raise ValueError("No se pudo detectar columna de valor (matriculados/inscritos).")

        out_value_col = "MATRICULADOS" if prefix == "matriculados" else "INSCRITOS"
        df[out_value_col] = pd.to_numeric(df[value_col], errors="coerce").fillna(0).astype(int)
        df_out = df[["CÓDIGO_SNIES_DEL_PROGRAMA", out_value_col, "SEMESTRE"]].copy()

        df_1 = df_out[df_out["SEMESTRE"] == 1].copy()
        df_2 = df_out[df_out["SEMESTRE"] == 2].copy()

        p1 = self.raw_dir / f"{prefix}_{year}_1.csv"
        p2 = self.raw_dir / f"{prefix}_{year}_2.csv"
        df_1.to_csv(p1, index=False, encoding="utf-8-sig")
        df_2.to_csv(p2, index=False, encoding="utf-8-sig")
        log_info(f"[Fase 2] {prefix} {year}: guardados {p1.name} ({len(df_1):,}) y {p2.name} ({len(df_2):,})")

        return df_1, df_2

    def download_matriculados(self, year: int, semestre: int) -> pd.DataFrame:
        """
        Obtiene matrículas para el año y semestre dados.
        Si el archivo ya existe en disco, lo carga sin descargar.
        Si la descarga falla, registra warning y retorna DataFrame vacío con columnas esperadas.
        """
        archivo = self.raw_dir / f"matriculados_{year}_{semestre}.csv"
        cached = self._load_cached(archivo, {"CÓDIGO_SNIES_DEL_PROGRAMA", "MATRICULADOS"})
        if cached is not None:
            log_info(f"[Fase 2] Matriculados {year}-{semestre}: cargado desde disco ({len(cached):,} filas)")
            return cached

        # Si el año ya fue descargado previamente para ambos semestres, usar el otro CSV como fuente
        other_path = self.raw_dir / f"matriculados_{year}_{1 if semestre == 2 else 2}.csv"
        other = self._load_cached(other_path, {"CÓDIGO_SNIES_DEL_PROGRAMA", "MATRICULADOS"})
        if other is not None and "SEMESTRE" in other.columns:
            # Si ya existe el otro semestre, es probable que ya se haya guardado el Excel anual.
            # En ese caso intentar cargar directamente el solicitado si aparece luego (evita descargar de nuevo).
            pass

        try:
            # Descargar Excel anual y dividir por semestre; cachear ambos CSVs.
            df_raw = self._descargar_excel_anual(year, name_contains=["matriculados"])
            df_1, df_2 = self._split_y_cache_por_semestre(
                df_raw,
                year=year,
                prefix="matriculados",
                value_col_candidates=["MATRICULADOS", "MATRICULA"],
            )
            return df_1 if int(semestre) == 1 else df_2
        except Exception as e:
            log_warning(f"[Fase 2] Matriculados {year}-{semestre}: {e}. Continuando con vacío.")
            return _empty_matriculados()

    def download_inscritos(self, year: int, semestre: int) -> pd.DataFrame:
        """
        Obtiene inscritos para el año y semestre dados.
        Si el archivo ya existe en disco, lo carga sin descargar.
        Si la descarga falla, registra warning y retorna DataFrame vacío con columnas esperadas.
        """
        archivo = self.raw_dir / f"inscritos_{year}_{semestre}.csv"
        cached = self._load_cached(archivo, {"CÓDIGO_SNIES_DEL_PROGRAMA", "INSCRITOS"})
        if cached is not None:
            log_info(f"[Fase 2] Inscritos {year}-{semestre}: cargado desde disco ({len(cached):,} filas)")
            return cached

        try:
            # En ListaArchivos la etiqueta suele ser "Primeros Inscritos" (con variaciones).
            df_raw = self._descargar_excel_anual(year, name_contains=["primeros inscritos", "inscritos"])
            df_1, df_2 = self._split_y_cache_por_semestre(
                df_raw,
                year=year,
                prefix="inscritos",
                value_col_candidates=["INSCRITOS", "PRIMEROS INSCRITOS", "PRIMEROS_INSCRITOS"],
            )
            return df_1 if int(semestre) == 1 else df_2
        except Exception as e:
            log_warning(f"[Fase 2] Inscritos {year}-{semestre}: {e}. Continuando con vacío.")
            return _empty_inscritos()
