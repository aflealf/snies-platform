"""
Extractores: descargan y parsean los archivos .xlsx del SNIES.

Flujo:
    1. download_snies_file(anio, perfil) -> Path local del .xlsx
    2. parse_snies_excel(path, perfil)   -> pd.DataFrame con columnas canónicas
    3. enrich_with_lineage(df, ...)      -> añade columnas de trazabilidad

Diseño:
    - Reintentos exponenciales para descargas (red inestable del SNIES).
    - Cada archivo SNIES tiene una portada institucional; el header real
      viene en una fila específica del catálogo (no siempre la fila 0).
    - Normalización robusta de nombres de columna (tildes, saltos de línea,
      capitalización) vía Column Mapper en snies_catalog.py.
    - Validación explícita de columnas mínimas.
    - Hash por fila para deduplicación downstream.
    - Todo el DataFrame se devuelve con dtype=str (schema-on-read en Bronze).
"""

from __future__ import annotations

import hashlib
import logging
import time
from pathlib import Path
from typing import Literal

import pandas as pd
import requests

from utils.snies_catalog import (
    COLUMN_ALIASES,
    REQUIRED_COLUMNS,
    _normalize_for_matching,
    get_source_config,
    normalize_column_name,
)

logger = logging.getLogger(__name__)

Perfil = Literal["matriculados", "docentes"]

# Directorio donde se cachean las descargas dentro del contenedor Airflow.
DOWNLOAD_DIR = Path("/tmp/snies_downloads")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# 1. DESCARGA
# =============================================================================

def download_snies_file(
    anio: int,
    perfil: Perfil,
    force_redownload: bool = False,
    max_retries: int = 3,
    timeout_seconds: int = 180,
) -> Path:
    """
    Descarga el archivo .xlsx del SNIES para (año, perfil).
    """
    config = get_source_config(anio, perfil)
    url = config["url"]
    filename = config["filename"]
    local_path = DOWNLOAD_DIR / filename

    if local_path.exists() and not force_redownload:
        size_kb = local_path.stat().st_size / 1024
        logger.info(f"Usando archivo en cache: {local_path} ({size_kb:.1f} KB)")
        return local_path

    logger.info(f"Descargando {perfil} {anio} desde {url}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; SNIESDataPlatform/1.0; "
            "+https://snies.mineducacion.gov.co)"
        )
    }

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=timeout_seconds,
                stream=True,
            )
            response.raise_for_status()

            tmp_path = local_path.with_suffix(".xlsx.tmp")
            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            tmp_path.rename(local_path)

            size_kb = local_path.stat().st_size / 1024
            logger.info(f"✓ Descarga completa: {local_path.name} ({size_kb:.1f} KB)")
            return local_path

        except requests.RequestException as e:
            last_error = e
            wait = 2 ** attempt
            logger.warning(
                f"Intento {attempt}/{max_retries} falló: {e}. "
                f"Reintentando en {wait}s..."
            )
            time.sleep(wait)

    raise RuntimeError(
        f"No se pudo descargar {url} tras {max_retries} intentos. "
        f"Último error: {last_error}"
    )


# =============================================================================
# 2. PARSEO + NORMALIZACIÓN DE COLUMNAS
# =============================================================================

def _detect_header_row_fallback(
    file_path: Path,
    sheet_name,
    perfil: Perfil,
    max_rows_to_check: int = 20,
) -> int | None:
    """
    Si el header_row declarado en el catálogo no funciona, intenta auto-detectar
    la fila del encabezado buscando una fila que contenga varias columnas
    conocidas del mapping.

    Retorna el índice de la fila detectada o None si no encuentra nada.
    """
    df_peek = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=None,
        nrows=max_rows_to_check,
        dtype=str,
    )
    mapping = COLUMN_ALIASES[perfil]

    best_row = None
    best_score = 0

    for idx, row in df_peek.iterrows():
        # Cuántas celdas de esta fila coinciden con aliases conocidos
        score = sum(
            1 for v in row.values
            if pd.notna(v) and _normalize_for_matching(v) in mapping
        )
        if score > best_score:
            best_score = score
            best_row = idx

    # Requerir al menos 4 columnas reconocidas para considerar que es el header
    if best_score >= 4:
        logger.info(
            f"  [auto-detect] header detectado en fila {best_row} "
            f"({best_score} columnas reconocidas)"
        )
        return best_row

    return None


def parse_snies_excel(
    file_path: Path,
    perfil: Perfil,
    sheet_name: int | str = 0,
    header_row: int = 0,
) -> pd.DataFrame:
    """
    Lee un archivo .xlsx del SNIES y lo retorna con columnas canónicas.

    Si el header_row declarado no contiene las columnas esperadas, intenta
    auto-detectar el header escaneando las primeras filas.

    Raises:
        ValueError: si faltan columnas mínimas requeridas incluso tras
        auto-detect.
    """
    logger.info(
        f"Parseando {file_path.name} "
        f"(perfil={perfil}, sheet={sheet_name!r}, header_row={header_row})"
    )

    df = pd.read_excel(
        file_path,
        sheet_name=sheet_name,
        header=header_row,
        dtype=str,
        engine="openpyxl",
    )

    logger.info(f"  Filas leídas: {len(df):,} | Columnas: {len(df.columns)}")

    # --- Normalización de nombres de columna ---
    original_cols = list(df.columns)
    renamed_cols = [normalize_column_name(c, perfil) for c in original_cols]
    df.columns = renamed_cols

    mapped = sum(
        1 for orig in original_cols
        if _normalize_for_matching(orig) in COLUMN_ALIASES[perfil]
    )
    logger.info(f"  Columnas mapeadas: {mapped}/{len(original_cols)}")

    # --- Validación de columnas requeridas con fallback de auto-detect ---
    required = REQUIRED_COLUMNS[perfil]
    missing = [c for c in required if c not in df.columns]

    if missing:
        logger.warning(
            f"  Faltan columnas requeridas ({missing}). "
            f"Intentando auto-detectar header..."
        )
        detected = _detect_header_row_fallback(file_path, sheet_name, perfil)
        if detected is not None and detected != header_row:
            logger.info(f"  Reintentando con header_row={detected}")
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=detected,
                dtype=str,
                engine="openpyxl",
            )
            df.columns = [normalize_column_name(c, perfil) for c in df.columns]
            missing = [c for c in required if c not in df.columns]

    if missing:
        available = sorted(df.columns.tolist())
        raise ValueError(
            f"Faltan columnas requeridas para '{perfil}': {missing}\n"
            f"Columnas disponibles tras el mapping: {available}\n"
            f"→ Revisa utils/snies_catalog.py COLUMN_ALIASES o "
            f"ajusta sheet_name/header_row en SNIES_CATALOG."
        )

    # --- Limpieza de filas basura (notas al pie, filas de total, etc.) ---
    # Descartamos filas donde el codigo_institucion no sea numérico: esas son
    # típicamente filas de comentarios al final del archivo SNIES.
    before = len(df)
    df = df[df["codigo_institucion"].astype(str).str.match(r"^\d+$", na=False)]
    removed = before - len(df)
    if removed > 0:
        logger.info(f"  Descartadas {removed} filas sin codigo_institucion numérico")

    # --- Normalización de valores ---
    df = df.replace({"": None, "nan": None, "NaN": None, "None": None})

    # Trim whitespace en columnas de texto
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = df[col].str.strip()
            except AttributeError:
                pass

    logger.info(f"  ✓ Parseo completo: {len(df):,} filas válidas")
    return df


# =============================================================================
# 3. ENRIQUECIMIENTO CON METADATA DE LINAJE
# =============================================================================

def _hash_row(row: pd.Series) -> str:
    """Calcula MD5 de una fila para detectar duplicados exactos."""
    content = "||".join(str(v) for v in row.values)
    return hashlib.md5(content.encode("utf-8")).hexdigest()


def enrich_with_lineage(
    df: pd.DataFrame,
    source_url: str,
    source_file: str,
    dag_run_id: str,
) -> pd.DataFrame:
    """
    Añade columnas de linaje para trazabilidad en Bronze.
    """
    df = df.copy()

    df["_row_hash"] = df.apply(_hash_row, axis=1)
    df["_ingested_at"] = pd.Timestamp.utcnow()
    df["_source_url"] = source_url
    df["_source_file"] = source_file
    df["_dag_run_id"] = dag_run_id

    return df


# =============================================================================
# 4. ORQUESTACIÓN: descarga + parsea + enriquece en un solo call
# =============================================================================

def extract_snies_data(
    anio: int,
    perfil: Perfil,
    dag_run_id: str,
    force_redownload: bool = False,
) -> pd.DataFrame:
    """
    Pipeline completo de extracción: descarga → parsea → enriquece.
    """
    config = get_source_config(anio, perfil)

    # 1. Descarga
    file_path = download_snies_file(anio, perfil, force_redownload=force_redownload)

    # 2. Parseo + mapping
    df = parse_snies_excel(
        file_path,
        perfil=perfil,
        sheet_name=config["sheet_name"],
        header_row=config["header_row"],
    )

    # 3. Enriquecimiento con linaje
    df = enrich_with_lineage(
        df,
        source_url=config["url"],
        source_file=config["filename"],
        dag_run_id=dag_run_id,
    )

    logger.info(
        f"✓ Extracción completa: {len(df):,} filas listas para Bronze "
        f"({perfil} {anio})"
    )
    return df
