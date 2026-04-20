"""
Prueba local del extractor SNIES (sin Airflow).

Ejecuta desde la raíz del repo:
    cd airflow/dags
    python -m tests.test_extractor_local 2024 matriculados

O dentro del contenedor Airflow:
    docker compose exec airflow-scheduler \\
        bash -c "cd /opt/airflow/dags && python -m tests.test_extractor_local 2024 matriculados"

Este script NO requiere que Postgres esté corriendo.
Solo descarga el Excel del SNIES y valida el parseo.
"""

import logging
import sys
from pathlib import Path

# Permitir imports desde dags/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.extractors import download_snies_file, parse_snies_excel, enrich_with_lineage
from utils.snies_catalog import get_source_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def main(anio: int, perfil: str) -> None:
    print(f"\n{'='*70}")
    print(f"Prueba local del extractor: {perfil} {anio}")
    print("="*70)

    # Paso 1: descargar
    print("\n[1/3] Descarga...")
    cfg = get_source_config(anio, perfil)
    print(f"    URL: {cfg['url']}")
    file_path = download_snies_file(anio, perfil)
    print(f"    ✓ Descargado en: {file_path}")

    # Paso 2: parsear
    print("\n[2/3] Parseo...")
    df = parse_snies_excel(
        file_path,
        perfil=perfil,
        sheet_name=cfg["sheet_name"],
        header_row=cfg["header_row"],
    )
    print(f"    ✓ DataFrame: {len(df):,} filas × {len(df.columns)} columnas")
    print(f"\n    Columnas detectadas:")
    for c in df.columns:
        print(f"      • {c}")

    # Paso 3: enriquecer con linaje
    print("\n[3/3] Enriquecimiento con linaje...")
    df = enrich_with_lineage(
        df,
        source_url=cfg["url"],
        source_file=cfg["filename"],
        dag_run_id="local_test",
    )
    print(f"    ✓ Columnas de linaje añadidas")

    # Resumen
    print("\n" + "="*70)
    print("RESUMEN")
    print("="*70)
    print(f"  Filas totales:       {len(df):,}")
    print(f"  Columnas totales:    {len(df.columns)}")
    print(f"  Columnas de datos:   {len([c for c in df.columns if not c.startswith('_')])}")
    print(f"  Columnas de linaje:  {len([c for c in df.columns if c.startswith('_')])}")

    # Filtro Bogotá (preview de lo que hará Silver)
    if "departamento_domicilio_ies" in df.columns:
        bogota_mask = df["departamento_domicilio_ies"].str.upper().str.contains(
            "BOGOT", na=False
        )
        print(f"  Filas en Bogotá:     {bogota_mask.sum():,}")

    print("\n  Primeras 3 filas (columnas clave):")
    key_cols = [c for c in [
        "codigo_institucion", "institucion_educacion_superior_ies",
        "departamento_domicilio_ies", "anio", "semestre",
        "matriculados", "docentes"
    ] if c in df.columns]
    print(df[key_cols].head(3).to_string(index=False))

    print("\n✅ Prueba local completada con éxito\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python -m tests.test_extractor_local <anio> <perfil>")
        print("Ejemplo: python -m tests.test_extractor_local 2024 matriculados")
        sys.exit(1)

    anio = int(sys.argv[1])
    perfil = sys.argv[2]
    main(anio, perfil)
