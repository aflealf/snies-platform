"""
Genera el diagrama de arquitectura de la SNIES Data Platform.

Regenera con:
    python docs/generate_diagram.py
"""
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

fig, ax = plt.subplots(figsize=(20, 13))
ax.set_xlim(0, 22)
ax.set_ylim(0, 15)
ax.axis('off')

# ===== Paleta de colores =====
COLOR_EXT = "#4A90E2"
COLOR_ORQ = "#017CEE"
COLOR_BRONZE = "#CD7F32"
COLOR_SILVER = "#A8A8A8"
COLOR_GOLD = "#D4AF37"
COLOR_TRANSF = "#FF694B"
COLOR_CONS = "#509EE3"
COLOR_DOCKER = "#2496ED"
COLOR_DATASET = "#9D4EDD"


def box(ax, x, y, w, h, text, color, fontsize=10, fontcolor="white",
        rounded=True, weight="bold", alpha=1.0):
    style = "round,pad=0.1,rounding_size=0.2" if rounded else "square,pad=0.05"
    bbox = FancyBboxPatch((x, y), w, h, boxstyle=style,
                          linewidth=2, edgecolor="black",
                          facecolor=color, alpha=alpha)
    ax.add_patch(bbox)
    ax.text(x + w/2, y + h/2, text, ha='center', va='center',
            fontsize=fontsize, color=fontcolor, weight=weight, wrap=True)


def arrow(ax, x1, y1, x2, y2, color="black", style="-|>", lw=2, label=None, label_pos=0.5,
          linestyle="-"):
    arr = FancyArrowPatch((x1, y1), (x2, y2),
                          arrowstyle=style, mutation_scale=20,
                          linewidth=lw, color=color, linestyle=linestyle)
    ax.add_patch(arr)
    if label:
        lx = x1 + (x2 - x1) * label_pos
        ly = y1 + (y2 - y1) * label_pos
        ax.text(lx, ly + 0.15, label, ha='center', va='bottom',
                fontsize=8, style='italic', color=color,
                bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor="none"))


# ===== Título =====
ax.text(11, 14.4, "SNIES Data Platform — Arquitectura de Referencia",
        ha='center', fontsize=18, weight='bold')
ax.text(11, 13.9, "Medallion Architecture • Airflow 2.9 (Datasets) • dbt 1.8 • PostgreSQL 16 • Docker",
        ha='center', fontsize=11, style='italic', color='#555')

# ===== Contenedor Docker (fondo) =====
docker_bg = FancyBboxPatch((0.5, 0.5), 21, 12.8,
                            boxstyle="round,pad=0.1,rounding_size=0.3",
                            linewidth=2.5, edgecolor=COLOR_DOCKER,
                            facecolor="#E8F4FD", alpha=0.25, linestyle="--")
ax.add_patch(docker_bg)
ax.text(1.0, 13.0, "[ Docker Compose Network ]", fontsize=11,
        weight='bold', color=COLOR_DOCKER)

# ===== 1. Fuente externa (SNIES) =====
box(ax, 0.8, 10.0, 2.6, 1.8,
    "SNIES\nPortal Mineducación\n\n.xlsx por año\n(2022-2024)",
    COLOR_EXT, fontsize=9)

# ===== 2. Airflow =====
box(ax, 4.3, 10.0, 3.2, 1.8,
    "Apache Airflow 2.9\n\nScheduler + Webserver\nLocalExecutor\n:8080",
    COLOR_ORQ, fontsize=9)

# Airflow metadata DB
box(ax, 8.0, 10.2, 1.5, 1.4,
    "Postgres\nMetadata\n(Airflow)",
    "#336791", fontsize=8)

# DAGs — 3 cajas
box(ax, 4.0, 7.5, 1.8, 1.9,
    "DAG\nsnies_ingest\n\n• Parametrizado\n  por año\n• Extract\n• Load Bronze",
    "#2E8FD6", fontsize=7.5)

box(ax, 6.0, 7.5, 1.8, 1.9,
    "DAG\nsnies_backfill_all\n\n• Dynamic\n  Mapping\n• Todos los años\n• Paralelo",
    "#2E8FD6", fontsize=7.5)

box(ax, 8.0, 7.5, 1.8, 1.9,
    "DAG\nsnies_transform\n\n• dbt_seed\n• run Silver\n• run Gold\n• test (49)",
    "#2E8FD6", fontsize=7.5)

# ===== Dataset (nodo clave de encadenamiento) =====
box(ax, 5.0, 5.6, 3.8, 1.2,
    "Airflow Dataset\ndwh://bronze/updated\n(encadena DAGs)",
    COLOR_DATASET, fontsize=8.5)

# ===== 3. dbt =====
box(ax, 10.0, 7.5, 2.3, 1.9,
    "dbt-core 1.8\n(dbt-postgres)\n\n6 modelos SQL\n49 tests\nmacros\nseeds",
    COLOR_TRANSF, fontsize=9)

# ===== 4. Data Warehouse (Postgres) - 3 capas =====
dwh_bg = FancyBboxPatch((13.0, 3.3), 8.3, 8.0,
                        boxstyle="round,pad=0.1,rounding_size=0.2",
                        linewidth=2, edgecolor="#336791",
                        facecolor="#E8EDF2", alpha=0.5)
ax.add_patch(dwh_bg)
ax.text(17.15, 10.95, "PostgreSQL 16 — Data Warehouse",
        ha='center', fontsize=11, weight='bold', color="#336791")

# Bronze
box(ax, 13.3, 8.5, 7.7, 2.2,
    "BRONZE (raw)\n\n• matriculados_raw  (217,303 filas)\n• docentes_raw  (50,038 filas)\n• _ingestion_log\nLinaje: _ingested_at, _source_file, _row_hash, _dag_run_id",
    COLOR_BRONZE, fontsize=8.5)

# Silver
box(ax, 13.3, 6.0, 7.7, 2.2,
    "SILVER (cleansed)\n\n• stg_ies  (115 filas — dedupe + sector normalizado)\n• stg_matriculados_bogota / stg_docentes_bogota\n• ies_sue  (seed, 51 sedes SUE verificadas)",
    COLOR_SILVER, fontsize=8.5, fontcolor="black")

# Gold
box(ax, 13.3, 3.5, 7.7, 2.2,
    "GOLD (star schema)\n\n• dim_ies  (91 IES, con (*) es_sue)\n• dim_tiempo  (3 años)\n• fact_capacidad_academica (265 filas)\n  (*) ratio_estudiante_docente",
    COLOR_GOLD, fontsize=8.5, fontcolor="black")

# ===== 5. Capa de consumo =====
box(ax, 1.0, 3.4, 3.0, 1.8,
    "Metabase\n(BI Demo)\n\n:3000",
    COLOR_CONS, fontsize=9)

box(ax, 1.0, 1.2, 3.0, 1.8,
    "Tableau\n(conexión externa)\n\nPostgres :5432",
    "#1F77B4", fontsize=9)

# ===== Flechas =====

# SNIES -> DAGs de ingesta
arrow(ax, 3.45, 10.9, 4.25, 10.9, color="black", label="HTTPS .xlsx")
arrow(ax, 2.1, 10.0, 4.9, 9.4, color="black", lw=1.5, label_pos=0.55)
arrow(ax, 2.1, 10.0, 6.9, 9.4, color="black", lw=1.5)

# Airflow scheduler -> DAGs (línea de gestión)
arrow(ax, 4.9, 10.0, 4.9, 9.42, color="#017CEE", style="-", lw=1.5, linestyle="--")
arrow(ax, 5.9, 10.0, 6.9, 9.42, color="#017CEE", style="-", lw=1.5, linestyle="--")
arrow(ax, 6.9, 10.0, 8.9, 9.42, color="#017CEE", style="-", lw=1.5, linestyle="--")
ax.text(6.4, 9.65, "gestiona", fontsize=7, style='italic', color='#017CEE', ha='center')

# Airflow <-> Metadata DB
arrow(ax, 7.5, 11.0, 8.0, 11.0, color="#336791", style="<->", lw=1.5)

# DAGs ingest/backfill -> Bronze
arrow(ax, 5.8, 8.5, 13.25, 9.5, color="black", lw=1.5)
arrow(ax, 7.8, 8.5, 13.25, 9.3, color="black", lw=1.5, label="pandas +\nSQLAlchemy", label_pos=0.5)

# DAGs ingest/backfill -> Dataset (publican)
arrow(ax, 4.9, 7.5, 6.0, 6.8, color=COLOR_DATASET, style="-|>", lw=1.5, linestyle="--")
arrow(ax, 6.9, 7.5, 6.9, 6.8, color=COLOR_DATASET, style="-|>", lw=1.5, linestyle="--")
ax.text(5.5, 7.15, "publica", fontsize=7, style='italic', color=COLOR_DATASET, ha='center')

# Dataset -> DAG transform (dispara)
arrow(ax, 8.0, 6.2, 8.9, 7.5, color=COLOR_DATASET, style="-|>", lw=2, linestyle="--")
ax.text(8.65, 6.9, "dispara", fontsize=7, style='italic', color=COLOR_DATASET, ha='center', weight='bold')

# DAG transform -> dbt
arrow(ax, 9.8, 8.45, 10.0, 8.45, color="black", lw=2, label="ejecuta")

# dbt -> Silver
arrow(ax, 12.3, 7.8, 13.25, 7.1, color=COLOR_TRANSF, style="-|>", lw=2)
# dbt -> Gold
arrow(ax, 12.3, 7.6, 13.25, 4.5, color=COLOR_TRANSF, style="-|>", lw=2)
# dbt lee Bronze
arrow(ax, 13.2, 9.2, 12.35, 8.9, color=COLOR_TRANSF, style="<|-", lw=1.5)
ax.text(12.7, 9.05, "SELECT", fontsize=7, style='italic', color=COLOR_TRANSF)

# Gold -> Metabase
arrow(ax, 13.2, 4.3, 4.05, 4.25, color="black", lw=1.5, label="SQL / JDBC", label_pos=0.55)

# Gold -> Tableau
arrow(ax, 13.2, 3.8, 4.05, 2.1, color="black", lw=1.5, label="PostgreSQL Connector", label_pos=0.6)

# ===== Leyenda =====
legend_elements = [
    patches.Patch(facecolor=COLOR_EXT, label='Fuente externa'),
    patches.Patch(facecolor=COLOR_ORQ, label='Orquestación'),
    patches.Patch(facecolor=COLOR_DATASET, label='Airflow Dataset (encadenamiento)'),
    patches.Patch(facecolor=COLOR_TRANSF, label='Transformación (dbt)'),
    patches.Patch(facecolor=COLOR_BRONZE, label='Bronze (raw)'),
    patches.Patch(facecolor=COLOR_SILVER, label='Silver (cleansed)'),
    patches.Patch(facecolor=COLOR_GOLD, label='Gold (star schema)'),
    patches.Patch(facecolor=COLOR_CONS, label='Consumo / BI'),
]
ax.legend(handles=legend_elements, loc='lower right', bbox_to_anchor=(0.995, 0.02),
          fontsize=8, framealpha=0.95, ncol=1)

# Nota de flujo
ax.text(11, 0.75,
        "Flujo end-to-end: (1) Airflow descarga .xlsx → (2) carga a Bronze → (3) publica Dataset → "
        "(4) Transform se dispara auto → (5) dbt construye Silver + Gold → (6) BI consume",
        ha='center', fontsize=9, style='italic', color='#444',
        bbox=dict(boxstyle="round,pad=0.3", facecolor="#FFFACD", edgecolor="#999"))

plt.tight_layout()
plt.savefig('/home/claude/snies-platform/docs/architecture.png',
            dpi=150, bbox_inches='tight', facecolor='white')
print("✅ Diagrama generado: docs/architecture.png")
