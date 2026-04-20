# SNIES Data Platform — Capacidad Académica de Bogotá

> **Plataforma de datos end-to-end** que ingiere, transforma y analiza datos del **SNIES** (Sistema Nacional de Información de la Educación Superior) para calcular la **relación estudiante/docente** de las universidades de Bogotá para los años **2022, 2023 y 2024**.

[![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?logo=apacheairflow)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.8-FF694A?logo=dbt)](https://www.getdbt.com/)
[![Postgres](https://img.shields.io/badge/Postgres-16-336791?logo=postgresql)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://docs.docker.com/compose/)
[![Tests](https://img.shields.io/badge/dbt_tests-49%20passing-success)]()

---

## 📋 Tabla de contenido

1. [Resumen ejecutivo](#-resumen-ejecutivo)
2. [Arquitectura](#-arquitectura)
3. [Stack tecnológico](#-stack-tecnológico)
4. [Quick start](#-quick-start)
5. [Estructura del proyecto](#-estructura-del-proyecto)
6. [Pipeline de datos](#-pipeline-de-datos)
7. [Decisiones de diseño](#-decisiones-de-diseño)
8. [Calidad de datos](#-calidad-de-datos)
9. [Acceso desde BI](#-acceso-desde-bi-tableaumetabase)
10. [Escalabilidad](#-escalabilidad-y-siguientes-pasos)
11. [Agilidad y Vibe Coding](#-agilidad-y-vibe-coding)
12. [Mapeo con criterios del reto](#-mapeo-con-los-criterios-del-reto)

---

## 🎯 Resumen ejecutivo

El **Sistema Nacional de Información de Educación Superior (SNIES)** publica anualmente archivos Excel con los datos de matriculados y docentes de todas las IES del país. Esta plataforma los convierte en un **modelo dimensional consultable** que responde la pregunta clave:

> *¿Cuál es la relación estudiante/docente de cada universidad de Bogotá y cómo ha evolucionado en los últimos 3 años?*

**Resultados:**
- 📥 **267,341 filas crudas** ingeridas desde 6 archivos del SNIES (3 años × 2 perfiles)
- 📐 **91 universidades de Bogotá** modeladas en un star schema con atributo SUE
- 📊 **265 combinaciones (IES × año)** con métrica calculada
- ✅ **49 tests automatizados** de calidad de datos pasando
- 🚀 **Pipeline completamente automatizado** con Airflow + dbt encadenados

---

## 🏛️ Arquitectura

### Visión general

```
┌──────────────┐     ┌───────────────────────────────────────┐     ┌──────────────┐
│              │     │                                       │     │              │
│  SNIES.gov   │────▶│  Airflow (orquestación)   + dbt       │────▶│  Tableau /   │
│  (.xlsx)     │     │  Postgres 16 (DWH Medallion)          │     │  Metabase    │
│              │     │                                       │     │              │
└──────────────┘     └───────────────────────────────────────┘     └──────────────┘
                                      │
                              ┌───────┴────────┐
                              │   Docker       │
                              │   Compose      │
                              └────────────────┘
```

### Arquitectura Medallion (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  🥉 BRONZE (raw landing)            Schema: bronze                      │
│  ─────────────────────────────                                          │
│  • matriculados_raw   (217,303 filas)   ← datos crudos con linaje       │
│  • docentes_raw        (50,038 filas)                                   │
│  • _ingestion_log                                                       │
│                                                                         │
│  📦 Responsable: Airflow DAGs (snies_ingest, snies_backfill_all)        │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  🥈 SILVER (clean + normalized)      Schema: silver                     │
│  ───────────────────────────────                                        │
│  • stg_ies                  ← IES deduplicadas, sector_ies normalizado  │
│  • stg_matriculados_bogota  ← filtrado Bogotá + agregado por año        │
│  • stg_docentes_bogota      ← filtrado Bogotá + agregado por año        │
│  • ies_sue (seed)           ← lookup de 51 sedes del SUE                │
│                                                                         │
│  📦 Responsable: dbt models/silver/                                     │
│                                                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  🥇 GOLD (star schema)               Schema: gold                       │
│  ──────────────────────────                                             │
│  • dim_ies (91 filas)                 ← dimensión con atributo es_sue   │
│  • dim_tiempo (3 filas)               ← dimensión temporal              │
│  • fact_capacidad_academica (265)     ← ⭐ métrica principal            │
│                                                                         │
│  📦 Responsable: dbt models/gold/                                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Star schema de la capa Gold

```
                        ┌────────────────────┐
                        │   dim_tiempo       │
                        │  ────────────      │
                        │  🔑 anio           │
                        │     anio_texto     │
                        │     decada         │
                        └────────┬───────────┘
                                 │
                                 │ (1 : N)
                                 │
                                 ▼
   ┌──────────────────┐    ┌──────────────────────────┐
   │    dim_ies       │    │ fact_capacidad_academica │
   │  ───────────     │    │  ──────────────────────  │
   │  🔑 codigo_snies │◀───│  🔗 codigo_snies (FK)    │
   │     nombre_ies   │    │  🔗 anio        (FK)     │
   │     sector_ies   │    │     total_matriculados   │
   │     caracter_ies │    │     total_docentes       │
   │     es_sue  ⭐   │    │  ⭐ ratio_estudiante_    │
   │     ...          │    │        docente           │
   └──────────────────┘    │     flag_calidad         │
                           └──────────────────────────┘
```

---

## 💻 Stack tecnológico

| Componente | Tecnología | Versión | Rol |
|---|---|---|---|
| **Ingesta + Orquestación** | Apache Airflow | 2.9.3 | DAGs de ingesta y transformación |
| **Transformación** | dbt (postgres) | 1.8.2 | Modelos SQL de Silver y Gold |
| **Data Warehouse** | PostgreSQL | 16 | Storage + compute (local) |
| **Contenerización** | Docker Compose | v2 | Un comando para levantar todo |
| **Testing** | dbt tests + dbt_utils | 1.3.3 | 49 tests de calidad |
| **BI (opcional)** | Metabase | latest | Dashboards incluidos en stack |
| **Lenguaje** | Python, SQL | 3.11, Postgres dialect | |

---

## 🚀 Quick start

### Prerequisitos

- Docker Desktop / Docker Engine (24+)
- 4 GB de RAM disponibles
- Puertos 5432, 8080 y 3000 libres

### Instalación (3 comandos)

```bash
# 1. Clonar y entrar al repo
git clone <url-del-repo> snies-platform && cd snies-platform

# 2. Configurar variables de entorno (solo la primera vez)
cp .env.example .env

# 3. Levantar toda la plataforma
make up
# (o sin make: docker compose up -d)
```

La primera vez tarda ~3 minutos (descarga imágenes, inicializa Airflow). Al terminar, tendrás:

| Servicio | URL | Credenciales |
|---|---|---|
| 🛩️ Airflow UI | http://localhost:8080 | `airflow` / `airflow` |
| 🐘 Postgres DWH | `localhost:5432` | ver `.env` |
| 📊 Metabase (opcional) | http://localhost:3000 | configurar al primer acceso |

### Ejecutar el pipeline completo

En la UI de Airflow (http://localhost:8080):

1. Activa los 3 DAGs con el toggle izquierdo: `snies_ingest`, `snies_backfill_all`, `snies_transform`
2. Dispara **`snies_backfill_all`** (botón ▶️) → cargará los 3 años completos (~10 min)
3. `snies_transform` se disparará **automáticamente** al terminar (vía Airflow Datasets)
4. Al finalizar, la tabla `gold.fact_capacidad_academica` estará lista

### Consultar los resultados

```bash
make shell-dwh
```

```sql
SELECT
    d.nombre_ies,
    f.anio,
    f.total_matriculados,
    f.total_docentes,
    f.ratio_estudiante_docente
FROM gold.fact_capacidad_academica f
JOIN gold.dim_ies d USING (codigo_snies)
WHERE d.es_sue = TRUE
ORDER BY d.nombre_ies, f.anio;
```

Para más queries, ver [`docs/queries_bi.sql`](docs/queries_bi.sql).

---

## 📁 Estructura del proyecto

```
snies-platform/
│
├── 📄 README.md                     ← este archivo
├── 🐳 docker-compose.yaml           ← 6 servicios: DWH, Airflow, Metabase
├── ⚙️  .env.example                 ← plantilla de variables
├── 🔧 Makefile                      ← atajos: up, down, shell-dwh, logs
│
├── 📁 airflow/
│   ├── Dockerfile                   ← Airflow 2.9 + dbt-postgres
│   ├── requirements.txt
│   └── dags/
│       ├── snies_ingest_dag.py          ← ingesta parametrizada por año
│       ├── snies_backfill_all_dag.py    ← carga histórica completa
│       ├── snies_transform_dag.py       ← orquesta dbt (Silver + Gold)
│       └── utils/
│           ├── snies_catalog.py         ← URLs + schemas del SNIES
│           ├── extractors.py            ← parser con column mapper robusto
│           └── loaders.py               ← carga idempotente a Bronze
│
├── 📁 dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/                      ← helpers reutilizables
│   ├── models/
│   │   ├── bronze/sources.yml       ← declara tablas Bronze
│   │   ├── silver/                  ← staging normalizado
│   │   └── gold/                    ← star schema
│   ├── seeds/
│   │   └── ies_sue.csv              ← 51 sedes SUE verificadas
│   └── tests/                       ← tests custom
│
├── 📁 postgres/
│   └── init.sql                     ← crea schemas bronze/silver/gold
│
└── 📁 docs/
    ├── architecture.md              ← decisiones arquitectónicas
    ├── dbt_lineage.md               ← grafo de modelos dbt
    └── queries_bi.sql               ← queries listas para Tableau
```

---

## 🔄 Pipeline de datos

### DAG 1: `snies_ingest` (parametrizado por año)

```
start → validate_year → [ingest_matriculados ⋮⋮ ingest_docentes] → validate_load → end
                         (paralelo: extract → load)
```

**Trigger:** manual con `{"anio": 2024}` o via API.
**Runtime:** ~3-5 min por año.

### DAG 2: `snies_backfill_all` (histórico)

Carga todos los años usando **Dynamic Task Mapping** de Airflow.

```
start → generate_workload → ingest_one[×6] (máx 2 paralelos) → summary → end
```

**Trigger:** manual, sin parámetros.
**Runtime:** ~8-10 min (6 archivos).

### DAG 3: `snies_transform` (dbt granular)

```
start → dbt_deps → dbt_seed → dbt_run_silver → dbt_run_gold → dbt_test → end
```

**Trigger:** **automático** vía `Dataset("dwh://bronze/updated")` cuando cualquier DAG de ingesta termina.
**Runtime:** ~2-3 min.

### Encadenamiento entre DAGs (Airflow Datasets)

```
snies_ingest            ──┐
                          ├──▶  [Dataset: dwh://bronze/updated]  ──▶  snies_transform
snies_backfill_all      ──┘                                             │
                                                                        ▼
                                                          [Dataset: dwh://gold/ready]
                                                              (consumible por BI)
```

Es el patrón moderno de Airflow 2.4+: los DAGs se conectan por **datos producidos/consumidos**, no por triggers explícitos. Desacopla los DAGs completamente.

---

## 🧠 Decisiones de diseño

### 1. Arquitectura Medallion sobre un solo Postgres

**Decisión:** Un Postgres con 3 schemas (`bronze`, `silver`, `gold`) en lugar de warehouses cloud (Snowflake/BigQuery).

**Razonamiento:**
- ✅ **Simplicidad:** un solo servicio, un solo backup, un solo monitoreo
- ✅ **Costo:** Postgres maneja cómodamente TB de datos
- ✅ **Escalable:** migración a RDS / Cloud SQL / Aurora es trivial (cambiar host en `profiles.yml`)
- ✅ **Estándar:** Medallion es el patrón moderno (Databricks, Snowflake lo usan)

### 2. `codigo_snies` como grano de `dim_ies` (sede, no universidad legal)

**Decisión:** Una fila por código SNIES (sede), no por universidad legal.

**Razonamiento:**
- El SNIES usa `codigo_snies` como identificador de sede. Universidad Nacional tiene 7 códigos (Bogotá, Medellín, Manizales, Palmira, Amazonas, San Andrés, Cesar).
- Para este reto (solo Bogotá), cada universidad pública tiene exactamente 1 sede en Bogotá, por lo que el grano "sede" y "universidad legal" son equivalentes.
- El seed `ies_sue.csv` incluye una columna `universidad_sue` que permitirá agregar a nivel de universidad legal si se expande a nivel nacional.

### 3. Suma de semestres para la agregación anual

**Decisión:** `total_matriculados = SUM(matriculados_2024_1) + SUM(matriculados_2024_2)`.

**Razonamiento:**
- Es la convención oficial del MEN en informes consolidados.
- Para el ratio estudiante/docente, la doble contabilización se cancela (ambos numerador y denominador suman semestres).
- Preserva la señal de "volumen total de servicio" prestado en el año.

### 4. Universo de IES: Universidad + Institución Universitaria/Escuela Tecnológica

**Decisión:** Incluir tanto `Universidad` (31 IES) como `Institución Universitaria/Escuela Tecnológica` (60 IES). Total: 91 IES.

**Razonamiento:**
- El término coloquial "universidad" incluye ambas categorías del SNIES.
- Excluir las Instituciones Universitarias dejaría fuera instituciones relevantes: Politécnico Grancolombiano (105K matriculados), Uniminuto (~55K), Konrad Lorenz, Los Libertadores, etc.
- Se excluyen explícitamente `Institución Tecnológica` e `Institución Técnica Profesional` (carreras cortas, naturaleza distinta).

### 5. Atributo SUE como columna booleana

**Decisión:** `dim_ies.es_sue BOOLEAN` en lugar de dos tablas separadas (SUE/no-SUE).

**Razonamiento:**
- **Un solo modelo de verdad.** Los dashboards filtran dinámicamente en BI.
- Permite comparaciones directas (SUE vs privadas) sin joins adicionales.
- Si mañana se añaden universidades al SUE, solo se actualiza el seed `ies_sue.csv`.

### 6. Normalización de `sector_ies` en Silver

**Hallazgo:** El SNIES es inconsistente — usa `'Privado'` en algunas filas y `'Privada'` en otras para el mismo sector.

**Decisión:** Normalizar en `stg_ies` con `CASE WHEN UPPER(sector_ies) IN ('PRIVADO','PRIVADA') THEN 'Privada'`.

**Razonamiento:**
- La consolidación de valores de enumeración es una función clásica de Silver.
- Dashboards consistentes: usuarios no ven categorías duplicadas.
- Resistente a cambios futuros del SNIES.

### 7. Airflow Datasets para encadenamiento Ingest → Transform

**Decisión:** Usar `Dataset("dwh://bronze/updated")` en lugar de `TriggerDagRunOperator` o `ExternalTaskSensor`.

**Razonamiento:**
- Feature nativa de Airflow 2.4+, diseñada exactamente para este caso.
- Desacopla los DAGs: cualquier nuevo productor de Bronze (un DAG nuevo mañana) automáticamente disparará el transform.
- Visualización del linaje en la UI de Airflow → Datasets.

### 8. Column Mapper robusto para variaciones del SNIES entre años

**Problema detectado:** El SNIES cambió el formato de los archivos entre 2022 y 2023:
- 2022: header en fila 7, columnas con saltos de línea ("Código de \n la Institución")
- 2023-2024: header en fila 5, columnas en MAYÚSCULAS
- Campo de docentes: "No. de Docentes" (2022) vs "DOCENTES" (2023+)

**Solución:** Column mapper que normaliza los headers (UPPER + quita tildes + quita saltos de línea + quita paréntesis) antes de mapearlos al schema canónico.

**Resultado:** 72 columnas reales mapeadas, 0 fallos en los 3 años.

---

## 🛡️ Calidad de datos

El proyecto incluye **49 tests automatizados** que validan:

### Tests por capa

| Capa | Cantidad | Tipos |
|---|---|---|
| **Bronze (sources)** | 4 | `not_null` en columnas críticas |
| **Silver (staging)** | 18 | `not_null`, `unique_combination`, `accepted_values`, `relationships` |
| **Gold (star schema)** | 26 | `not_null`, `unique`, `relationships`, `accepted_values`, integridad referencial |
| **Custom** | 1 | `fact_solo_bogota`: invariante del reto (no se cuelan IES fuera de Bogotá) |

### Ejemplos de validaciones

```yaml
- dim_ies.codigo_snies: unique, not_null          # PK
- fact.codigo_snies → dim_ies.codigo_snies        # integridad referencial
- fact.anio → dim_tiempo.anio                     # integridad referencial
- sector_ies IN ('Oficial', 'Privada')            # normalización enforced
- ratio_estudiante_docente >= 0                   # invariante matemática
- (codigo_snies, anio) único en fact              # grano del hecho
```

### Ejecución

```bash
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && dbt test"
# Done. PASS=49 WARN=0 ERROR=0 SKIP=0
```

Estos tests **corren automáticamente** al final de cada ejecución del DAG `snies_transform`.

---

## 📊 Acceso desde BI (Tableau/Metabase)

### Conexión al DWH

El puerto 5432 de Postgres está expuesto al host:

```
Host:     localhost
Port:     5432
Database: snies_dwh
User:     snies         (ver .env)
Password: ******        (ver .env)
Schema:   gold          (fuente de datos para BI)
```

### Tablas para consumir

| Tabla | Rol en el modelo | Filas |
|---|---|---|
| `gold.fact_capacidad_academica` | Hechos | 265 |
| `gold.dim_ies` | Dimensión IES (con `es_sue`) | 91 |
| `gold.dim_tiempo` | Dimensión temporal | 3 |

### Ejemplo de conexión en Tableau

1. Conectar → PostgreSQL
2. Servidor: `localhost`, Puerto: `5432`, Base: `snies_dwh`
3. Schema: `gold`
4. Arrastrar `fact_capacidad_academica` al lienzo
5. Relacionar con `dim_ies` por `codigo_snies` y con `dim_tiempo` por `anio`

**Ver [`docs/queries_bi.sql`](docs/queries_bi.sql) para queries listas para copiar-pegar.**

---

## 🚀 Escalabilidad y siguientes pasos

### Escalabilidad horizontal

| Recurso actual | Estrategia de escalamiento |
|---|---|
| **Postgres local** | Migrar a RDS / Cloud SQL / Aurora. Cambio: 1 línea en `profiles.yml`. |
| **Airflow LocalExecutor** | Cambiar a `CeleryExecutor` con múltiples workers. Archivo: `docker-compose.yaml`. |
| **Storage de archivos** | Actualmente `/tmp/snies_downloads`. Mover a S3/GCS con hook nativo de Airflow. |
| **Computación dbt** | dbt se paraleliza a 4 threads. Con Postgres → Redshift/BigQuery escala automáticamente. |

### Extensiones del dominio

Ya contempladas en el diseño:

1. **Agregar más años:** añadir entrada a `SNIES_CATALOG` en `snies_catalog.py`. Cero cambios en dbt.
2. **Agregar otros departamentos:** `dim_ies` filtra por Bogotá en un solo lugar. Cambiar patrón en `dbt_project.yml:vars.bogota_pattern`.
3. **Agregar métricas nuevas:** añadir columnas a `fact_capacidad_academica` (p.ej. tasa de graduación, años promedio de permanencia).
4. **Nuevos DAGs:** se integran al encadenamiento vía `Dataset`, sin modificar los existentes.
5. **Granularidad semestral:** en `dim_tiempo` añadir `semestre`; en los staging cambiar `GROUP BY codigo_snies, anio, semestre`.

### CI/CD recomendado

```
push → GitHub Actions:
  ├── dbt parse         (valida SQL sin correrlo)
  ├── pytest unit tests (de los módulos utils/)
  ├── dbt test --select source:*   (valida Bronze)
  └── docker build + push
```

### Observabilidad

- **Airflow:** UI nativa + logs por task (ya incluidos)
- **dbt:** genera artifacts JSON que un script puede subir a Datadog/Grafana
- **Freshness checks:** ya declarados en `sources.yml` — alerta si Bronze no se actualiza en 400 días

---

## 🤖 Agilidad y Vibe Coding

### Uso de IA para acelerar desarrollo

Este proyecto fue desarrollado usando **Claude (Anthropic)** como copiloto técnico bajo el paradigma de **Vibe Coding**:

**Tareas delegadas a IA:**
- Generación de scaffolding (estructura inicial, Dockerfile, dbt_project.yml)
- Implementación del **Column Mapper** para manejar las 72 variantes de nombres de columna del SNIES (2022 vs 2023-2024 tienen headers muy distintos)
- Debugging de problemas sutiles (schemas `public_silver` vs `silver`, variables de entorno en BashOperator)
- Escritura de tests dbt declarativos en `schema.yml`
- Este mismo README

**Tareas realizadas por el desarrollador:**
- Decisiones arquitectónicas (Medallion vs ELT plano, grano de `dim_ies`, normalización)
- Validación con datos reales en cada paso (queries exploratorias)
- Curaduría del seed SUE (verificación contra datos reales, no inventar códigos)
- Debugging de integración
- Pruebas end-to-end

**Lecciones aprendidas:**
- La IA acelera 5-10x la escritura de código boilerplate
- La IA **no reemplaza** el criterio técnico: cada decisión pasó por validación humana contra datos reales
- La iteración rápida (escribir → validar con queries → corregir) es donde la IA brilla
- Los "hallazgos" del SNIES (`caracter_ies` con slash, `sector_ies` inconsistente) los encontró el desarrollador; la IA ayudó a corregirlos rápido

---

## 📋 Mapeo con los criterios del reto

| Criterio | Cómo se aborda | Ubicación |
|---|---|---|
| **A) Arquitectura** — Modelo OLAP, integración BI | Star schema en `gold.*`, Postgres expuesto por puerto 5432 | [§ Arquitectura](#-arquitectura), [`docs/architecture.md`](docs/architecture.md) |
| **A) Trazabilidad** — Medallion, linaje | Arquitectura Medallion + columnas `_ingested_at`, `_source_file`, `_dag_run_id`, `_row_hash` en Bronze | [`airflow/dags/utils/loaders.py`](airflow/dags/utils/loaders.py) |
| **A) Orquestación** | Airflow 2.9 con 3 DAGs encadenados vía Datasets | [§ Pipeline](#-pipeline-de-datos) |
| **B) Calidad** — Nulos, duplicados, normalización | 49 tests automatizados + normalización `sector_ies` en Silver + dedupe por `codigo_snies` | [§ Calidad](#-calidad-de-datos) |
| **B) Atributo SUE** | Columna `es_sue BOOLEAN` en `dim_ies` alimentada desde seed verificado | [`dbt/seeds/ies_sue.csv`](dbt/seeds/ies_sue.csv) |
| **C) Sostenibilidad** — Documentación | README + docs/ + doc inline en cada SQL + schema.yml descriptivos | [`docs/`](docs/) |
| **C) Sostenibilidad** — Escalabilidad | Arquitectura preparada para migración a cloud | [§ Escalabilidad](#-escalabilidad-y-siguientes-pasos) |
| **D) Docker** | `docker-compose.yaml` con 6 servicios. Un comando: `make up` | [`docker-compose.yaml`](docker-compose.yaml) |
| **E) Agilidad / Vibe Coding** | Uso documentado de Claude como copiloto técnico | [§ Agilidad](#-agilidad-y-vibe-coding) |

---

## 📚 Documentación adicional

- 📐 [Arquitectura detallada](docs/architecture.md)
- 🌳 [Linaje de modelos dbt](docs/dbt_lineage.md)
- 📊 [Queries para BI (Tableau/Metabase)](docs/queries_bi.sql)

---

## 📝 Licencia

Proyecto desarrollado como reto técnico. Los datos del SNIES son de dominio público (Ley 1712 de 2014 — Transparencia y Acceso a la Información Pública).

---

## 👤 Autor

Desarrollado como parte del proceso de selección para **Data Architect**.

Stack: Python • SQL • dbt • Airflow • Postgres • Docker
