# 🏛️ Arquitectura Técnica — SNIES Data Platform

> **Proyecto:** Monitoreo de capacidad académica de IES en Bogotá (2022–2024)
> **Métrica principal:** Relación Estudiante por Docente
> **Reto Técnico:** Data Architect

---

## 1. Visión general

La plataforma implementa un **pipeline ELT moderno** bajo el patrón **Medallion Architecture** (Bronze → Silver → Gold), totalmente containerizado con Docker Compose y reproducible en un solo comando. Se desacoplan **orquestación**, **almacenamiento** y **transformación** para garantizar escalabilidad horizontal y mantenibilidad.

### Principios de diseño aplicados

| Principio | Aplicación concreta |
|---|---|
| **Separación de responsabilidades** | Airflow orquesta; dbt transforma; Postgres almacena |
| **Idempotencia** | Cada DAG puede re-ejecutarse sin duplicar datos (`TRUNCATE + INSERT` por partición año) |
| **Trazabilidad** | Columnas de linaje en Bronze (`_ingested_at`, `_source_file`, `_source_url`, `_dag_run_id`, `_row_hash`) |
| **Schema-on-read en Bronze** | Absorbe variabilidad de archivos SNIES entre años (72 columnas reales mapeadas) |
| **Schema-on-write en Gold** | Star schema estricto, tipos fuertes, integridad referencial |
| **ELT > ETL** | Cargar primero, transformar dentro del DWH con SQL declarativo (dbt) |
| **Desacoplamiento de DAGs** | Airflow Datasets conectan DAGs por el dato producido, no por triggers explícitos |

---

## 2. Diagrama de arquitectura

Ver [`docs/architecture.mermaid`](architecture.mermaid) — GitHub/GitLab lo renderizan automáticamente.

### Flujo end-to-end

```
[SNIES .xlsx]
     │
     ├─(1)─▶ Airflow DAG descarga archivo con URL parametrizada por año
     │
     ├─(2)─▶ Column Mapper normaliza headers (72 variantes entre 2022/2023/2024)
     │
     ├─(3)─▶ Carga a schema BRONZE en Postgres (raw + metadata de linaje)
     │
     ├─(4)─▶ Publicación de Dataset dwh://bronze/updated (Airflow 2.4+)
     │
     ├─(5)─▶ snies_transform arranca AUTOMÁTICAMENTE al detectar el Dataset
     │
     ├─(6)─▶ dbt SILVER: dedupe IES, normaliza sector, filtra Bogotá
     │
     ├─(7)─▶ dbt GOLD: star schema con atributo SUE vía seed verificado
     │
     ├─(8)─▶ dbt test: 49 validaciones automatizadas
     │
     └─(9)─▶ Tableau/Metabase consumen tablas Gold por JDBC/ODBC
```

---

## 3. Stack tecnológico y justificación

| Capa | Tecnología | Versión | Alternativas evaluadas | Razón de la elección |
|---|---|---|---|---|
| **Orquestación** | Apache Airflow | 2.9.3 | Prefect, Dagster | Sugerido por el reto; estándar industrial; soporte nativo de Datasets para encadenamiento |
| **Data Warehouse** | PostgreSQL | 16 | DuckDB, ClickHouse, BigQuery | Soporte nativo en Tableau y cualquier BI; ACID; open-source; ruta directa a RDS/Cloud SQL para producción |
| **Transformación** | dbt-core + dbt-postgres | 1.8.2 | Scripts SQL puros, SQLMesh | Versionado Git; 49 tests declarativos; lineage automático; macros reutilizables; industry standard |
| **Extracción** | Python 3.11 + `pandas` + `openpyxl` | — | Airbyte, Fivetran | Los .xlsx del SNIES tienen schemas variables por año → requiere lógica custom; ingestion managers agregan overhead innecesario para 3 fuentes estructuradas |
| **BI (demo)** | Metabase | latest | Superset, Redash | Más liviano; UI amigable; demuestra que cualquier BI funciona sobre el mismo Postgres |
| **Containerización** | Docker Compose | v2 | Kubernetes | Requisito del reto; migración a K8s es directa con Helm charts |

---

## 4. Modelo de datos

### 4.1 Capa Bronze (Raw + linaje)

**Objetivo:** preservar los datos originales sin pérdida para permitir reprocesamiento futuro.

```sql
CREATE SCHEMA bronze;

CREATE TABLE bronze.matriculados_raw (
    -- Columnas originales del SNIES (todas TEXT para absorber variabilidad)
    codigo_institucion                      TEXT,
    institucion_educacion_superior_ies      TEXT,
    tipo_ies                                TEXT,
    sector_ies                              TEXT,
    caracter_ies                            TEXT,
    departamento_domicilio_ies              TEXT,
    municipio_domicilio_ies                 TEXT,
    ies_acreditada                          TEXT,
    -- ... resto de columnas del SNIES ...
    anio                                    TEXT,
    semestre                                TEXT,
    matriculados                            TEXT,

    -- Columnas de linaje (añadidas por el loader)
    _ingested_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _source_file    TEXT NOT NULL,
    _source_url     TEXT NOT NULL,
    _dag_run_id     TEXT NOT NULL,
    _row_hash       TEXT NOT NULL   -- MD5 para detección de duplicados
);

CREATE TABLE bronze._ingestion_log (
    id SERIAL PRIMARY KEY,
    perfil TEXT, anio TEXT, status TEXT,
    rows_ingested INTEGER,
    started_at TIMESTAMP, finished_at TIMESTAMP,
    error_message TEXT, dag_run_id TEXT
);
```

**Volumen real cargado:**

| Tabla | Filas | Bogotá | % Bogotá |
|---|---:|---:|---:|
| `bronze.matriculados_raw` | 217,303 | 120,773 | 55.6% |
| `bronze.docentes_raw` | 50,038 | 16,706 | 33.4% |

**Decisión clave:** todas las columnas como `TEXT` en Bronze. La variabilidad entre años (headers en mayúsculas vs capitalizado, con saltos de línea, columnas renombradas) se absorbe aquí, no en el extractor.

### 4.2 Capa Silver (cleaning + normalización)

Tres vistas (`materialized='view'`) que se recomputan desde Bronze cada `dbt run`.

#### `stg_ies` — deduplicación de IES

Toma la unión de `matriculados_raw` y `docentes_raw` (algunas IES aparecen solo en una), deduplica por `codigo_institucion` tomando el registro más reciente, y **normaliza `sector_ies`**:

```sql
CASE
    WHEN UPPER(sector_ies) IN ('PRIVADO', 'PRIVADA') THEN 'Privada'
    WHEN UPPER(sector_ies) IN ('OFICIAL', 'PÚBLICO', 'PUBLICO') THEN 'Oficial'
    ELSE sector_ies
END AS sector_ies
```

**Hallazgo real:** el SNIES es inconsistente — usa `'Privado'` en algunas filas y `'Privada'` en otras para el mismo sector. Se canonicaliza aquí en Silver.

#### `stg_matriculados_bogota` y `stg_docentes_bogota`

Filtro + agregación:

```sql
WHERE UPPER(departamento_domicilio_ies) LIKE 'BOGOT%'          -- cubre 'Bogotá D.C.' y 'Bogotá, D.C.'
  AND caracter_ies IN ('Universidad', 'Institución Universitaria/Escuela Tecnológica')
GROUP BY codigo_snies, anio
```

**Hallazgo real:** el SNIES usa literalmente `'Institución Universitaria/Escuela Tecnológica'` (con slash), NO `'Institución Universitaria'` solo. El filtro original estaba equivocado y se corrigió tras inspeccionar los datos crudos.

### 4.3 Capa Gold (star schema materializado)

Tablas (`materialized='table'`) optimizadas para consumo BI.

#### `dim_ies`

```sql
codigo_snies           INTEGER PRIMARY KEY     -- clave SNIES
nombre_ies             VARCHAR                  -- nombre oficial
sector_ies             VARCHAR                  -- 'Oficial' o 'Privada' (normalizado)
caracter_ies           VARCHAR                  -- Universidad / IU / IU-ET
departamento_ies       VARCHAR                  -- siempre Bogotá (filtrado)
municipio_ies          VARCHAR
ies_acreditada         VARCHAR
es_sue                 BOOLEAN                  -- ⭐ Atributo Plus
fecha_ultima_carga     TIMESTAMP
```

**Volumen real:** 91 filas (31 Universidad + 60 Institución Universitaria/Escuela Tecnológica).

**Distribución:**
- Por sector: 19 Oficial, 72 Privada
- Por SUE: 6 TRUE (UNAL, Pedagógica, Militar, Colegio Mayor, Distrital, UNAD), 85 FALSE

#### `dim_tiempo`

Dimensión temporal **dinámica**: se construye con `SELECT DISTINCT anio` desde las tablas Silver. Se extiende automáticamente cuando se añaden nuevos años.

```sql
anio INTEGER PRIMARY KEY
anio_texto VARCHAR
decada INTEGER
es_bisiesto BOOLEAN
```

**Volumen real:** 3 filas (2022, 2023, 2024).

#### `fact_capacidad_academica` ⭐

**Hecho principal del reto.**

```sql
codigo_snies                INTEGER     FK → dim_ies
anio                        INTEGER     FK → dim_tiempo
total_matriculados          INTEGER
total_docentes              INTEGER
ratio_estudiante_docente    NUMERIC(10,2)   -- ⭐ Métrica principal
flag_calidad                VARCHAR         -- 'ok', 'sin_docentes', 'docentes_bajos', ...
fecha_calculo               TIMESTAMP
```

**Granularidad:** una fila por `(codigo_snies, anio)`.

**Volumen real:** 265 filas (91 IES × 3 años, con algunos gaps cuando una IES no reporta en un año).

**Agregación anual:** `SUM(matriculados_semestre_1) + SUM(matriculados_semestre_2)` — convención del MEN. La doble contabilización se cancela en el ratio.

**Flag de calidad:** marca outliers para que dashboards filtren (`WHERE flag_calidad = 'ok'`) sin perder las filas en el DWH.

---

## 5. Decisiones de diseño justificadas

### 5.1 Grano de `dim_ies` = sede (`codigo_snies`), no universidad legal

**Hallazgo real:** una misma universidad tiene múltiples códigos SNIES. La Universidad Nacional tiene 7 códigos (1101 Bogotá, 1102 Antioquia, 1103 Caldas, 1104 Valle del Cauca, 1125 Amazonas, 1126 San Andrés, 9933 Cesar). Universidad de Antioquia tiene 7 también. UPTC tiene 4, UDEC tiene 3.

**Decisión:** grano = sede. Una fila por `codigo_snies`.

**Justificación:**
- Fidelidad al sistema fuente: `codigo_snies` es el identificador natural en el SNIES.
- Para este reto (solo Bogotá), cada universidad pública tiene 1 sede en Bogotá, por lo que el grano "sede" coincide con "universidad legal".
- El seed `ies_sue.csv` incluye columna `universidad_sue` que permite agregar a nivel legal cuando se expanda a nacional.

### 5.2 Suma de semestres para la agregación anual

**Decisión:** `total_matriculados = SUM(S1) + SUM(S2)` por año.

**Justificación:**
- Convención oficial del MEN en informes consolidados.
- Para el ratio E/D, la doble contabilización se cancela (ambos numerador y denominador suman semestres).
- Preserva la señal de "volumen total de servicio" prestado en el año.

### 5.3 Universo de IES: Universidad + Institución Universitaria/Escuela Tecnológica

**Decisión:** incluir 2 caracteres académicos (91 IES en total).

**Justificación:**
- El término coloquial "universidad" incluye ambas categorías del SNIES.
- Excluir las IU dejaría fuera instituciones relevantes: Politécnico Grancolombiano (105K matriculados), Uniminuto, Konrad Lorenz, Los Libertadores.
- Se excluyen `Institución Tecnológica` e `Institución Técnica Profesional` (carreras cortas, naturaleza distinta).

### 5.4 Atributo SUE como columna booleana

**Decisión:** `es_sue BOOLEAN` en `dim_ies`, no tablas separadas.

**Justificación:**
- Un solo modelo de verdad; los dashboards filtran en BI.
- Permite comparaciones directas SUE vs Privadas sin joins adicionales.
- Fuente: seed `ies_sue.csv` con 51 sedes SUE validadas contra los datos reales (no lista externa inventada).

### 5.5 Airflow Datasets para encadenamiento Ingest → Transform

**Decisión:** `Dataset("dwh://bronze/updated")` en vez de `TriggerDagRunOperator`.

**Justificación:**
- Feature nativa de Airflow 2.4+, diseñada exactamente para este caso.
- Desacopla los DAGs: cualquier nuevo productor de Bronze dispara automáticamente el transform.
- Visualización de linaje en la UI de Airflow → Datasets.

---

## 6. Calidad de datos

### 6.1 Inventario de tests (49 en total)

| Capa | Cantidad | Tipos aplicados |
|---|---:|---|
| **Bronze (sources)** | 4 | `not_null` en columnas críticas |
| **Silver (staging)** | 18 | `not_null`, `unique_combination`, `accepted_values`, `relationships` |
| **Gold (star schema)** | 26 | `not_null`, `unique`, `relationships`, `accepted_values` |
| **Custom** | 1 | `fact_solo_bogota.sql` — invariante del reto |

**Estado actual:** `PASS=49 WARN=0 ERROR=0`

### 6.2 Normalización de nombres IES (criterio del reto)

**Estrategia:** la verdad canónica es el **código SNIES** (numérico, estable entre años). El nombre es atributo derivado. Variaciones tipográficas del nombre NO generan duplicados porque el grano es por código.

En `stg_ies` se dedupe por `codigo_institucion` con `ROW_NUMBER() OVER (PARTITION BY codigo_institucion ORDER BY anio DESC)`, tomando el registro más reciente. Esto maneja el caso real detectado: el código `3828` tiene 2 nombres distintos entre años (probable cambio de razón social).

### 6.3 Manejo de nulos y duplicados

| Tipo | Dónde | Estrategia |
|---|---|---|
| Nulos en `codigo_institucion` | Silver | `WHERE codigo_institucion IS NOT NULL` descarta filas basura |
| Nulos en métricas | Silver | `WHERE matriculados IS NOT NULL AND matriculados >= 0` |
| Duplicados IES (renombres) | Silver | `ROW_NUMBER()` por código, toma el más reciente |
| Duplicados de ingesta | Bronze | `_row_hash` MD5 + `_dag_run_id` permiten detectar re-cargas |

---

## 7. Manejo de variabilidad entre años (Fase A del reto)

**Problema real:** el SNIES publica Excel con formato distinto cada año:

| Año | Hojas | Header en fila | Formato columnas |
|---|---|---|---|
| 2022 | hoja única | fila 7 | Capitalizado con saltos de línea ("Código de \nla Institución") |
| 2023 | "ÍNDICE" + "1." | fila 5 | MAYÚSCULAS |
| 2024 | "ÍNDICE" + "1." | fila 5 | MAYÚSCULAS |

Y cambio de nombre en columnas clave: `"No. de Docentes"` (2022) → `"DOCENTES"` (2023+).

**Solución:** patrón **Column Mapper** en `airflow/dags/utils/extractors.py`:

```python
# 1. Normaliza cada header: UPPER + quita tildes + quita \n + quita paréntesis
def normalize_header(h):
    return (h.upper()
             .replace('\n', ' ')
             .translate(str.maketrans('ÁÉÍÓÚÑ', 'AEIOUN'))
             .split('(')[0]
             .strip())

# 2. Mapea headers normalizados → nombre canónico en Bronze
COLUMN_ALIASES = {
    'CODIGO DE LA INSTITUCION': 'codigo_institucion',
    'CODIGO INSTITUCION':       'codigo_institucion',
    'ID IES':                   'codigo_institucion',
    # ... 72 mapeos en total ...
}
```

El catálogo (`snies_catalog.py`) define URL, hoja, fila de header y perfil por año:

```python
SNIES_CATALOG = {
    2024: {
        "matriculados": {
            "url": "https://snies.mineducacion.gov.co/.../articles-425151_recurso.xlsx",
            "sheet": "1.", "header_row": 4,
        },
        ...
    },
    2023: {...},
    2022: {...},
}
```

**Para agregar un nuevo año:** añadir entrada en `SNIES_CATALOG` + revisar si hay headers nuevos para añadir al mapper. **Sin cambios en DAGs, Silver, ni Gold.**

---

## 8. Escalabilidad y transferencia de conocimiento

### Escenario actual (Bogotá 2022–2024)

- **Volumen bruto:** ~35 MB de Excel (3 años × 2 perfiles × ~6MB c/u)
- **Filas en Bronze:** 267K
- **Filas en Gold:** 265
- **Performance:** backfill completo en ~10 min; transform en ~2 min
- **Postgres local maneja esto trivialmente**

### Escala a Colombia completa + histórico 2007–2024

- **Volumen estimado:** ~2 GB raw, ~5M filas en Silver, ~30K filas en Gold
- **Gold se mantiene chico** (≤50K filas) porque el grano es `codigo_snies × anio` — escala linealmente con años y sedes

**Cambios necesarios (mínimos, sin rediseño):**

| Dimensión | Cambio |
|---|---|
| **Ingesta** | `TaskGroup` paralelo por año × perfil ya existe (`snies_backfill_all`); cambiar a `CeleryExecutor` para 2-3 workers paralelos |
| **Almacenamiento** | Particionar `bronze.*_raw` por año con `PARTITION BY RANGE (anio)`; Gold sigue cabiendo en Postgres normal |
| **Transformación** | dbt ya escala; subir `threads: 8` en `profiles.yml`; convertir fact a `incremental` si matrícula supera 50M filas |
| **Infra** | Migrar de Docker Compose a **Kubernetes** con Helm charts oficiales de Airflow; Postgres → **RDS Aurora** o **Cloud SQL** |
| **Observabilidad** | Añadir **OpenLineage** para linaje cross-tools; **Prometheus + Grafana** para métricas de DAGs |
| **Data Quality** | Los 49 tests dbt son suficientes; añadir **Great Expectations** o **Soda Core** si se necesitan suites más ricas |
| **Secretos** | En producción: integrar Airflow con **HashiCorp Vault** o **AWS Secrets Manager** |

**Lo crítico:** la arquitectura **Medallion + dbt + Airflow no requiere rediseño**. Los componentes se cambian por versiones gestionadas o particionadas.

### Expansión del dominio

Ya contemplada en el diseño:

1. **Agregar años:** entrada en `SNIES_CATALOG`. Cero cambios en dbt.
2. **Agregar departamentos:** cambiar `dbt_project.yml:vars.bogota_pattern`.
3. **Agregar métricas:** columnas nuevas en `fact_capacidad_academica`.
4. **Granularidad semestral:** añadir `semestre` a `dim_tiempo` y cambiar `GROUP BY` en staging.
5. **Nuevos DAGs:** se integran al encadenamiento vía `Dataset`, sin modificar los existentes.

### CI/CD recomendado para producción

```
push → GitHub Actions:
  ├── dbt parse                       (valida SQL sin correrlo)
  ├── pytest                          (módulos utils/)
  ├── dbt build --select source:*     (valida Bronze + corre sources)
  ├── dbt docs generate               (regenera lineage)
  └── docker build + push
```

---

## 9. Seguridad

| Aspecto | Implementación actual | Upgrade producción |
|---|---|---|
| **Credenciales DWH** | En `.env` (no en Git), `.gitignore` configurado | Vault / AWS Secrets Manager |
| **Airflow Connections** | Via `AIRFLOW_CONN_*` env vars | Secrets backend configurado |
| **Red Docker** | Red `snies-net` aislada | VPC privada + security groups |
| **Postgres** | `fe_sendauth` passworded, puerto expuesto para BI | SSL/TLS obligatorio, IAM auth |
| **Logs** | Airflow logs en volumen local | CloudWatch / Stackdriver |

---

## 10. Agilidad (Vibe Coding)

Se documenta el uso de **Claude (Anthropic)** como copiloto técnico:

### Delegado a IA:
- Scaffolding inicial de proyecto (estructura, Dockerfile, `dbt_project.yml`)
- **Column Mapper** para 72 variantes de headers del SNIES
- Modelos SQL dbt y tests declarativos en `schema.yml`
- Debugging de problemas sutiles (schemas `public_silver` vs `silver`, variables de entorno en `BashOperator`)
- Documentación

### Mantenido en el desarrollador:
- Decisiones arquitectónicas (Medallion vs ELT plano, grano de `dim_ies`, normalización)
- Validación contra datos reales en cada paso (queries exploratorias)
- Curaduría del seed SUE (códigos verificados contra Bronze, no inventados)
- Debugging de integración
- Pruebas end-to-end

### Lecciones aplicadas en este proyecto:
- La IA acelera 5-10x el código boilerplate
- No reemplaza el criterio técnico: cada decisión pasó por validación humana con datos reales
- Los hallazgos del SNIES (caracter con slash, sector inconsistente, codigo_snies como sede) se descubrieron con queries exploratorias, no con corazonadas

---

## 11. Entregables mapeados al reto

| Requisito del PDF del reto | Dónde se cumple |
|---|---|
| Código fuente en repositorio | Repo completo con estructura clara |
| Diagrama de arquitectura | [`docs/architecture.mermaid`](architecture.mermaid) + [`docs/architecture.png`](architecture.png) |
| Guía de instalación y ejecución | [`README.md`](../README.md) → "Quick start" (2 comandos) |
| Decisiones técnicas | Este documento (§5) + [`README.md`](../README.md) |
| Transferencia de Conocimiento (escalabilidad) | §8 de este documento |
| **Fase A** (ETL + orquestación) | 3 DAGs Airflow + Python extractor con Column Mapper |
| **Fase B** (OLAP + trazabilidad + BI) | Postgres + dbt + Medallion + linaje en Bronze |
| **Fase C** (accesible para Tableau) | Postgres puerto 5432 expuesto, schema `gold.*` disponible |
| **Fase D** (Docker) | `docker-compose.yaml` con 6 servicios, `make up` |
| **Criterio Arquitectura** | Medallion + star schema + Airflow Datasets |
| **Criterio Calidad** | 49 tests dbt + normalización sector + dedupe IES |
| **Criterio Sostenibilidad** | README + docs/ + schema.yml descriptivos |
| **Criterio Agilidad** | §10 con uso documentado de Claude |

---

## 12. Estructura del repositorio

```
snies-platform/
├── docker-compose.yaml              # 6 servicios: DWH, Airflow, Metabase
├── .env.example                     # Template (se auto-copia a .env en `make up`)
├── Makefile                         # make up | down | shell-dwh | dbt-test | ...
├── README.md                        # Quick start + diagrama embebido
│
├── airflow/
│   ├── Dockerfile                   # Airflow 2.9 + dbt-postgres
│   ├── requirements.txt
│   └── dags/
│       ├── snies_ingest_dag.py          # Ingesta parametrizada por año
│       ├── snies_backfill_all_dag.py    # Backfill masivo con dynamic mapping
│       ├── snies_transform_dag.py       # dbt orquestado (Silver + Gold)
│       └── utils/
│           ├── snies_catalog.py         # URLs + schemas por año
│           ├── extractors.py            # Column Mapper + parser xlsx
│           └── loaders.py               # Carga idempotente a Bronze
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                 # Usa env_var() para credenciales
│   ├── packages.yml                 # dbt_utils 1.3.3
│   ├── macros/
│   │   ├── snies_helpers.sql        # es_bogota, es_universidad, safe_cast_int
│   │   └── generate_schema_name.sql # Override para schemas limpios
│   ├── models/
│   │   ├── bronze/sources.yml       # Declaración de Bronze
│   │   ├── silver/
│   │   │   ├── stg_ies.sql
│   │   │   ├── stg_matriculados_bogota.sql
│   │   │   ├── stg_docentes_bogota.sql
│   │   │   └── schema.yml           # 18 tests
│   │   └── gold/
│   │       ├── dim_ies.sql
│   │       ├── dim_tiempo.sql
│   │       ├── fact_capacidad_academica.sql
│   │       └── schema.yml           # 26 tests
│   ├── seeds/
│   │   ├── ies_sue.csv              # 51 sedes SUE verificadas
│   │   └── seeds.yml
│   └── tests/
│       └── fact_solo_bogota.sql     # Test custom: invariante del reto
│
├── postgres/
│   └── init.sql                     # CREATE SCHEMA bronze, silver, gold
│
└── docs/
    ├── architecture.md              # Este documento
    ├── architecture.mermaid         # Diagrama renderizable
    ├── architecture.png             # Diagrama exportado
    ├── dbt_lineage.md               # Grafo dbt + descripción de cada modelo
    └── queries_bi.sql               # 10 queries listas para Tableau/Metabase
```

---

**Estado del proyecto:** ✅ Pipeline end-to-end funcional y validado.
**Tests:** 49/49 en verde.
**Ejecutable con:** `make up` (un solo comando).
