# 🌳 Linaje de modelos dbt

Este documento describe el **grafo de dependencias** del proyecto dbt y lo que hace cada modelo, capa por capa.

---

## Grafo completo (DAG de dbt)

```
                        ╔═══════════════════════════════╗
                        ║       BRONZE (sources)         ║
                        ║    [ Cargado por Airflow ]     ║
                        ╚═══════════════════════════════╝
                                      │
             ┌────────────────────────┼────────────────────────┐
             │                        │                        │
             ▼                        ▼                        ▼
┌─────────────────────────┐ ┌─────────────────────┐ ┌─────────────────┐
│ bronze.matriculados_raw │ │ bronze.docentes_raw │ │ _ingestion_log  │
│  (217,303 filas)        │ │   (50,038 filas)    │ │ (no es source)  │
└──────────┬──────────────┘ └──────────┬──────────┘ └─────────────────┘
           │                            │
           └─────────────┬──────────────┘
                         │
                         ▼
       ╔═══════════════════════════════════════════╗
       ║              SILVER (staging)              ║
       ║       [ Vistas: recomputadas siempre ]     ║
       ╚═══════════════════════════════════════════╝
                         │
        ┌────────────────┼──────────────────────┐
        │                │                      │
        ▼                ▼                      ▼
┌──────────────┐  ┌─────────────────────────┐  ┌──────────────────────┐
│   stg_ies    │  │ stg_matriculados_bogota │  │ stg_docentes_bogota  │
│  (115 IES)   │  │    (265 filas)          │  │    (265 filas)       │
└──────┬───────┘  └───────────┬─────────────┘  └───────────┬──────────┘
       │                      │                            │
       │                      │    ┌──────────────────┐    │
       │                      │    │  ies_sue (seed)  │    │
       │                      │    │  (51 filas)      │    │
       │                      │    └────┬─────────────┘    │
       │                      │         │                  │
       └──────────┬───────────┼─────────┘                  │
                  │           │                            │
                  ▼           ▼                            │
       ╔════════════════════════════════════════════════╗  │
       ║              GOLD (star schema)                ║  │
       ║           [ Tablas materializadas ]            ║  │
       ╚════════════════════════════════════════════════╝  │
                  │                                        │
       ┌──────────┼──────────────┐                         │
       │                         │                         │
       ▼                         ▼                         │
 ┌──────────────┐     ┌────────────────────┐               │
 │   dim_ies    │     │    dim_tiempo      │               │
 │   (91 IES)   │     │     (3 años)       │               │
 └──────┬───────┘     └──────┬─────────────┘               │
        │                    │                             │
        │                    │                             │
        └───────────┬────────┴─────────────┬───────────────┘
                    │                      │
                    ▼                      ▼
              ┌──────────────────────────────────┐
              │    fact_capacidad_academica      │
              │         (265 filas)              │
              │  ⭐ ratio_estudiante_docente    │
              └──────────────────────────────────┘
```

Para regenerar este grafo en dbt localmente:

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs generate"
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs serve --port 8081"
# Abre http://localhost:8081 en tu navegador
```

---

## 🥉 Capa Bronze (sources, no son modelos dbt)

| Tabla | Descripción | Granularidad |
|---|---|---|
| `bronze.matriculados_raw` | Datos crudos de matriculados de todas las IES de Colombia | IES × programa × nivel × sexo × año × semestre |
| `bronze.docentes_raw` | Datos crudos de docentes | IES × sexo × dedicación × tipo_contrato × año × semestre |
| `bronze._ingestion_log` | Log de ingestas (duración, filas, estado) | 1 fila por ejecución |

**Columnas de linaje** (presentes en todas las tablas Bronze):
- `_ingested_at` — timestamp de ingestión
- `_source_file` — nombre del archivo .xlsx original
- `_dag_run_id` — run de Airflow que cargó la fila
- `_row_hash` — hash MD5 para detectar duplicados

Los sources están declarados en [`models/bronze/sources.yml`](../dbt/models/bronze/sources.yml).

---

## 🥈 Capa Silver (staging)

Todas las vistas Silver (`materialized='view'`) — se recomputan desde Bronze cada vez que se consulta. Rápido de iterar.

### `stg_ies`

**Archivo:** [`models/silver/stg_ies.sql`](../dbt/models/silver/stg_ies.sql)
**Propósito:** Dimensión de IES deduplicada a nivel nacional.

**Transformaciones:**
1. `UNION` entre `matriculados_raw` y `docentes_raw` (algunas IES solo aparecen en uno)
2. `ROW_NUMBER()` por `codigo_institucion`, toma el registro más reciente → evita duplicados cuando una IES cambia de nombre entre años
3. **Normaliza `sector_ies`**: `'Privado'/'Privada'` → `'Privada'`; `'Oficial'/'Público'` → `'Oficial'`
4. Cast seguro de `codigo_institucion` a entero

**Salida:** ~115 IES (todas las del país)
**Clave:** `codigo_snies` (unique)

### `stg_matriculados_bogota`

**Archivo:** [`models/silver/stg_matriculados_bogota.sql`](../dbt/models/silver/stg_matriculados_bogota.sql)
**Propósito:** Matriculados agregados por (IES × año), solo Bogotá, solo universidades.

**Transformaciones:**
1. Filtro Bogotá: `UPPER(departamento_domicilio_ies) LIKE 'BOGOT%'` (cubre 'Bogotá D.C.' y 'Bogotá, D.C.')
2. Filtro tipo IES: `caracter_ies IN ('Universidad', 'Institución Universitaria/Escuela Tecnológica')`
3. Agregación: `SUM(matriculados) GROUP BY codigo_snies, anio` (suma de ambos semestres)
4. Filtros de calidad: `matriculados IS NOT NULL AND matriculados >= 0`

**Salida:** ~91 universidades × 3 años ≈ 270 filas
**Clave compuesta:** (`codigo_snies`, `anio`)

### `stg_docentes_bogota`

**Archivo:** [`models/silver/stg_docentes_bogota.sql`](../dbt/models/silver/stg_docentes_bogota.sql)
**Propósito:** Docentes agregados por (IES × año), solo Bogotá, solo universidades.

**Idéntica lógica a `stg_matriculados_bogota`** pero para la tabla de docentes. Mantener en modelos separados permite:
- Evolucionar las reglas de filtrado independientemente
- Detectar en tests si hay IES presentes en un modelo pero no en el otro

**Salida:** ~91 universidades × 3 años
**Clave compuesta:** (`codigo_snies`, `anio`)

### `ies_sue` (seed)

**Archivo:** [`seeds/ies_sue.csv`](../dbt/seeds/ies_sue.csv)
**Propósito:** Lookup de sedes pertenecientes al Sistema Universitario Estatal (SUE).

**Contenido:**
- 51 filas (1 por sede)
- Columnas: `codigo_snies`, `nombre_oficial`, `departamento_principal`, `universidad_sue`

**Nota:** Una universidad SUE puede tener múltiples códigos (Universidad Nacional tiene 7: 1101, 1102, 1103, 1104, 1125, 1126, 9933). La columna `universidad_sue` permite agrupar por universidad legal.

**Verificación:** Todos los códigos fueron validados contra `bronze.matriculados_raw` filtrando `sector_ies='Oficial' AND caracter_ies='Universidad'`.

---

## 🥇 Capa Gold (star schema)

Tablas materializadas (`materialized='table'`) — persistidas en disco para queries rápidas desde BI.

### `dim_ies`

**Archivo:** [`models/gold/dim_ies.sql`](../dbt/models/gold/dim_ies.sql)
**Propósito:** Dimensión final de IES, restringida a universidades de Bogotá, enriquecida con atributo SUE.

**Transformaciones:**
1. Toma `stg_ies` y filtra Bogotá + Universidad/IU
2. `LEFT JOIN` con seed `ies_sue` para marcar `es_sue`
3. Índices en `codigo_snies` (unique) y en `es_sue` (para filtros rápidos en BI)

**Columnas clave:**
| Columna | Tipo | Descripción |
|---|---|---|
| `codigo_snies` | integer | 🔑 PK |
| `nombre_ies` | varchar | Nombre oficial de la IES |
| `sector_ies` | varchar | `'Oficial'` o `'Privada'` (normalizado) |
| `caracter_ies` | varchar | Tipo académico |
| `es_sue` | boolean | ⭐ **Atributo Plus**: pertenece al SUE |
| `ies_acreditada` | varchar | Estado de acreditación |
| `departamento_ies` | varchar | Siempre Bogotá (filtrado) |

**Salida:** 91 filas (6 es_sue=TRUE, 85 es_sue=FALSE)

### `dim_tiempo`

**Archivo:** [`models/gold/dim_tiempo.sql`](../dbt/models/gold/dim_tiempo.sql)
**Propósito:** Dimensión temporal, construida dinámicamente.

**Transformación:** `SELECT DISTINCT anio` de las tablas Silver. Extiende automáticamente cuando se añaden nuevos años.

**Columnas:**
| Columna | Tipo |
|---|---|
| `anio` | integer (PK) |
| `anio_texto` | varchar |
| `decada` | integer |
| `es_bisiesto` | boolean |

**Salida:** 3 filas (2022, 2023, 2024)

### `fact_capacidad_academica` ⭐

**Archivo:** [`models/gold/fact_capacidad_academica.sql`](../dbt/models/gold/fact_capacidad_academica.sql)
**Propósito:** **HECHO PRINCIPAL del reto.** Relación estudiante/docente por (IES × año).

**Transformaciones:**
1. `INNER JOIN` entre `stg_matriculados_bogota` y `stg_docentes_bogota` por (codigo_snies, anio)
2. `INNER JOIN` con `dim_ies` para garantizar integridad referencial
3. Calcula `ratio_estudiante_docente = total_matriculados / total_docentes`
4. Genera flag de calidad para resaltar outliers en BI

**Columnas:**
| Columna | Tipo | Descripción |
|---|---|---|
| `codigo_snies` | integer | 🔗 FK → `dim_ies.codigo_snies` |
| `anio` | integer | 🔗 FK → `dim_tiempo.anio` |
| `total_matriculados` | integer | Suma de ambos semestres |
| `total_docentes` | integer | Suma de ambos semestres |
| `ratio_estudiante_docente` | numeric | ⭐ **Métrica del reto** |
| `flag_calidad` | varchar | `'ok'`, `'sin_docentes'`, `'docentes_bajos'`, etc. |
| `fecha_calculo` | timestamp | Auditoría |

**Salida:** 265 filas
**Clave compuesta:** (`codigo_snies`, `anio`)

**Flag de calidad:**
- `'ok'` — valores razonables
- `'sin_docentes'` — `total_docentes = 0`
- `'sin_matriculados'` — `total_matriculados = 0`
- `'docentes_bajos'` — `total_docentes < 10`
- `'matriculados_bajos'` — `total_matriculados < 100`

Estos flags permiten a los dashboards **excluir outliers** (`WHERE flag_calidad = 'ok'`) sin perder las filas en el DWH.

---

## 🧪 Tests del proyecto dbt

**49 tests automatizados** declarados en los archivos `schema.yml`:

### Por modelo

| Modelo | Tests | Detalle |
|---|---|---|
| **Sources Bronze** | 4 | `not_null` en `codigo_institucion`, `anio` de ambas tablas |
| **`stg_ies`** | 4 | `unique(codigo_snies)`, `not_null(codigo_snies)`, `not_null(nombre_ies)` |
| **`stg_matriculados_bogota`** | 7 | not_null, relationships → stg_ies, accepted_values, unique_combination |
| **`stg_docentes_bogota`** | 7 | idénticos a matriculados |
| **`ies_sue`** | 4 | unique, not_null en varios campos |
| **`dim_ies`** | 7 | unique, not_null, accepted_values para sector/caracter/es_sue |
| **`dim_tiempo`** | 3 | unique, not_null, accepted_values |
| **`fact_capacidad_academica`** | 11 | relationships → dim_ies, relationships → dim_tiempo, unique_combination, expression_is_true (ratios ≥ 0) |
| **Test custom** | 1 | `fact_solo_bogota.sql` — invariante del reto |

### Test custom clave: `fact_solo_bogota`

**Archivo:** [`tests/fact_solo_bogota.sql`](../dbt/tests/fact_solo_bogota.sql)

```sql
-- Test: todas las IES del fact deben ser de Bogotá
SELECT f.codigo_snies, f.anio, d.departamento_ies
FROM {{ ref('fact_capacidad_academica') }} f
LEFT JOIN {{ ref('dim_ies') }} d USING (codigo_snies)
WHERE d.departamento_ies IS NULL
   OR UPPER(d.departamento_ies) NOT LIKE 'BOGOT%'
```

Si este test devuelve **cualquier fila**, el modelo está roto: el fact tiene IES fuera de Bogotá.

---

## 🔧 Macros del proyecto

**Archivo:** [`macros/snies_helpers.sql`](../dbt/macros/snies_helpers.sql)

Utilidades reutilizables a lo largo de los modelos:

| Macro | Uso |
|---|---|
| `{{ es_bogota('col') }}` | Expande a `UPPER(col) LIKE 'BOGOT%'` — uso uniforme del filtro |
| `{{ es_universidad('col') }}` | Expande a `col IN ('Universidad', 'Institución Universitaria/Escuela Tecnológica')` |
| `{{ safe_cast_int('col') }}` | Cast seguro a entero: tolera nulls, comas, espacios |

**Archivo:** [`macros/generate_schema_name.sql`](../dbt/macros/generate_schema_name.sql)

Override de la macro estándar de dbt para evitar que los schemas se prefijen con `public_`. Resultado: schemas limpios `silver` y `gold` en lugar de `public_silver`/`public_gold`.

---

## 📐 Regenerar documentación

Después de cambios en modelos o schemas.yml:

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs generate"
```

Servir localmente:

```bash
docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs serve --host 0.0.0.0 --port 8081"
```

Luego abre `http://localhost:8081` (necesitas exponer el puerto en `docker-compose.yaml`).
