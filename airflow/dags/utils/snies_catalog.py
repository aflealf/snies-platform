"""
Catálogo del SNIES: URLs de descarga y aliases de columnas por año.

Este módulo implementa el patrón "Column Mapper" que absorbe la variabilidad
de los archivos SNIES entre años.

HALLAZGOS REALES (inspección 2026-04):

    - Los archivos tienen una PORTADA antes de los datos (filas 0-4 o 0-6).
    - 2022: hoja única 'Matriculados E.S. 2022' / 'Docentes 2022', header en F7.
    - 2023 y 2024: dos hojas ('ÍNDICE' y '1.'), datos en '1.', header en F5.
    - Los nombres de columna cambian de capitalización y tildes entre años:
        2022: 'Código de la Institución' (con saltos de línea embebidos)
        2024: 'CÓDIGO DE LA INSTITUCIÓN' (MAYÚSCULAS)
    - El departamento viene como 'Bogotá, D.C.' (con tilde y coma, NO MAYÚSCULAS).

Para agregar un nuevo año:
    1. Añadir entrada en SNIES_CATALOG[anio].
    2. Si cambia el header o la hoja, ajustar `sheet_name` y `header_row`.
    3. Si aparecen nombres nuevos, añadir entrada en COLUMN_ALIASES.

URLs verificadas el 2026-04 en:
    https://snies.mineducacion.gov.co/portal/ESTADISTICAS/Bases-consolidadas/
"""

from typing import Literal

# =============================================================================
# Tipos
# =============================================================================
Perfil = Literal["matriculados", "docentes"]


# =============================================================================
# CATÁLOGO DE FUENTES
# =============================================================================
# Estructura: SNIES_CATALOG[año][perfil] = {url, sheet_name, header_row, filename}
#
# sheet_name: nombre exacto o índice de la hoja con datos.
# header_row: fila (0-indexed) donde está el encabezado real.
#             El extractor leerá desde header_row+1 hacia abajo como datos.
# =============================================================================

SNIES_CATALOG: dict[int, dict[Perfil, dict]] = {
    2022: {
        "matriculados": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-416244_recurso.xlsx",
            "sheet_name": "Matriculados E.S. 2022",
            "header_row": 7,
            "filename": "matriculados_2022.xlsx",
        },
        "docentes": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-416249_recurso.xlsx",
            "sheet_name": "Docentes 2022",
            "header_row": 7,
            "filename": "docentes_2022.xlsx",
        },
    },
    2023: {
        "matriculados": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-421539_recurso.xlsx",
            # Asumimos formato igual a 2024. Si falla, inspeccionar el archivo
            # y ajustar aquí. El extractor tiene auto-detección como fallback.
            "sheet_name": "1.",
            "header_row": 5,
            "filename": "matriculados_2023.xlsx",
        },
        "docentes": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-421822_recurso.xlsx",
            "sheet_name": "1.",
            "header_row": 5,
            "filename": "docentes_2023.xlsx",
        },
    },
    2024: {
        "matriculados": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-425151_recurso.xlsx",
            "sheet_name": "1.",
            "header_row": 5,
            "filename": "matriculados_2024.xlsx",
        },
        "docentes": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-425156_recurso.xlsx",
            "sheet_name": "1.",
            "header_row": 5,
            "filename": "docentes_2024.xlsx",
        },
    },
}


# =============================================================================
# MAPEO DE COLUMNAS
# =============================================================================
# IMPORTANTE: las claves en estos diccionarios se comparan contra el nombre
# YA NORMALIZADO: MAYÚSCULAS + sin saltos de línea + sin tildes + espacios
# colapsados. La normalización la hace `normalize_column_name()` en este módulo.
#
# Por ejemplo: "Código de \nla Institución"  → "CODIGO DE LA INSTITUCION"
#              "CÓDIGO DE LA INSTITUCIÓN"    → "CODIGO DE LA INSTITUCION"
# Ambos caen en la misma clave del mapping.
# =============================================================================

# Aliases comunes a ambos perfiles
COLUMN_ALIASES_COMUNES: dict[str, str] = {
    # --- Código SNIES de la IES ---
    "CODIGO DE LA INSTITUCION": "codigo_institucion",
    "CODIGO INSTITUCION": "codigo_institucion",
    "CODIGO IES": "codigo_institucion",
    "ID IES": "codigo_institucion",

    # --- IES padre (cuando es seccional) ---
    "IES PADRE": "ies_padre",

    # --- Nombre de la IES ---
    "INSTITUCION DE EDUCACION SUPERIOR IES": "institucion_educacion_superior_ies",
    "INSTITUCION DE EDUCACION SUPERIOR": "institucion_educacion_superior_ies",
    "IES": "institucion_educacion_superior_ies",
    "NOMBRE IES": "institucion_educacion_superior_ies",

    # --- Tipo (principal o seccional) ---
    # 2022 usa "Principal o Seccional" / 2024 usa "TIPO IES"
    "PRINCIPAL O SECCIONAL": "tipo_ies",
    "TIPO IES": "tipo_ies",
    "TIPO SEDE": "tipo_ies",

    # --- Sector (oficial/privada) ---
    "ID SECTOR IES": "id_sector_ies",
    "ID SECTOR": "id_sector_ies",
    "SECTOR IES": "sector_ies",
    "SECTOR": "sector_ies",

    # --- Carácter académico ---
    # 2022: "ID Caracter" (sin tilde, sin IES al final)
    # 2024: "ID CARÁCTER IES"
    "ID CARACTER": "id_caracter",
    "ID CARACTER IES": "id_caracter",
    "CARACTER IES": "caracter_ies",
    "CARACTER": "caracter_ies",
    "CARACTER ACADEMICO": "caracter_ies",

    # --- Ubicación geográfica de la IES ---
    "CODIGO DEL DEPARTAMENTO IES": "codigo_departamento_ies",
    "CODIGO DEL DEPARTAMENTO": "codigo_departamento_ies",

    "DEPARTAMENTO DE DOMICILIO DE LA IES": "departamento_domicilio_ies",
    "DEPARTAMENTO DE DOMICILIO IES": "departamento_domicilio_ies",
    "DEPARTAMENTO DE DOMICILIO": "departamento_domicilio_ies",
    "DEPARTAMENTO IES": "departamento_domicilio_ies",

    "CODIGO DEL MUNICIPIO IES": "codigo_municipio_ies",
    "CODIGO DEL MUNICIPIO": "codigo_municipio_ies",

    "MUNICIPIO DE DOMICILIO DE LA IES": "municipio_domicilio_ies",
    "MUNICIPIO DE DOMICILIO IES": "municipio_domicilio_ies",
    "MUNICIPIO DE DOMICILIO": "municipio_domicilio_ies",
    "MUNICIPIO IES": "municipio_domicilio_ies",

    # --- Acreditación ---
    "IES ACREDITADA": "ies_acreditada",

    # --- Periodo ---
    "ANO": "anio",   # Recordatorio: la tilde se remueve en la normalización
    "ANIO": "anio",
    "SEMESTRE": "semestre",
}

# Aliases específicos de MATRICULADOS (tiene granularidad por programa/sexo/nivel)
COLUMN_ALIASES_MATRICULADOS: dict[str, str] = {
    **COLUMN_ALIASES_COMUNES,

    # Métrica principal
    "MATRICULADOS": "matriculados",
    "TOTAL MATRICULADOS": "matriculados",
    "ESTUDIANTES MATRICULADOS": "matriculados",

    # Programa
    "CODIGO SNIES DEL PROGRAMA": "codigo_snies_programa",
    "CODIGO DEL PROGRAMA": "codigo_snies_programa",
    "PROGRAMA ACADEMICO": "programa_academico",
    "PROGRAMA": "programa_academico",
    "PROGRAMA ACREDITADO": "programa_acreditado",

    # Nivel académico / formación
    "ID NIVEL ACADEMICO": "id_nivel_academico",
    "NIVEL ACADEMICO": "nivel_academico",
    "ID NIVEL DE FORMACION": "id_nivel_formacion",
    "NIVEL DE FORMACION": "nivel_formacion",

    # Modalidad
    "ID MODALIDAD": "id_modalidad",
    "MODALIDAD": "modalidad",

    # Área de conocimiento
    "ID AREA": "id_area",
    "AREA DE CONOCIMIENTO": "area_conocimiento",
    "ID NUCLEO": "id_nucleo",
    "NUCLEO BASICO DEL CONOCIMIENTO NBC": "nucleo_basico_conocimiento",
    "NUCLEO BASICO DEL CONOCIMIENTO": "nucleo_basico_conocimiento",

    # CINE (clasificación internacional)
    "ID CINE CAMPO AMPLIO": "id_cine_campo_amplio",
    "DESC CINE CAMPO AMPLIO": "cine_campo_amplio",
    "ID CINE CAMPO ESPECIFICO": "id_cine_campo_especifico",
    "DESC CINE CAMPO ESPECIFICO": "cine_campo_especifico",
    "ID CINE CAMPO DETALLADO": "id_cine_campo_detallado",
    "DESC CINE CAMPO DETALLADO": "cine_campo_detallado",

    # Ubicación del PROGRAMA (puede diferir de la IES para programas a distancia)
    "CODIGO DEL DEPARTAMENTO PROGRAMA": "codigo_departamento_programa",
    "DEPARTAMENTO DE OFERTA DEL PROGRAMA": "departamento_oferta_programa",
    "CODIGO DEL MUNICIPIO PROGRAMA": "codigo_municipio_programa",
    "MUNICIPIO DE OFERTA DEL PROGRAMA": "municipio_oferta_programa",

    # Sexo
    "ID SEXO": "id_sexo",
    "SEXO": "sexo",
}

# Aliases específicos de DOCENTES
# Basado en inspección real de los archivos 2022 (granular), 2023, 2024.
COLUMN_ALIASES_DOCENTES: dict[str, str] = {
    **COLUMN_ALIASES_COMUNES,

    # --- Métrica principal ---
    # 2022: "No. de Docentes"  → tras normalizar se convierte en "NO DE DOCENTES"
    # 2023/2024: "DOCENTES"
    "DOCENTES": "docentes",
    "NO DE DOCENTES": "docentes",
    "NUMERO DE DOCENTES": "docentes",
    "TOTAL DOCENTES": "docentes",

    # --- Sexo del docente ---
    # 2022: "Sexo del \nDocente"  → "SEXO DEL DOCENTE"
    # 2023/2024: "SEXO DEL DOCENTE"
    "SEXO DEL DOCENTE": "sexo_docente",

    # --- Máximo nivel de formación del docente ---
    # 2022: "Máximo nivel \nde formación \ndel docente"  → "MAXIMO NIVEL DE FORMACION DEL DOCENTE"
    # 2023/2024: "MÁXIMO NIVEL DE FORMACIÓN DEL DOCENTE"
    "ID MAXIMO NIVEL DE FORMACION DEL DOCENTE": "id_nivel_formacion_docente",
    "MAXIMO NIVEL DE FORMACION DEL DOCENTE": "nivel_formacion_docente",
    # Variantes más cortas (por si en futuros años simplifican)
    "NIVEL DE FORMACION ALTO": "nivel_formacion_docente",
    "MAXIMO NIVEL DE FORMACION": "nivel_formacion_docente",

    # --- Nivel CINE (solo en 2022, descontinuado en 2023+) ---
    "NIVEL CINE": "nivel_cine",

    # --- Tiempo de dedicación ---
    # 2022: "Tiempo de dedicación \ndel Docente"
    # 2023/2024: "TIEMPO DE DEDICACIÓN DEL DOCENTE"
    "ID TIEMPO DE DEDICACION": "id_tiempo_dedicacion",
    "TIEMPO DE DEDICACION DEL DOCENTE": "tiempo_dedicacion_docente",
    "TIEMPO DE DEDICACION": "tiempo_dedicacion_docente",
    "DEDICACION": "tiempo_dedicacion_docente",

    # --- Tipo de contrato ---
    # 2022: "Tipo de contrato \ndel Docente"
    # 2023/2024: "TIPO DE CONTRATO"
    "ID TIPO DE CONTRATO": "id_tipo_contrato",
    "TIPO DE CONTRATO DEL DOCENTE": "tipo_contrato",
    "TIPO DE CONTRATO": "tipo_contrato",
}

# Diccionario que usará el extractor según perfil
COLUMN_ALIASES: dict[Perfil, dict[str, str]] = {
    "matriculados": COLUMN_ALIASES_MATRICULADOS,
    "docentes": COLUMN_ALIASES_DOCENTES,
}


# =============================================================================
# COLUMNAS MÍNIMAS REQUERIDAS
# =============================================================================
# Si alguna de estas columnas no se detecta tras aplicar el mapping,
# el extractor debe fallar explícitamente (no cargar Bronze incompleto).
# =============================================================================

REQUIRED_COLUMNS: dict[Perfil, list[str]] = {
    "matriculados": [
        "codigo_institucion",
        "institucion_educacion_superior_ies",
        "departamento_domicilio_ies",
        "anio",
        "semestre",
        "matriculados",
    ],
    "docentes": [
        "codigo_institucion",
        "institucion_educacion_superior_ies",
        "departamento_domicilio_ies",
        "anio",
        "semestre",
        "docentes",
    ],
}


# =============================================================================
# Helpers
# =============================================================================

def _normalize_for_matching(col: str) -> str:
    """
    Normaliza un nombre de columna cruda del SNIES para compararlo con los
    aliases. Es la clave del Column Mapper: permite que distintas formas
    del mismo nombre caigan en la misma entrada del diccionario.

    Aplica:
        1. Remueve saltos de línea y colapsa espacios múltiples.
        2. Convierte a MAYÚSCULAS.
        3. Remueve tildes (NFD + filtro de combining marks).
        4. Remueve paréntesis (solo los caracteres, no su contenido).

    Ejemplos:
        "Código de \\nla Institución"  → "CODIGO DE LA INSTITUCION"
        "CÓDIGO DE LA INSTITUCIÓN"     → "CODIGO DE LA INSTITUCION"
        "DEPARTAMENTO DE DOMICILIO (IES)" → "DEPARTAMENTO DE DOMICILIO IES"
    """
    import re
    import unicodedata

    # 1. Remueve saltos de línea y colapsa espacios
    s = re.sub(r"\s+", " ", str(col)).strip()

    # 2. MAYÚSCULAS
    s = s.upper()

    # 3. Remueve tildes
    s = "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

    # 4. Remueve paréntesis y otros símbolos problemáticos
    s = s.replace("(", "").replace(")", "").replace(".", "")

    # 5. Segunda pasada de colapsar espacios (paréntesis pueden dejar dobles)
    s = re.sub(r"\s+", " ", s).strip()

    return s


def get_source_config(anio: int, perfil: Perfil) -> dict:
    """
    Retorna la configuración de la fuente para un año y perfil dados.

    Raises:
        KeyError: si el año o perfil no están en el catálogo.
    """
    if anio not in SNIES_CATALOG:
        anios_disponibles = sorted(SNIES_CATALOG.keys())
        raise KeyError(
            f"Año {anio} no disponible en el catálogo. "
            f"Años disponibles: {anios_disponibles}"
        )
    if perfil not in SNIES_CATALOG[anio]:
        perfiles = list(SNIES_CATALOG[anio].keys())
        raise KeyError(
            f"Perfil '{perfil}' no disponible para {anio}. "
            f"Perfiles disponibles: {perfiles}"
        )
    return SNIES_CATALOG[anio][perfil]


def normalize_column_name(col: str, perfil: Perfil) -> str:
    """
    Convierte un nombre de columna cruda del SNIES a su nombre canónico.

    Si no se encuentra en el mapping, hace una normalización por defecto:
    MAYÚSCULAS → minúsculas, espacios → _, sin tildes.

    Args:
        col: nombre de columna tal como viene en el Excel.
        perfil: 'matriculados' o 'docentes' (determina qué mapping usar).

    Returns:
        Nombre canónico (snake_case).
    """
    # 1. Normalización para matching contra el diccionario
    col_key = _normalize_for_matching(col)
    mapping = COLUMN_ALIASES[perfil]

    # 2. Búsqueda exacta en el mapping
    if col_key in mapping:
        return mapping[col_key]

    # 3. Fallback: convertir a snake_case
    default_name = col_key.lower().replace(" ", "_")
    return default_name


def list_available_years() -> list[int]:
    """Retorna los años disponibles en el catálogo, ordenados."""
    return sorted(SNIES_CATALOG.keys())
