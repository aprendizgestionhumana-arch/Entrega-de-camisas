from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from streamlit_autorefresh import st_autorefresh


# =========================================================
# CONFIG
# =========================================================
st.set_page_config(
    page_title="Entrega de camisas",
    page_icon="🍪",
    layout="centered",
)

SPREADSHEET_ID = "1o4J-MGyQ6GjJ_5UAZfJniqx4k-Rpl5LHGIvV64S3DzU"
HOJA_EMPLEADOS = "Empleados"
HOJA_ENTREGAS = "Entregas"

# Si subes un logo al repo, por ejemplo "logo_noel.png",
# cambia esta variable a True.
MOSTRAR_LOGO = False
RUTA_LOGO = "logo_noel.png"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

COLUMNAS_EMPLEADOS_REQUERIDAS = [
    "Código Trabajador",
    "Cédula",
    "Nombre",
    "Apellido1",
    "Apellido2",
]

COLUMNAS_ENTREGAS = [
    "codigo_trabajador",
    "cedula",
    "nombre_completo",
    "compania",
    "fecha_entrega",
]


# =========================================================
# AUTOREFRESH SILENCIOSO CADA 15 SEGUNDOS
# =========================================================
st_autorefresh(interval=15_000, key="auto_refresh_principal")


# =========================================================
# HELPERS
# =========================================================
def normalizar_texto(valor) -> str:
    if pd.isna(valor):
        return ""
    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto


def nombre_completo_desde_fila(fila: pd.Series) -> str:
    partes = [
        normalizar_texto(fila.get("Nombre", "")),
        normalizar_texto(fila.get("Apellido1", "")),
        normalizar_texto(fila.get("Apellido2", "")),
    ]
    return " ".join([p for p in partes if p]).strip()


def compania_desde_fila(fila: pd.Series) -> str:
    descripcion = normalizar_texto(fila.get("Descripcion", ""))
    compania = normalizar_texto(fila.get("Compañía", ""))
    return descripcion or compania


def validar_columnas_empleados(df: pd.DataFrame) -> List[str]:
    return [c for c in COLUMNAS_EMPLEADOS_REQUERIDAS if c not in df.columns]


# =========================================================
# GOOGLE SHEETS
# =========================================================
@st.cache_resource
def get_gspread_client():
    service_account_info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(
        service_account_info,
        scopes=SCOPES,
    )
    return gspread.authorize(creds)


@st.cache_resource
def get_spreadsheet():
    client = get_gspread_client()
    return client.open_by_key(SPREADSHEET_ID)


def get_worksheet(nombre_hoja: str):
    spreadsheet = get_spreadsheet()
    return spreadsheet.worksheet(nombre_hoja)


def asegurar_hoja_entregas():
    spreadsheet = get_spreadsheet()

    try:
        ws = spreadsheet.worksheet(HOJA_ENTREGAS)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=HOJA_ENTREGAS, rows=2000, cols=10)
        ws.append_row(COLUMNAS_ENTREGAS)
        return ws

    valores = ws.get_all_values()

    if not valores:
        ws.append_row(COLUMNAS_ENTREGAS)
        return ws

    encabezados_actuales = [str(x).strip() for x in valores[0]]

    if encabezados_actuales != COLUMNAS_ENTREGAS:
        max_cols = max(len(encabezados_actuales), len(COLUMNAS_ENTREGAS))
        if ws.col_count < max_cols:
            ws.add_cols(max_cols - ws.col_count)
        rango = f"A1:{chr(64 + len(COLUMNAS_ENTREGAS))}1"
        ws.update(rango, [COLUMNAS_ENTREGAS])

    return ws


def leer_empleados_directo() -> pd.DataFrame:
    ws = get_worksheet(HOJA_EMPLEADOS)
    data = ws.get_all_records()

    df = pd.DataFrame(data)
    if df.empty:
        return df

    df.columns = [str(c).strip() for c in df.columns]

    for col in ["Código Trabajador", "Cédula", "Nombre", "Apellido1", "Apellido2"]:
        if col in df.columns:
            df[col] = df[col].apply(normalizar_texto)

    return df


def leer_entregas_directo() -> pd.DataFrame:
    ws = asegurar_hoja_entregas()
    data = ws.get_all_records()

    if not data:
        return pd.DataFrame(columns=COLUMNAS_ENTREGAS)

    df = pd.DataFrame(data)
    df.columns = [str(c).strip() for c in df.columns]

    for col in COLUMNAS_ENTREGAS:
        if col not in df.columns:
            df[col] = ""

    df = df[COLUMNAS_ENTREGAS]

    for col in ["codigo_trabajador", "cedula"]:
        df[col] = df[col].apply(normalizar_texto)

    return df


@st.cache_data(ttl=8)
def cargar_empleados() -> pd.DataFrame:
    return leer_empleados_directo()


@st.cache_data(ttl=3)
def cargar_entregas() -> pd.DataFrame:
    return leer_entregas_directo()


def buscar_empleado(df: pd.DataFrame, termino_busqueda: str) -> pd.DataFrame:
    termino = normalizar_texto(termino_busqueda)
    if not termino or df.empty:
        return pd.DataFrame()

    resultado = df[
        (df["Código Trabajador"].astype(str).str.strip() == termino)
        | (df["Cédula"].astype(str).str.strip() == termino)
    ].copy()

    return resultado


def ya_fue_entregado(
    codigo_trabajador: str,
    cedula: str,
    entregas_df: pd.DataFrame,
) -> Optional[Dict]:
    if entregas_df.empty:
        return None

    codigo = normalizar_texto(codigo_trabajador)
    ced = normalizar_texto(cedula)

    filtro = entregas_df[
        (entregas_df["codigo_trabajador"].astype(str).str.strip() == codigo)
        | (entregas_df["cedula"].astype(str).str.strip() == ced)
    ]

    if filtro.empty:
        return None

    return filtro.iloc[0].to_dict()


def registrar_entrega(
    codigo_trabajador: str,
    cedula: str,
    nombre_completo: str,
    compania: str,
):
    ws = asegurar_hoja_entregas()
    fecha_hora_colombia = datetime.now(ZoneInfo("America/Bogota")).strftime("%Y-%m-%d %H:%M:%S")

    fila = [
        normalizar_texto(codigo_trabajador),
        normalizar_texto(cedula),
        nombre_completo,
        compania,
        fecha_hora_colombia,
    ]
    ws.append_row(fila)


# =========================================================
# SESSION STATE
# =========================================================
if "termino_busqueda" not in st.session_state:
    st.session_state.termino_busqueda = ""

if "flash_ok" not in st.session_state:
    st.session_state.flash_ok = False

if "flash_msg" not in st.session_state:
    st.session_state.flash_msg = ""

if "limpiar_busqueda" not in st.session_state:
    st.session_state.limpiar_busqueda = False

if st.session_state.limpiar_busqueda:
    st.session_state.termino_busqueda = ""
    st.session_state.limpiar_busqueda = False


# =========================================================
# ESTILOS
# =========================================================
st.markdown(
    """
    <style>
    .main-title {
        font-size: 2rem;
        font-weight: 800;
        margin-bottom: 0.2rem;
        text-align: center;
        color: #7a1f1f;
    }

    .subtle-text {
        color: #5c5c5c;
        margin-bottom: 1rem;
        text-align: center;
    }

    .employee-card {
        border: 2px solid #f3d6a4;
        border-radius: 20px;
        padding: 24px;
        background: linear-gradient(180deg, #fffdf8 0%, #fff3d9 100%);
        box-shadow: 0 4px 14px rgba(0,0,0,0.06);
        margin-top: 0.8rem;
        margin-bottom: 1rem;
    }

    .employee-company {
        font-size: 0.95rem;
        font-weight: 800;
        color: #8b5e00;
        text-transform: uppercase;
        margin-bottom: 0.6rem;
    }

    .employee-name {
        font-size: 1.9rem;
        font-weight: 900;
        color: #1f1f1f;
        line-height: 1.15;
        margin-bottom: 0.5rem;
    }

    .employee-id {
        font-size: 1.15rem;
        color: #333;
    }

    .status-ok {
        padding: 1rem;
        border-radius: 14px;
        background: #eefaf0;
        border: 1px solid #b8e0c2;
        color: #166534;
        font-weight: 800;
        text-align: center;
        margin-bottom: 1rem;
    }

    .status-bad {
        padding: 1rem;
        border-radius: 14px;
        background: #fff1f1;
        border: 1px solid #efb7b7;
        color: #991b1b;
        font-weight: 800;
        text-align: center;
        margin-bottom: 1rem;
    }

    .big-search-label {
        font-size: 1.15rem;
        font-weight: 800;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
        color: #7a1f1f;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# UI
# =========================================================
if MOSTRAR_LOGO:
    col_logo1, col_logo2, col_logo3 = st.columns([1, 2, 1])
    with col_logo2:
        st.image(RUTA_LOGO, width=180)

st.markdown(
    '<div class="main-title">Entrega Camisetas ¡El sabor de Creer! 🍪⚽</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="subtle-text">Busca por cédula o código de trabajador y registra la entrega.</div>',
    unsafe_allow_html=True,
)

if st.session_state.flash_ok:
    st.success(st.session_state.flash_msg or "✅ Entregado")
    st.session_state.flash_ok = False
    st.session_state.flash_msg = ""

with st.sidebar:
    st.subheader("Opciones")
    st.caption("La app se actualiza automáticamente cada 15 segundos.")
    if st.button("Refrescar datos", width="stretch"):
        cargar_empleados.clear()
        cargar_entregas.clear()
        st.rerun()

try:
    empleados_df = cargar_empleados()
    entregas_df = cargar_entregas()
except Exception as e:
    st.error(f"Error conectando con Google Sheets: {e}")
    st.stop()

if empleados_df.empty:
    st.error(f"La hoja '{HOJA_EMPLEADOS}' está vacía o no se pudo leer.")
    st.stop()

faltantes = validar_columnas_empleados(empleados_df)
if faltantes:
    st.error("Faltan columnas obligatorias en la hoja de empleados:")
    st.write(faltantes)
    st.stop()

total_empleados = len(empleados_df)
total_entregados = len(entregas_df)
pendientes = max(total_empleados - total_entregados, 0)

col1, col2, col3 = st.columns(3)
col1.metric("👥 Empleados", total_empleados)
col2.metric("✅ Entregados", total_entregados)
col3.metric("📦 Pendientes", pendientes)

if entregas_df.empty:
    st.caption("Aún no hay entregas registradas.")
else:
    st.markdown("### Últimos 5 entregados")
    ultimos_5 = entregas_df.copy().sort_values(
        by="fecha_entrega",
        ascending=False,
        kind="stable",
    ).head(5)

    columnas_visibles = ["cedula", "nombre_completo", "compania", "fecha_entrega"]
    columnas_visibles = [c for c in columnas_visibles if c in ultimos_5.columns]

    st.dataframe(
        ultimos_5[columnas_visibles],
        width="stretch",
    )

st.markdown('<div class="big-search-label">Digita la cédula o el código</div>', unsafe_allow_html=True)

termino_busqueda = st.text_input(
    "Buscar por cédula o código trabajador",
    placeholder="Ej: 71641330 o 11048",
    key="termino_busqueda",
    label_visibility="collapsed",
)

if termino_busqueda:
    resultado = buscar_empleado(empleados_df, termino_busqueda)

    if resultado.empty:
        st.warning("No encontré ningún colaborador con ese dato.")
    elif len(resultado) > 1:
        st.warning("Encontré más de un resultado. Revisa la hoja de empleados.")
        st.dataframe(resultado, width="stretch")
    else:
        empleado = resultado.iloc[0]
        codigo = normalizar_texto(empleado.get("Código Trabajador", ""))
        cedula = normalizar_texto(empleado.get("Cédula", ""))
        nombre_completo = nombre_completo_desde_fila(empleado)
        compania = compania_desde_fila(empleado)

        entrega_existente = ya_fue_entregado(codigo, cedula, entregas_df)

        st.markdown(
            f"""
            <div class="employee-card">
                <div class="employee-company">{compania}</div>
                <div class="employee-name">{nombre_completo}</div>
                <div class="employee-id"><strong>Cédula:</strong> {cedula}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if entrega_existente:
            st.markdown(
                '<div class="status-bad">🔴 Esta persona ya tiene una entrega registrada.</div>',
                unsafe_allow_html=True,
            )
            st.write(f"**Fecha registrada:** {entrega_existente.get('fecha_entrega', '')}")
        else:
            st.markdown(
                '<div class="status-ok">🟢 Disponible para entregar.</div>',
                unsafe_allow_html=True,
            )

            if st.button("Registrar entrega", type="primary", width="stretch"):
                entregas_actualizadas = leer_entregas_directo()
                entrega_existente_2 = ya_fue_entregado(codigo, cedula, entregas_actualizadas)

                if entrega_existente_2:
                    st.error("⚠️ Esta persona acaba de ser registrada por otra persona.")
                else:
                    try:
                        registrar_entrega(
                            codigo_trabajador=codigo,
                            cedula=cedula,
                            nombre_completo=nombre_completo,
                            compania=compania,
                        )

                        cargar_entregas.clear()
                        cargar_empleados.clear()

                        st.session_state.flash_ok = True
                        st.session_state.flash_msg = "✅ Entregado"
                        st.session_state.limpiar_busqueda = True
                        st.rerun()
                    except Exception as e:
                        st.error(f"No pude registrar la entrega: {e}")

st.divider()

with st.expander("Ver histórico de entregas"):
    if entregas_df.empty:
        st.caption("Aún no hay entregas registradas.")
    else:
        historico_mostrar = entregas_df.copy()
        historico_mostrar = historico_mostrar.sort_values(
            by="fecha_entrega",
            ascending=False,
            kind="stable",
        )
        st.dataframe(historico_mostrar, width="stretch")
