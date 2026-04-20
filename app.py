import json
from datetime import datetime

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Entrega de camisas", page_icon="👕", layout="wide")

# =========================
# CONFIGURA ESTO
# =========================
SPREADSHEET_ID = "1o4J-MGyQ6GjJ_5UAZfJniqx4k-Rpl5LHGIvV64S3DzU"
HOJA_EMPLEADOS = "Empleados"
HOJA_ENTREGAS = "Entregas"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

COLUMNAS_REQUERIDAS = [
    "Código Trabajador",
    "Cédula",
    "Nombre",
    "Apellido1",
    "Apellido2",
    "Compañía",
    "Descripcion",
]


# =========================
# CONEXION GOOGLE SHEETS
# =========================
@st.cache_resource

def get_gspread_client():
    service_account_info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_resource

def get_spreadsheet():
    client = get_gspread_client()
    return client.open_by_key(SPREADSHEET_ID)


def get_worksheet(nombre_hoja):
    spreadsheet = get_spreadsheet()
    return spreadsheet.worksheet(nombre_hoja)


# =========================
# UTILIDADES
# =========================
def normalizar_texto(valor):
    if pd.isna(valor):
        return ""
    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto


def validar_columnas(df):
    faltantes = [col for col in COLUMNAS_REQUERIDAS if col not in df.columns]
    return faltantes


@st.cache_data(ttl=10)
def cargar_empleados():
    ws = get_worksheet(HOJA_EMPLEADOS)
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    df.columns = [str(c).strip() for c in df.columns]

    for col in ["Código Trabajador", "Cédula", "Nombre", "Apellido1", "Apellido2"]:
        if col in df.columns:
            df[col] = df[col].apply(normalizar_texto)

    return df


@st.cache_data(ttl=5)
def cargar_entregas():
    ws = get_worksheet(HOJA_ENTREGAS)
    data = ws.get_all_records()

    columnas_esperadas = [
        "codigo_trabajador",
        "cedula",
        "nombre_completo",
        "compania",
        "fecha_entrega",
        "usuario_registra",
        "observacion",
    ]

    if not data:
        return pd.DataFrame(columns=columnas_esperadas)

    df = pd.DataFrame(data)
    df.columns = [str(c).strip() for c in df.columns]

    # Si la hoja existe pero tiene encabezados distintos o faltantes,
    # creamos las columnas faltantes para evitar KeyError.
    for col in columnas_esperadas:
        if col not in df.columns:
            df[col] = ""

    for col in ["codigo_trabajador", "cedula"]:
        df[col] = df[col].apply(normalizar_texto)

    return df[columnas_esperadas]


def buscar_empleado(df, termino_busqueda):
    termino = normalizar_texto(termino_busqueda)
    if not termino:
        return pd.DataFrame()

    resultado = df[
        (df["Código Trabajador"].astype(str).str.strip() == termino)
        | (df["Cédula"].astype(str).str.strip() == termino)
    ].copy()

    return resultado


def ya_fue_entregado(codigo_trabajador, cedula, entregas_df):
    codigo = normalizar_texto(codigo_trabajador)
    ced = normalizar_texto(cedula)

    if entregas_df.empty:
        return None

    filtro = entregas_df[
        (entregas_df["codigo_trabajador"].astype(str).str.strip() == codigo)
        | (entregas_df["cedula"].astype(str).str.strip() == ced)
    ]

    if filtro.empty:
        return None

    return filtro.iloc[0].to_dict()


def asegurar_hoja_entregas():
    spreadsheet = get_spreadsheet()
    encabezados = [
        "codigo_trabajador",
        "cedula",
        "nombre_completo",
        "compania",
        "fecha_entrega",
        "usuario_registra",
        "observacion",
    ]

    try:
        ws = spreadsheet.worksheet(HOJA_ENTREGAS)
        valores = ws.get_all_values()

        if not valores:
            ws.append_row(encabezados)
        else:
            encabezados_actuales = [str(x).strip() for x in valores[0]]
            if encabezados_actuales != encabezados:
                ws.clear()
                ws.append_row(encabezados)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=HOJA_ENTREGAS, rows=1000, cols=10)
        ws.append_row(encabezados)

    return ws


def registrar_entrega(codigo_trabajador, cedula, nombre_completo, compania, usuario_registra, observacion):
    ws = asegurar_hoja_entregas()
    ws.append_row([
        normalizar_texto(codigo_trabajador),
        normalizar_texto(cedula),
        nombre_completo,
        compania,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        usuario_registra,
        observacion,
    ])
    cargar_entregas.clear()


# =========================
# UI
# =========================
st.title("👕 Control de entrega de camisas")
st.caption("Consulta por cédula o código y evita duplicados en la misma hoja compartida.")

with st.sidebar:
    st.header("Configuración")
    usuario_registra = st.text_input("Nombre de quien entrega", placeholder="Ej: Estefany Molina")
    if st.button("Refrescar datos"):
        cargar_empleados.clear()
        cargar_entregas.clear()
        st.rerun()

try:
    empleados_df = cargar_empleados()
    entregas_df = cargar_entregas()
except Exception as e:
    st.error(f"Error conectando con Google Sheets: {e}")
    st.stop()

faltantes = validar_columnas(empleados_df)
if faltantes:
    st.error("Faltan columnas obligatorias en la hoja de empleados:")
    st.write(faltantes)
    st.stop()

col1, col2 = st.columns([2, 1])
with col1:
    termino_busqueda = st.text_input(
        "Buscar por cédula o código trabajador",
        placeholder="Ej: 71641330 o 11048",
    )
with col2:
    st.metric("Entregas registradas", len(entregas_df))

if termino_busqueda:
    resultado = buscar_empleado(empleados_df, termino_busqueda)

    if resultado.empty:
        st.warning("No encontré ningún empleado con ese dato.")
    elif len(resultado) > 1:
        st.warning("Encontré más de un resultado. Revisa la hoja Empleados.")
        st.dataframe(resultado, use_container_width=True)
    else:
        empleado = resultado.iloc[0]
        codigo = normalizar_texto(empleado["Código Trabajador"])
        cedula = normalizar_texto(empleado["Cédula"])
        nombre_completo = " ".join([
            normalizar_texto(empleado.get("Nombre", "")),
            normalizar_texto(empleado.get("Apellido1", "")),
            normalizar_texto(empleado.get("Apellido2", "")),
        ]).strip()
        compania = normalizar_texto(empleado.get("Descripcion", empleado.get("Compañía", "")))

        entrega_existente = ya_fue_entregado(codigo, cedula, entregas_df)

        st.subheader("Datos del empleado")
        st.write(f"**Compañía:** {compania}")
        st.write(f"**Nombre completo:** {nombre_completo}")
        st.write(f"**Cédula:** {cedula}")

        if entrega_existente:
            st.error("⚠️ Esta persona YA tiene una entrega registrada.")
            st.write(f"**Fecha de entrega registrada:** {entrega_existente.get('fecha_entrega', '')}")
            st.write(f"**Registrado por:** {entrega_existente.get('usuario_registra', '')}")
        else:
            with st.form("form_registro_entrega"):
                observacion = st.text_area("Observación", placeholder="Opcional")
                enviar = st.form_submit_button("Registrar entrega")

                if enviar:
                    if not usuario_registra.strip():
                        st.warning("Debes escribir el nombre de quien entrega.")
                    else:
                        # doble validacion antes de guardar
                        entregas_actualizadas = cargar_entregas()
                        entrega_existente_2 = ya_fue_entregado(codigo, cedula, entregas_actualizadas)

                        if entrega_existente_2:
                            st.error("⚠️ Esta persona acaba de ser registrada por otra persona. Refresca la pantalla.")
                        else:
                            registrar_entrega(
                                codigo_trabajador=codigo,
                                cedula=cedula,
                                nombre_completo=nombre_completo,
                                compania=compania,
                                usuario_registra=usuario_registra.strip(),
                                observacion=observacion.strip(),
                            )
                            st.success("✅ Entrega registrada correctamente.")
                            st.rerun()

st.divider()
st.subheader("Histórico de entregas")
if entregas_df.empty:
    st.caption("Aún no hay entregas registradas.")
else:
    st.dataframe(entregas_df, use_container_width=True)
