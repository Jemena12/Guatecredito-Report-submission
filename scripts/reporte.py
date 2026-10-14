"""
Genera reporte diario y acumulado desde MongoDB, Excel, PNG y envío por correo.
Pensado para ejecución local y en GitHub Actions (sin persistencia local entre corridas).
"""

from __future__ import annotations

import logging
import os
import smtplib
import sys
import tempfile
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any, Iterable

if "MPLCONFIGDIR" not in os.environ:
    _mpl_dir = os.path.join(tempfile.gettempdir(), "matplotlib-cache")
    os.makedirs(_mpl_dir, exist_ok=True)
    os.environ["MPLCONFIGDIR"] = _mpl_dir

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.offsetbox import AnnotationBbox, OffsetImage
from openpyxl.drawing.image import Image as XLImage
from openpyxl import load_workbook
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
from pymongo import MongoClient
from pymongo.collection import Collection

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

LOG = logging.getLogger("reporte")


@dataclass(frozen=True)
class Config:
    mongo_uri: str
    db_name: str
    lead_collection: str
    loans_collection: str
    users_collection: str
    ecosystem_id: str
    email_remitente: str
    email_password: str
    email_destinos: list[str]
    fecha_lanzamiento: date


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Falta la variable de entorno obligatoria: {name}")
    return value


def _parse_fecha_lanzamiento(raw: str) -> date:
    raw = raw.strip()
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            "FECHA_LANZAMIENTO debe ser YYYY-MM-DD (ej. 2026-03-02)"
        ) from exc


def load_config() -> Config:
    # Si el secreto no existe en GitHub Actions, la variable puede venir vacía.
    fecha_raw = (os.environ.get("FECHA_LANZAMIENTO") or "").strip() or "2026-03-02"
    destinos_raw = _require_env("EMAIL_DESTINO")
    destinos = [d.strip() for d in destinos_raw.split(",") if d.strip()]
    if not destinos:
        raise ValueError("EMAIL_DESTINO debe contener al menos un correo (separados por comas).")

    return Config(
        mongo_uri=_require_env("MONGO_URI"),
        db_name=_require_env("DB_NAME"),
        lead_collection=_require_env("LEAD_COLLECTION"),
        loans_collection=_require_env("LOANS_COLLECTION"),
        users_collection=_require_env("USERS_COLLECTION"),
        ecosystem_id=_require_env("ECOSYSTEM_ID"),
        email_remitente=_require_env("EMAIL_REMITENTE"),
        email_password=_require_env("EMAIL_PASSWORD"),
        email_destinos=destinos,
        fecha_lanzamiento=_parse_fecha_lanzamiento(fecha_raw),
    )


def script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def logo_path() -> str:
    return os.path.join(script_dir(), "logos", "guatecredito.png")


def load_logo_for_matplotlib():
    path = logo_path()
    if not os.path.isfile(path):
        LOG.warning("No se encontró %s; los gráficos irán sin logo.", path)
        return None
    return plt.imread(path)


def guatemala_tz() -> timezone:
    return timezone(timedelta(hours=-6))


def date_to_day_bounds_utc_iso(d: date) -> tuple[str, str]:
    """Inicio y fin del día calendario `d` en zona Guatemala, expresados en ISO UTC Z."""
    gt = guatemala_tz()
    inicio_gt = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=gt)
    fin_gt = inicio_gt + timedelta(days=1) - timedelta(seconds=1)
    inicio_utc = inicio_gt.astimezone(timezone.utc)
    fin_utc = fin_gt.astimezone(timezone.utc)
    return (
        inicio_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        fin_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def add_logo(ax, logo, x: float, y: float, zoom: float = 0.22) -> None:
    if logo is None:
        return
    imagebox = OffsetImage(logo, zoom=zoom)
    ab = AnnotationBbox(imagebox, (x, y), frameon=False, xycoords="axes fraction")
    ax.add_artist(ab)


CORTE_PENDIENTES = "2026-10-08"
FECHA_INICIO_ANULADOS = date(2026, 3, 29)
_EXACTOS_PENDIENTE = ("no estaban en base de datos", "no estaba en base de datos")
_FRASE_RENAP = "ocurrio un error al realizar la validacion contra renap"
_FRASE_CONSULTAR = "ocurrio un error al momento de consultar tu"


def _normalizar_texto(texto: str) -> str:
    """Minúsculas, sin tildes y sin espacios de más."""
    base = unicodedata.normalize("NFD", str(texto or ""))
    sin_tilde = "".join(c for c in base if unicodedata.category(c) != "Mn")
    return " ".join(sin_tilde.strip().lower().split()).rstrip(".")


def _fecha_gt_desde_iso(texto: str) -> str | None:
    crudo = str(texto or "").replace("Z", "+00:00")
    try:
        cuando = datetime.fromisoformat(crudo)
    except ValueError:
        return None
    if cuando.tzinfo is None:
        cuando = cuando.replace(tzinfo=timezone.utc)
    return cuando.astimezone(guatemala_tz()).strftime("%Y-%m-%d")


def _motivos_y_cupo(doc: dict) -> tuple[list[str], bool]:
    motivos: list[str] = []
    cupo = False
    variables = (doc.get("additional_data") or {}).get("variables_data") or []
    if not isinstance(variables, list):
        return motivos, cupo
    for item in variables:
        clave = str(item.get("key") or "").strip().lower()
        valor = str(item.get("value") or "").strip()
        if clave in ("motivos_negacion", "motivonegacion") and valor and valor not in motivos:
            motivos.append(valor)
        if clave == "montoaprobado":
            cupo = valor not in ("", "0", "None")
    return motivos, cupo


def _clasificar_lead(fecha: str, motivos: list[str], lead_status: str, cupo: bool) -> str | None:
    """Clasifica un lead según la fecha de solicitud y el motivo de negación.

    Hasta el 08/10/2026, pendiente es el texto exacto de no estar en base de
    datos, el error de Renap o el error al consultar. Desde el 09/10/2026,
    pendiente es solo Renap o el error al consultar. El NIS que no está en la
    BBDD sigue en rechazos BD.
    """
    normas = [texto for texto in (_normalizar_texto(m) for m in motivos) if texto]

    def es_pendiente(texto: str) -> bool:
        if any(frase in texto for frase in _EXACTOS_PENDIENTE):
            return fecha <= CORTE_PENDIENTES
        if _FRASE_RENAP in texto or _FRASE_CONSULTAR in texto:
            return True
        return False

    if any(es_pendiente(texto) for texto in normas):
        return "pendientes"
    if any("bbdd" in texto or "no se encuentra en la base de datos" in texto for texto in normas):
        return "bd"
    if any("politic" in texto or "ainova" in texto for texto in normas):
        return "politicas"
    if str(lead_status or "") == "new" and not cupo and not normas:
        return "inconclusas"
    return None


def _clasificar_leads_por_dia(
    lead_col: Collection, ecosystem_id: str, inicio_iso: str, fin_iso: str
) -> dict[str, dict[str, int]]:
    conteo: dict[str, dict[str, int]] = {}
    cursor = lead_col.find(
        {"ecosystem": ecosystem_id, "created_at": {"$gte": inicio_iso, "$lte": fin_iso}},
        {"created_at": 1, "lead_status": 1, "additional_data.variables_data": 1},
    )
    for doc in cursor:
        fecha = _fecha_gt_desde_iso(str(doc.get("created_at") or ""))
        if not fecha:
            continue
        motivos, cupo = _motivos_y_cupo(doc)
        tipo = _clasificar_lead(fecha, motivos, doc.get("lead_status"), cupo)
        if tipo is None:
            continue
        dia = conteo.setdefault(fecha, {"inconclusas": 0, "bd": 0, "pendientes": 0, "politicas": 0})
        dia[tipo] += 1
    return conteo


def _aprobados_por_dia(users_col: Collection, ecosystem_id: str, inicio_iso: str, fin_iso: str) -> dict[str, int]:
    conteo: dict[str, int] = {}
    cursor = users_col.find(
        {
            "ecosystem": ecosystem_id,
            "created_at": {"$gte": inicio_iso, "$lte": fin_iso},
            "quota": {"$gt": 0},
        },
        {"created_at": 1},
    )
    for doc in cursor:
        fecha = _fecha_gt_desde_iso(str(doc.get("created_at") or ""))
        if not fecha:
            continue
        conteo[fecha] = conteo.get(fecha, 0) + 1
    return conteo


def _loans_por_dia(loans_col: Collection, ecosystem_id: str, inicio_iso: str, fin_iso: str) -> dict[str, dict[str, float]]:
    pipeline = [
        {
            "$match": {
                "financial_entity_id": ecosystem_id,
                "assignment_date": {"$gte": inicio_iso, "$lte": fin_iso},
            }
        },
        {
            "$set": {
                "dt": {"$dateFromString": {"dateString": "$assignment_date", "onError": None, "onNull": None}}
            }
        },
        {"$match": {"dt": {"$ne": None}}},
        {
            "$set": {
                "fecha_gt": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$dt",
                        "timezone": "America/Guatemala",
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$fecha_gt",
                "creditos_nuevos": {"$sum": 1},
                "desembolsos": {"$sum": {"$cond": [{"$eq": ["$disbursed", True]}, 1, 0]}},
                "monto_desembolsado": {"$sum": {"$cond": [{"$eq": ["$disbursed", True]}, "$amount", 0]}},
            }
        },
    ]
    out: dict[str, dict[str, float]] = {}
    for doc in loans_col.aggregate(pipeline, allowDiskUse=True):
        fecha = str(doc.get("_id") or "")
        if not fecha:
            continue
        out[fecha] = {
            "creditos_nuevos": int(doc.get("creditos_nuevos") or 0),
            "desembolsos": int(doc.get("desembolsos") or 0),
            "monto_desembolsado": float(doc.get("monto_desembolsado") or 0) / 100.0,
        }
    return out


def _abonos_por_dia(col: Collection, entity_id: str, inicio_iso: str, fin_iso: str) -> dict[str, tuple[int, float, int]]:
    pipeline = [
        {"$match": {"financial_entity_id": entity_id, "date": {"$gte": inicio_iso, "$lte": fin_iso}}},
        {"$set": {"dt": {"$dateFromString": {"dateString": "$date", "onError": None, "onNull": None}}}},
        {"$match": {"dt": {"$ne": None}}},
        {
            "$set": {
                "fecha_gt": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$dt", "timezone": "America/Guatemala"}
                }
            }
        },
        {
            "$group": {
                "_id": "$fecha_gt",
                "cantidad_abonos": {"$sum": 1},
                "monto_abonos": {"$sum": "$amount"},
                "clientes": {"$addToSet": "$user_id"},
            }
        },
        {"$set": {"clientes_unicos": {"$size": {"$ifNull": ["$clientes", []]}}}},
    ]
    out: dict[str, tuple[int, float, int]] = {}
    for doc in col.aggregate(pipeline, allowDiskUse=True):
        fecha = str(doc.get("_id") or "")
        if not fecha:
            continue
        out[fecha] = (
            int(doc.get("cantidad_abonos") or 0),
            float(doc.get("monto_abonos") or 0) / 100.0,
            int(doc.get("clientes_unicos") or 0),
        )
    return out


def _anulados_por_dia(col: Collection, entity_id: str, inicio_iso: str, fin_iso: str) -> dict[str, int]:
    pipeline = [
        {"$match": {"financial_entity_id": entity_id}},
        {"$set": {"dt": {"$convert": {"input": "$deleted_at", "to": "date", "onError": None, "onNull": None}}}},
        {"$match": {"dt": {"$ne": None}}},
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$gte": ["$dt", {"$dateFromString": {"dateString": inicio_iso}}]},
                        {"$lte": ["$dt", {"$dateFromString": {"dateString": fin_iso}}]},
                    ]
                }
            }
        },
        {
            "$set": {
                "fecha_gt": {
                    "$dateToString": {"format": "%Y-%m-%d", "date": "$dt", "timezone": "America/Guatemala"}
                }
            }
        },
        {"$group": {"_id": "$fecha_gt", "cantidad_anulados": {"$sum": 1}}},
    ]
    out: dict[str, int] = {}
    for doc in col.aggregate(pipeline, allowDiskUse=True):
        fecha = str(doc.get("_id") or "")
        if fecha:
            out[fecha] = int(doc.get("cantidad_anulados") or 0)
    return out


def _fila_dia(
    dia: date,
    clasificacion: dict[str, dict[str, int]],
    aprobados_dia: dict[str, int],
    loans_dia: dict[str, dict[str, float]],
    abonos_dia: dict[str, tuple[int, float, int]],
    anulados_dia: dict[str, int],
) -> dict[str, Any]:
    fecha_str = dia.strftime("%Y-%m-%d")
    clases = clasificacion.get(fecha_str, {"inconclusas": 0, "bd": 0, "pendientes": 0, "politicas": 0})
    aprobados = int(aprobados_dia.get(fecha_str, 0))
    inconclusas = int(clases["inconclusas"])
    rechazos_bd = int(clases["bd"])
    pendientes = int(clases["pendientes"])
    politicas = int(clases["politicas"])
    clientes_nuevos = aprobados + rechazos_bd + pendientes + politicas
    prestamo = loans_dia.get(fecha_str, {"creditos_nuevos": 0, "desembolsos": 0, "monto_desembolsado": 0.0})
    desembolsos = int(prestamo["desembolsos"])
    monto = float(prestamo["monto_desembolsado"])
    abonos, monto_abonos, clientes_abonados = abonos_dia.get(fecha_str, (0, 0.0, 0))
    return {
        "Fecha": fecha_str,
        "Solicitudes": clientes_nuevos + inconclusas,
        "Solicitudes inconclusas": inconclusas,
        "Clientes nuevos": clientes_nuevos,
        "Rechazos BD": rechazos_bd,
        "Clientes aprobados": aprobados,
        "Clientes pendientes": pendientes,
        "Rechazos políticas": politicas,
        "Créditos nuevos": int(prestamo["creditos_nuevos"]),
        "Monto desembolsado": monto,
        "Ticket promedio": round(monto / desembolsos, 2) if desembolsos else 0,
        "# de abonos recibidos": abonos,
        "# de clientes abonados": clientes_abonados,
        "Monto abonos": monto_abonos,
        "Créditos anulados": int(anulados_dia.get(fecha_str, 0)),
        "_desembolsos": desembolsos,
    }


def build_dataframe_historico_mongo(
    lead_col: Collection,
    users_col: Collection,
    loans_col: Collection,
    payments_col: Collection,
    deleted_col: Collection,
    ecosystem_id: str,
    fecha_inicio: date,
    fecha_fin: date,
) -> pd.DataFrame:
    inicio_iso, _ = date_to_day_bounds_utc_iso(fecha_inicio)
    _, fin_iso = date_to_day_bounds_utc_iso(fecha_fin)
    inicio_anulados = max(fecha_inicio, FECHA_INICIO_ANULADOS)
    inicio_anulados_iso, _ = date_to_day_bounds_utc_iso(inicio_anulados)
    clasificacion = _clasificar_leads_por_dia(lead_col, ecosystem_id, inicio_iso, fin_iso)
    aprobados = _aprobados_por_dia(users_col, ecosystem_id, inicio_iso, fin_iso)
    loans = _loans_por_dia(loans_col, ecosystem_id, inicio_iso, fin_iso)
    abonos = _abonos_por_dia(payments_col, ecosystem_id, inicio_iso, fin_iso)
    anulados = _anulados_por_dia(deleted_col, ecosystem_id, inicio_anulados_iso, fin_iso)
    rows: list[dict[str, Any]] = []
    dia = fecha_inicio
    while dia <= fecha_fin:
        rows.append(_fila_dia(dia, clasificacion, aprobados, loans, abonos, anulados))
        dia += timedelta(days=1)
    return pd.DataFrame(rows)


def acumulado_desde_diario(df: pd.DataFrame) -> dict[str, Any]:
    """Suma las filas diarias. El ticket sale del monto total entre los desembolsos."""
    monto = float(df["Monto desembolsado"].sum())
    desembolsos = int(df["_desembolsos"].sum())
    return {
        "Solicitudes": int(df["Solicitudes"].sum()),
        "Solicitudes inconclusas": int(df["Solicitudes inconclusas"].sum()),
        "Clientes nuevos": int(df["Clientes nuevos"].sum()),
        "Rechazos BD": int(df["Rechazos BD"].sum()),
        "Clientes aprobados": int(df["Clientes aprobados"].sum()),
        "Clientes pendientes": int(df["Clientes pendientes"].sum()),
        "Rechazos políticas": int(df["Rechazos políticas"].sum()),
        "Créditos nuevos": int(df["Créditos nuevos"].sum()),
        "Monto desembolsado": monto,
        "Ticket promedio": round(monto / desembolsos, 2) if desembolsos else 0,
        "# de abonos recibidos": int(df["# de abonos recibidos"].sum()),
        "# de clientes abonados": int(df["# de clientes abonados"].sum()),
        "Monto abonos": float(df["Monto abonos"].sum()),
        "Créditos anulados": int(df["Créditos anulados"].sum()),
    }


def format_excel_workbook(path: str, fecha_ejecucion: str) -> None:
    wb = load_workbook(path)
    ws = wb.active
    for column in ws.columns:
        max_length = 0
        column_letter = get_column_letter(column[0].column)
        for cell in column:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
            except Exception:
                continue
        ws.column_dimensions[column_letter].width = max_length + 2

    ws["C1"] = "Reporte acumulado Guatecrédito"
    ws["C1"].font = Font(bold=True)
    ws["C2"] = f"Fecha de ejecución: {fecha_ejecucion}"

    lp = logo_path()
    if os.path.isfile(lp):
        try:
            logo_excel = XLImage(lp)
            logo_excel.width = 120
            logo_excel.height = 60
            ws.add_image(logo_excel, "A1")
        except Exception:
            LOG.exception("No se pudo insertar el logo en el Excel.")

    wb.save(path)


def render_report_image(
    title: str,
    subtitle_lines: Iterable[str],
    labels: list[str],
    values: list[Any],
    out_path: str,
    logo,
) -> None:
    fig, ax = plt.subplots(figsize=(6, 8))
    ax.axis("off")
    ax.text(0.5, 0.96, title, ha="center", fontsize=18, weight="bold")
    y_sub = 0.92
    for line in subtitle_lines:
        ax.text(0.5, y_sub, line, ha="center", fontsize=11)
        y_sub -= 0.04
    add_logo(ax, logo, 0.08, 0.95)
    add_logo(ax, logo, 0.92, 0.95)
    y = 0.78
    for label, value in zip(labels, values):
        ax.text(0.10, y, label, fontsize=10, weight="bold")
        ax.text(0.80, y, str(value), ha="right", fontsize=10)
        y -= 0.048
    plt.subplots_adjust(top=0.92)
    fig.savefig(out_path, dpi=300)
    plt.close(fig)


def send_email_gmail_ssl(
    remitente: str,
    password: str,
    destinos: list[str],
    asunto: str,
    cuerpo: str,
    adjuntos: list[tuple[str, str, str, bytes]],
) -> None:
    """
    adjuntos: lista de (maintype, subtype, filename, data)
    """
    msg = EmailMessage()
    msg["Subject"] = asunto
    msg["From"] = remitente
    msg["To"] = ", ".join(destinos)
    msg.set_content(cuerpo)
    for maintype, subtype, filename, data in adjuntos:
        msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=filename)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=120) as smtp:
        smtp.login(remitente, password)
        smtp.send_message(msg)


def main() -> int:
    setup_logging()
    if load_dotenv is not None:
        load_dotenv()

    try:
        cfg = load_config()
    except ValueError as e:
        LOG.error("%s", e)
        return 1

    gt = guatemala_tz()
    hoy = datetime.now(gt)
    fecha_consulta = hoy.date()
    fecha_consulta_str = fecha_consulta.strftime("%Y-%m-%d")
    hora_actual = hoy.strftime("%H%M%S")
    fecha_ejecucion = hoy.strftime("%Y-%m-%d %H:%M:%S")

    if fecha_consulta < cfg.fecha_lanzamiento:
        LOG.error(
            "La fecha actual en Guatemala (%s) es anterior a FECHA_LANZAMIENTO (%s).",
            fecha_consulta_str,
            cfg.fecha_lanzamiento,
        )
        return 1

    base_dir = os.path.join(script_dir(), "reportes")
    excel_dir = os.path.join(base_dir, "excel_acumulado")
    carpeta_dia = os.path.join(base_dir, fecha_consulta_str)
    os.makedirs(excel_dir, exist_ok=True)
    os.makedirs(carpeta_dia, exist_ok=True)

    archivo_excel = os.path.join(carpeta_dia, "reporte_general.xlsx")
    imagen_diaria = os.path.join(carpeta_dia, "reporte_diario.png")
    imagen_acumulada = os.path.join(carpeta_dia, "reporte_acumulado.png")

    inicio_dia_iso, fin_dia_iso = date_to_day_bounds_utc_iso(fecha_consulta)
    inicio_acum_gt = datetime(
        cfg.fecha_lanzamiento.year,
        cfg.fecha_lanzamiento.month,
        cfg.fecha_lanzamiento.day,
        0,
        0,
        0,
        tzinfo=gt,
    )
    inicio_acum_iso = inicio_acum_gt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    LOG.info("Rango día actual (UTC): %s .. %s", inicio_dia_iso, fin_dia_iso)
    LOG.info("Acumulado desde %s hasta fin del día %s (UTC inicio acum: %s)", cfg.fecha_lanzamiento, fecha_consulta_str, inicio_acum_iso)

    logo = load_logo_for_matplotlib()

    client: MongoClient | None = None
    try:
        client = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=30_000)
        client.admin.command("ping")
        db = client[cfg.db_name]
        lead_col = db[cfg.lead_collection]
        loans_col = db[cfg.loans_collection]
        users_col = db[cfg.users_collection]
        payments_col = db[(os.environ.get("PAYMENT_COLLECTION") or "payment").strip() or "payment"]
        deleted_col = db[(os.environ.get("DELETED_LOAN_COLLECTION") or "deleted_loan").strip() or "deleted_loan"]
    except Exception:
        LOG.exception("Error conectando a MongoDB.")
        return 1

    try:
        df_final = build_dataframe_historico_mongo(
            lead_col,
            users_col,
            loans_col,
            payments_col,
            deleted_col,
            cfg.ecosystem_id,
            cfg.fecha_lanzamiento,
            fecha_consulta,
        )
        acum = acumulado_desde_diario(df_final)
        df_excel = df_final.drop(columns=["_desembolsos"])
        df_excel.to_excel(archivo_excel, index=False, startrow=4)
        format_excel_workbook(archivo_excel, fecha_ejecucion)

        fila_hoy = df_final[df_final["Fecha"] == fecha_consulta_str]
        if fila_hoy.empty:
            LOG.error("No hay fila para el día actual en el DataFrame; revisar fechas.")
            return 1
        row = fila_hoy.iloc[0]

        labels = [
            "Solicitudes",
            "Solicitudes inconclusas",
            "Clientes nuevos",
            "Rechazos BD",
            "Clientes aprobados",
            "Clientes pendientes",
            "Rechazos políticas",
            "Créditos nuevos",
            "Monto desembolsado",
            "Ticket promedio",
            "# de abonos recibidos",
            "# de clientes abonados",
            "Monto abonos",
            "Créditos anulados",
        ]
        values_dia = [
            int(row["Solicitudes"]),
            int(row["Solicitudes inconclusas"]),
            int(row["Clientes nuevos"]),
            int(row["Rechazos BD"]),
            int(row["Clientes aprobados"]),
            int(row["Clientes pendientes"]),
            int(row["Rechazos políticas"]),
            int(row["Créditos nuevos"]),
            f"Q{float(row['Monto desembolsado']):,.2f}",
            f"Q{float(row['Ticket promedio']):,.2f}",
            int(row["# de abonos recibidos"]),
            int(row["# de clientes abonados"]),
            f"Q{float(row['Monto abonos']):,.2f}",
            int(row["Créditos anulados"]),
        ]

        render_report_image(
            "REPORTE DIARIO",
            [
                f"Fecha: {fecha_consulta_str}",
                f"Fecha de ejecución: {fecha_ejecucion}",
            ],
            labels,
            values_dia,
            imagen_diaria,
            logo,
        )

        values_acum = [
            acum["Solicitudes"],
            acum["Solicitudes inconclusas"],
            acum["Clientes nuevos"],
            acum["Rechazos BD"],
            acum["Clientes aprobados"],
            acum["Clientes pendientes"],
            acum["Rechazos políticas"],
            acum["Créditos nuevos"],
            f"Q{acum['Monto desembolsado']:,.2f}",
            f"Q{acum['Ticket promedio']:,.2f}",
            acum["# de abonos recibidos"],
            acum["# de clientes abonados"],
            f"Q{acum['Monto abonos']:,.2f}",
            acum["Créditos anulados"],
        ]

        render_report_image(
            "ACUMULADO",
            [
                f"Desde: {cfg.fecha_lanzamiento} hasta: {fecha_consulta_str}",
                f"Fecha de ejecución: {fecha_ejecucion}",
            ],
            labels,
            values_acum,
            imagen_acumulada,
            logo,
        )
    except Exception:
        LOG.exception("Error generando reportes o Excel.")
        return 1
    finally:
        if client is not None:
            client.close()

    correo_ok = False
    try:
        with open(imagen_diaria, "rb") as f:
            png_diario = f.read()
        with open(imagen_acumulada, "rb") as f:
            png_acum = f.read()
        with open(archivo_excel, "rb") as f:
            xlsx_data = f.read()

        send_email_gmail_ssl(
            cfg.email_remitente,
            cfg.email_password,
            cfg.email_destinos,
            f"Reporte Guatecrédito {fecha_consulta_str}",
            (
                "Adjunto reporte diario y acumulado generado.\n\n"
                f"Fecha de consulta: {fecha_consulta_str}\n"
                f"Hora (Guatemala): {hora_actual}"
            ),
            [
                ("image", "png", "reporte_diario.png", png_diario),
                ("image", "png", "reporte_acumulado.png", png_acum),
                (
                    "application",
                    "vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "reporte_general.xlsx",
                    xlsx_data,
                ),
            ],
        )
        correo_ok = True
        LOG.info("Correo enviado correctamente a: %s", ", ".join(cfg.email_destinos))
    except Exception:
        LOG.exception("Error enviando correo.")

    if not correo_ok:
        return 1

    LOG.info("Excel: %s", archivo_excel)
    LOG.info("PNG diario: %s", imagen_diaria)
    LOG.info("PNG acumulado: %s", imagen_acumulada)
    return 0


if __name__ == "__main__":
    sys.exit(main())
