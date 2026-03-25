# Reporte diario Guatecrédito

Genera desde **MongoDB** el reporte del día, el acumulado desde la fecha de lanzamiento, un Excel histórico (una fila por día), dos PNG y envía todo por **Gmail (SMTP SSL)**.

## Requisitos

- Python 3.11+
- Logo opcional: coloca `guatecredito.png` en `scripts/logos/` (si falta, el reporte se genera sin logo).

## Ejecución local

1. Copia `.env.example` a `.env` y completa los valores (sin subir `.env` a Git).
2. Crea un entorno virtual e instala dependencias:

   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. Ejecuta:

   ```bash
   python scripts/reporte.py
   ```

Las salidas quedan en `scripts/reportes/<YYYY-MM-DD>/` (`reporte_general.xlsx`, `reporte_diario.png`, `reporte_acumulado.png`).

## Secretos en GitHub

En el repositorio: **Settings → Secrets and variables → Actions → New repository secret**, crea al menos:

| Secreto | Descripción |
|--------|-------------|
| `MONGO_URI` | Cadena de conexión MongoDB |
| `DB_NAME` | Nombre de la base de datos |
| `LEAD_COLLECTION` | Colección de leads |
| `LOANS_COLLECTION` | Colección de préstamos |
| `USERS_COLLECTION` | Colección de usuarios |
| `ECOSYSTEM_ID` | UUID del ecosistema |
| `EMAIL_REMITENTE` | Cuenta Gmail que envía |
| `EMAIL_PASSWORD` | Contraseña de aplicación de Google |
| `EMAIL_DESTINO` | Destinatarios separados por comas |

Opcional:

| Secreto | Descripción |
|--------|-------------|
| `FECHA_LANZAMIENTO` | Inicio del acumulado y del Excel, formato `YYYY-MM-DD`. Si no lo defines, el script usa `2026-03-02` por defecto. |

## Automatización (GitHub Actions)

El workflow [`.github/workflows/reporte-diario.yml`](.github/workflows/reporte-diario.yml):

- Corre en **ubuntu-latest** con Python **3.11**.
- Se ejecuta **cada día** por cron (hora UTC configurable en el YAML).
- Permite ejecución manual con **Run workflow** (`workflow_dispatch`).

Los runners no guardan archivos entre ejecuciones: el Excel **se reconstruye completo** consultando Mongo día a día desde `FECHA_LANZAMIENTO` hasta la fecha actual en zona **Guatemala (UTC-6)**.

## Notas

- Para Gmail usa una [contraseña de aplicación](https://support.google.com/accounts/answer/185833), no la contraseña normal de la cuenta.
- Asegúrate de que el logo `scripts/logos/guatecredito.png` esté en el repositorio si quieres marca en gráficos y Excel.
