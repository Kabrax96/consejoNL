# lambda_handler.py
import os
from app.etl_central.connectors.postgresql import PostgreSqlClient

from app.etl_central.pipelines.balance_presupuestario_pipeline import run_balance_pipeline
from app.etl_central.pipelines.egresos_detallado_pipeline import run_egresos_pipeline as run_egresos_single
from app.etl_central.pipelines.ingresos_detallados_pipeline import run_pipeline as run_ingresos_single

# Optional bulk routes (don’t error if missing)
try:
    from app.etl_central.pipelines.balance_presupuestario_bulk_pipeline import run_balance_pipeline as run_balance_bulk
except Exception:
    run_balance_bulk = None
try:
    from app.etl_central.pipelines.egresos_detallados_bulk_pipeline import run_egresos_pipeline as run_egresos_bulk
except Exception:
    run_egresos_bulk = None
try:
    from app.etl_central.pipelines.ingresos_detallados_bulk_pipeline import run_ingresos_pipeline as run_ingresos_bulk
except Exception:
    run_ingresos_bulk = None

def _log_client():
    return PostgreSqlClient(
        server_name=os.environ["LOGGING_SERVER_NAME"],
        database_name=os.environ["LOGGING_DATABASE_NAME"],
        username=os.environ["LOGGING_USERNAME"],
        password=os.environ["LOGGING_PASSWORD"],
        port=int(os.environ.get("LOGGING_PORT", "5432")),
    )

def handler(event, context):
    target = ((event or {}).get("pipeline") or os.environ.get("PIPELINE_TARGET", "")).strip().lower()

    # ensure file logs go to Lambda's writable dir
    os.environ.setdefault("LOG_DIR", "/tmp/logs")
    os.makedirs(os.environ["LOG_DIR"], exist_ok=True)

    routes = {
        "balance_single": lambda: run_balance_pipeline("balance_presupuestario_pipeline", _log_client()),
        "egresos_single": lambda: run_egresos_single("egresos_detallado_pipeline", _log_client()),
        "ingresos_single": lambda: run_ingresos_single("ingresos_detallado_pipeline", _log_client()),
        "balance_bulk": (lambda: run_balance_bulk("balance_presupuestario_bulk_pipeline", _log_client())) if run_balance_bulk else None,
        "egresos_bulk": (lambda: run_egresos_bulk("egresos_detallado_bulk_pipeline", _log_client())) if run_egresos_bulk else None,
        "ingresos_bulk": (lambda: run_ingresos_bulk("ingresos_detallado_bulk_pipeline", _log_client())) if run_ingresos_bulk else None,
    }

    if target not in routes or routes[target] is None:
        valid = [k for k, v in routes.items() if v is not None]
        raise ValueError(f"Unknown or unavailable pipeline '{target}'. Valid: {valid}")

    routes[target]()
    return {"ok": True, "pipeline": target}
