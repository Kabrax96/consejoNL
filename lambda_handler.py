# lambda_handler.py
import os
from urllib.parse import unquote_plus
from app.etl_central.assets.pipeline_logging import PipelineLogging

# Import the raw pipeline() functions (not the run_* wrappers)
from app.etl_central.pipelines.egresos_detallado_pipeline import pipeline as egresos_single_pipeline
from app.etl_central.pipelines.egresos_detallados_bulk_pipeline import pipeline as egresos_bulk_pipeline

# Optional routes if you want them too (safe to leave out)
try:
    from app.etl_central.pipelines.ingresos_detallados_pipeline import pipeline as ingresos_single_pipeline
except Exception:
    ingresos_single_pipeline = None
try:
    from app.etl_central.pipelines.ingresos_detallados_bulk_pipeline import pipeline as ingresos_bulk_pipeline
except Exception:
    ingresos_bulk_pipeline = None
try:
    from app.etl_central.pipelines.balance_presupuestario_pipeline import pipeline as balance_single_pipeline
except Exception:
    balance_single_pipeline = None
try:
    from app.etl_central.pipelines.balance_presupuestario_bulk_pipeline import pipeline as balance_bulk_pipeline
except Exception:
    balance_bulk_pipeline = None


def _route_from_s3_event(event):
    """Return a pipeline target based on S3 key prefix, or None."""
    try:
        rec = event["Records"][0]
        if rec.get("eventSource") != "aws:s3":
            return None
        key = unquote_plus(rec["s3"]["object"]["key"])
    except Exception:
        return None

    if key.startswith("finanzas/Balance_Presupuestario/raw/"):
        return "balance_single"
    if key.startswith("finanzas/Egresos_Detallado/raw/"):
        return "egresos_single"
    if key.startswith("finanzas/Ingresos_Detallado/raw/"):
        return "ingresos_single"
    return None


def _run(pipeline_func, pipeline_name: str):
    os.environ.setdefault("LOG_DIR", "/tmp/logs")
    os.makedirs(os.environ["LOG_DIR"], exist_ok=True)
    plog = PipelineLogging(pipeline_name=pipeline_name, log_folder_path=os.environ["LOG_DIR"])
    pipeline_func(pipeline_logging=plog)


def handler(event, context):
    # 1) Prefer S3 event-based routing if this was triggered by S3
    s3_target = _route_from_s3_event(event) if event else None

    # 2) Fallbacks: explicit payload {"pipeline":"..."} or env PIPELINE_TARGET
    target = s3_target or (event or {}).get("pipeline") or os.environ.get("PIPELINE_TARGET", "")

    routes = {
        "egresos_single": lambda: _run(egresos_single_pipeline, "egresos_detallado_pipeline"),
        "egresos_bulk":   lambda: _run(egresos_bulk_pipeline,   "egresos_detallado_bulk_pipeline"),
        "ingresos_single":(lambda: _run(ingresos_single_pipeline, "ingresos_detallado_pipeline")) if ingresos_single_pipeline else None,
        "ingresos_bulk":  (lambda: _run(ingresos_bulk_pipeline,   "ingresos_detallado_bulk_pipeline")) if ingresos_bulk_pipeline else None,
        "balance_single": (lambda: _run(balance_single_pipeline,  "balance_presupuestario_pipeline")) if balance_single_pipeline else None,
        "balance_bulk":   (lambda: _run(balance_bulk_pipeline,    "balance_presupuestario_bulk_pipeline")) if balance_bulk_pipeline else None,
    }

    if target not in routes or routes[target] is None:
        valid = [k for k, v in routes.items() if v is not None]
        raise ValueError(f"Unknown pipeline '{target}'. Valid: {valid}")

    routes[target]()
    return {"ok": True, "pipeline": target, "db_logging": False}
