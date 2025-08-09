import os
import re
import boto3
from dotenv import load_dotenv
from sqlalchemy import MetaData

from app.etl_central.assets.egresos_detallado import (
    get_egresos_detallado_table,
    generate_surrogate_key,
    extract_egresos_detallado_data,
    transform_egresos_detallado_data,
)
from app.etl_central.assets.pipeline_logging import PipelineLogging
from app.etl_central.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from app.etl_central.connectors.postgresql import PostgreSqlClient


def find_latest_egresos_file(bucket_name: str) -> tuple[int | None, str | None]:
    """
    Find the most recent 'Egresos Detallado' file in S3.
    Returns (year, quarter) like (2025, "Q3"), or (None, None) if not found.
    """
    prefix = "finanzas/Egresos_Detallado/raw/"
    s3 = boto3.client("s3")
    pattern = r"F6_a_EAPED_Clas_Obj_Gas_LDF_([1-4]T)(\d{4})\.xlsx"
    quarter_map = {"1T": "Q1", "2T": "Q2", "3T": "Q3", "4T": "Q4"}

    latest_year = -1
    latest_quarter = -1
    latest_q = None
    latest_y = None

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            match = re.match(pattern, fname)
            if match:
                quarter_num = int(match.group(1)[0])  # "3T" -> 3
                year = int(match.group(2))
                if (year > latest_year) or (year == latest_year and quarter_num > latest_quarter):
                    latest_year = year
                    latest_quarter = quarter_num
                    latest_q = quarter_map[match.group(1)]
                    latest_y = year

    return (latest_y, latest_q) if latest_q and latest_y else (None, None)


def pipeline(pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("100 | Starting ETL pipeline for Egresos Detallado")

    # Env/config
    SERVER_NAME = os.getenv("SERVER_NAME")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    PORT = int(os.getenv("PORT", "5432"))
    BUCKET_NAME = os.getenv("BUCKET_NAME", "centralfiles3")

    # 1) Discover latest file
    year, quarter = find_latest_egresos_file(bucket_name=BUCKET_NAME)
    if not year or not quarter:
        raise FileNotFoundError("No valid Egresos Detallado file found in S3.")
    pipeline_logging.logger.info(f"110 | Latest file detected: year={year}, quarter={quarter}")

    # 2) Extract
    pipeline_logging.logger.info("200 | Extracting data from S3")
    extracted_df, file_path = extract_egresos_detallado_data(
        year=year,
        quarter=quarter,
        source="s3",
        bucket_name=BUCKET_NAME,
    )
    if extracted_df.empty:
        raise ValueError(f"400 | File {file_path} is empty or could not be read.")
    pipeline_logging.logger.info(f"210 | Extracted rows: {extracted_df.shape[0]} from {file_path}")

    # 3) Transform (egresos logic)
    pipeline_logging.logger.info("300 | Transforming data")
    transformed_df = transform_egresos_detallado_data(extracted_df, file_path)
    transformed_df = generate_surrogate_key(transformed_df)
    pipeline_logging.logger.info(f"310 | Transformed rows: {transformed_df.shape[0]}")

    # 4) Load (UPSERT)
    pipeline_logging.logger.info("400 | Preparing DB objects")
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    metadata = MetaData()
    table = get_egresos_detallado_table(metadata)

    pipeline_logging.logger.info("410 | Loading data into PostgreSQL (upsert)")
    postgresql_client.upsert(
        data=transformed_df.to_dict(orient="records"),
        table=table,
        metadata=metadata,
    )
    pipeline_logging.logger.info("499 | Load completed successfully")


def run_egresos_pipeline(pipeline_name: str, log_client: PostgreSqlClient):
    """
    Wrapper that:
      - sets up file+stdout logging
      - writes start/success/failure to metadata table
      - runs the pipeline()
    """
    log_dir = os.getenv("LOG_DIR", "./logs")
    pipeline_logging = PipelineLogging(pipeline_name=pipeline_name, log_folder_path=log_dir)

    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name,
        postgresql_client=log_client,
        config={},  # include run params here if needed
    )

    try:
        metadata_logger.log()  # start
        pipeline(pipeline_logging=pipeline_logging)
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS,
            logs=pipeline_logging.get_logs(),
        )
        pipeline_logging.logger.handlers.clear()
    except Exception as e:
        pipeline_logging.logger.error(f"500 | Pipeline run failed: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE,
            logs=pipeline_logging.get_logs(),
        )
        pipeline_logging.logger.handlers.clear()
        raise


if __name__ == "__main__":
    load_dotenv()

    # Logging DB (metadata)
    LOGGING_SERVER_NAME = os.getenv("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.getenv("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.getenv("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.getenv("LOGGING_PASSWORD")
    LOGGING_PORT = int(os.getenv("LOGGING_PORT", "5432"))

    log_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )

    PIPELINE_NAME = "egresos_detallado_pipeline"
    run_egresos_pipeline(
        pipeline_name=PIPELINE_NAME,
        log_client=log_client
        )
