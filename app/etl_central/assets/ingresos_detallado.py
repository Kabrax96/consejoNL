import os
import re
import pandas as pd
import boto3
import base64
import uuid
from io import BytesIO
from sqlalchemy import MetaData, Table, Column, String, Float, Integer, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.etl_central.connectors.postgresql import PostgreSqlClient
import logging


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")



#-------------------------------------------------------------
#--------------------------EXTRACT----------------------------
#-------------------------------------------------------------

def extract_ingresos_detallado_data(
    year: int,
    quarter: str,
    source: str = "s3",
    bucket_name: str = None
) -> tuple[pd.DataFrame, str | None]:
    reverse_quarter_map = {"Q1": "1T", "Q2": "2T", "Q3": "3T", "Q4": "4T"}
    file_quarter = reverse_quarter_map.get(quarter, quarter)
    file_name = f"F5_Edo_Ana_Ing_Det_LDF_{file_quarter}{year}.xlsx"

    if source == "s3":
        if bucket_name is None:
            raise ValueError("bucket_name is required for S3 extraction")

        s3_key = f"finanzas/Ingresos_Detallado/raw/{file_name}"
        try:
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
            df = pd.read_excel(BytesIO(obj["Body"].read()), sheet_name="F5 EAI", header=None)
            return df, f"s3://{bucket_name}/{s3_key}"
        except Exception as e:
            logging.error(f"Failed to read file from S3: s3://{bucket_name}/{s3_key}. Error: {e}")
            return pd.DataFrame(), None

    else:
        raise ValueError("Invalid source. Use 's3'")

#-------------------------------------------------------------
#--------------------------TRANSFORM--------------------------
#-------------------------------------------------------------

def generate_truly_unique_key():
    uuid_part = uuid.uuid4().bytes
    entropy = os.urandom(16)
    raw = uuid_part + entropy
    return base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')


def generate_surrogate_key(df: pd.DataFrame) -> pd.DataFrame:
    df["surrogate_key"] = [generate_truly_unique_key() for _ in range(len(df))]
    return df


# =========================
# Definición de Tabla
# =========================

def get_ingresos_detallado_table(metadata: MetaData) -> Table:
    return Table(
        "nuevo_leon_ingresos_detallado",
        metadata,
        Column("surrogate_key", String, primary_key=True),
        Column("concepto", String, nullable=True),
        Column("estimado", String, nullable=True),
        Column("ampliaciones_reducciones", String, nullable=True),
        Column("modificado", String, nullable=True),
        Column("devengado", String, nullable=True),
        Column("recaudado", String, nullable=True),
        Column("diferencia", String, nullable=True),
        Column("clave_primaria", String, nullable=True),
        Column("clave_secundaria", String, nullable=True),
        Column("fecha", String, nullable=True),
        Column("cuarto", String, nullable=True),
        Column("seccion", String, nullable=True),
    )


# =========================
# Transformación de Datos
# =========================

def transform_ingresos_detallado_data(df: pd.DataFrame, file_path: str = None) -> pd.DataFrame:
    fecha, cuarto = extract_fecha_y_cuarto(df)

    columnas_base = [
        'concepto',
        'estimado',
        'ampliaciones_reducciones',
        'modificado',
        'devengado',
        'recaudado',
        'diferencia'
    ]

    # Sección I
    data_I = df.iloc[7:44, 1:8].values
    tbl_I = pd.DataFrame(data_I, columns=columnas_base)
    df_I = procesar_tabla_ingresos(tbl_I, fecha, cuarto, seccion="I")

    # Sección II
    data_II = df.iloc[45:76, 1:8].values
    tbl_II = pd.DataFrame(data_II, columns=columnas_base)
    df_II = procesar_tabla_ingresos(tbl_II, fecha, cuarto, seccion="II")

    return pd.concat([df_I, df_II], ignore_index=True)


# =========================
# Funciones auxiliares
# =========================

def extract_fecha_y_cuarto(df: pd.DataFrame):
    date_cells = df.iloc[3, 1:8].astype(str).str.strip()
    date_range_str = ' '.join(date_cells)

    month_map = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
        'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
        'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }

    m = re.search(r'al (\d{1,2}) de (\w+) de (\d{4})', date_range_str)
    if m:
        day = int(m.group(1))
        month = month_map.get(m.group(2).lower(), 0)
        year = int(m.group(3))
        fecha = f"{year}-{month:02d}-{day:02d}"
        cuarto = f"{year}_Q{(month - 1) // 3 + 1}"
        return fecha, cuarto
    return "", ""


def procesar_tabla_ingresos(df_tabla: pd.DataFrame, fecha: str, cuarto: str, seccion: str) -> pd.DataFrame:
    df_tabla = df_tabla.copy()

    prim_pat = r'^([A-Z]\.)'
    sec_pat = r'^([a-z]\d+\))'

    df_tabla['clave_primaria'] = df_tabla['concepto'].astype(str).str.extract(prim_pat)[0]
    df_tabla['clave_secundaria'] = df_tabla['concepto'].astype(str).str.extract(sec_pat)[0]
    df_tabla['fecha'] = fecha
    df_tabla['cuarto'] = cuarto
    df_tabla['seccion'] = seccion
    return df_tabla


def find_all_ingresos_files(bucket_name="centralfiles3", prefix="finanzas/Ingresos_Detallado/raw/"):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pattern = r"F5_Edo_Ana_Ing_Det_LDF_([1-4]T)(\d{4})\.xlsx"

    file_keys = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            filename = obj["Key"].split("/")[-1]
            match = re.match(pattern, filename)
            if match:
                quarter_map = {"1T": "Q1", "2T": "Q2", "3T": "Q3", "4T": "Q4"}
                quarter = quarter_map[match.group(1)]
                year = int(match.group(2))
                file_keys.append((year, quarter))

    return sorted(file_keys)

#-------------------------------------------------------------
#----------------------------LOAD-----------------------------
#-------------------------------------------------------------


def single_load(df: pd.DataFrame, postgresql_client, table: Table, metadata: MetaData) -> None:
    try:
        metadata.create_all(postgresql_client.engine)
        with postgresql_client.engine.connect() as conn:
            insert_stmt = pg_insert(table).values(df.to_dict(orient="records"))
            update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["surrogate_key"],
                set_={col.name: insert_stmt.excluded[col.name] for col in table.columns if col.name != "id"}
            )
            conn.execute(update_stmt)
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Single load (upsert) failed: {e}")

def bulk_load(df: pd.DataFrame, postgresql_client, table: Table, metadata: MetaData) -> None:
    try:
        metadata.create_all(postgresql_client.engine)
        with postgresql_client.engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table.name};"))
            conn.execute(table.insert(), df.to_dict(orient="records"))
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Bulk load failed: {e}")

def load(df: pd.DataFrame, postgresql_client, table: Table, metadata: MetaData, load_method: str = "upsert") -> None:
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise ValueError("Invalid load method: choose from [insert, upsert, overwrite]")
