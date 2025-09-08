import duckdb
from prefect import flow, task, get_run_logger
import uuid
from pydantic_settings import BaseSettings
from pydantic import ConfigDict
import os 
import typer

def new_id():
    return str(uuid.uuid4())

class Settings(BaseSettings):
    input_file: str
    output_dir: str
    db_path: str = "pipeline.duckdb"

@task(retries=3, retry_delay_seconds=10)
def init_tables(db_path: str, schema_file: str = "table_schemas.sql"):
    with open(schema_file, "r") as f:
        schema_sql = f.read()
    with duckdb.connect(db_path) as con:
        con.execute(schema_sql)

@task(retries=3, retry_delay_seconds=10)
def extract(csv_path: str, pipeline_id: str , db_path: str, extract_id: str | None = None) -> str:
    extract_id = extract_id or new_id()
    logger = get_run_logger()

    logger.info(f"[{pipeline_id} | {extract_id}] Extracting {csv_path} into db")
    with duckdb.connect(db_path) as con:
        con.execute(
            """
            INSERT INTO sales
            SELECT 
                "Order ID"::TEXT,
                TRY_CAST("Order Date" AS DATE),
                TRY_CAST("Ship Date" AS DATE),
                "Item Type" AS product_type,
                Country,
                Region,
                "Sales Channel",
                "Order Priority",
                "Units Sold",
                "Unit Price",
                ? AS pipeline_id,
                ? AS extract_id,
                now() AS ts
            FROM read_csv_auto(?, header=True)
            """,
            [pipeline_id, extract_id, csv_path],  # safe binding
        )

    return extract_id

@task(retries=3, retry_delay_seconds=10)
def transform(db_path: str, pipeline_id: str, extract_id: str, transform_id: str | None = None) -> str:
    transform_id = transform_id or new_id()
    logger = get_run_logger()
    logger.info(f"[{pipeline_id} | {transform_id}] Transforming data for extract {extract_id}")

    with duckdb.connect(db_path) as con:
        con.execute(
            """
            INSERT INTO top10
            SELECT 
                product_type,
                SUM(units_sold * unit_price) AS total_revenue,
                ? AS pipeline_id,
                ? AS transform_id,
                ? AS source_extract_id,
                now() AS ts
            FROM sales
            WHERE extract_id = ?
            GROUP BY product_type
            ORDER BY total_revenue DESC
            LIMIT 10
            """,
            [pipeline_id, transform_id, extract_id, extract_id],
        )

    return transform_id

@task(retries=3, retry_delay_seconds=10)
def load(db_path: str, out_dir: str, pipeline_id: str, transform_id: str, load_id: str | None = None) -> str:
    load_id = load_id or new_id()

    logger = get_run_logger()
    logger.info(f"[{pipeline_id} | {load_id}] Exporting results for transform {transform_id}")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"top10_{pipeline_id}_{transform_id}.parquet")

    if os.path.exists(out_path):
        logger.warning(f"[{pipeline_id} | {load_id}] Output file already exists → {out_path} (will be overwritten)")

    with duckdb.connect(db_path) as con:
        con.execute(
            f"""
            COPY (
                SELECT * FROM top10 WHERE transform_id = ?
            ) TO '{out_path}' (FORMAT PARQUET)
            """,
            [transform_id],
        )

    logger.info(f"[{pipeline_id} | {load_id}] Load complete → {out_path}")
    return out_path

@flow
def etl_pipeline(csv_path: str, out_dir: str, db_path: str = "pipeline.duckdb", pipeline_id: str | None = None, extract_id: str | None = None, transform_id: str | None = None, load_id: str | None = None):
    init_tables(db_path=db_path)
    pipeline_id = pipeline_id or new_id()
    extract_id = extract(csv_path=csv_path, pipeline_id=pipeline_id, db_path=db_path, extract_id=extract_id)
    transform_id = transform(pipeline_id=pipeline_id, extract_id=extract_id, db_path=db_path, transform_id=transform_id)
    loaded_file_path = load(pipeline_id=pipeline_id, transform_id=transform_id, out_dir=out_dir, db_path=db_path, load_id=load_id)
    return loaded_file_path

def main(
    input_file: str = typer.Argument(..., help="Path to input CSV file"),
    output_dir: str = typer.Argument(..., help="Directory to store outputs")
):
    global settings
    settings = Settings(input_file=input_file, output_dir=output_dir)
    etl_pipeline(settings.input_file, settings.output_dir)


if __name__ == "__main__":
    typer.run(main)
