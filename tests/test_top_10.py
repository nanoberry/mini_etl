import pandas as pd
import pytest
from pathlib import Path
from datetime import datetime, timedelta
from src.top_10 import etl_pipeline 


@pytest.fixture
def sample_sales_csv(tmp_path: Path):
    base_date = datetime(2023, 1, 1)
    data = {
        "Order ID": [f"O{i}" for i in range(1, 21)],
        "Order Date": [base_date + timedelta(days=i) for i in range(20)],
        "Ship Date": [base_date + timedelta(days=i+2) for i in range(20)],
        "Item Type": [f"Type_{i % 5}" for i in range(20)],  # 5 categories
        "Country": ["USA" if i % 2 == 0 else "Canada" for i in range(20)],
        "Region": [f"Region_{i % 3}" for i in range(20)],
        "Sales Channel": ["Online" if i % 2 == 0 else "Offline" for i in range(20)],
        "Order Priority": ["H" if i % 4 == 0 else "M" for i in range(20)],
        "Units Sold": [10 + i for i in range(20)],
        "Unit Price": [100 + (i % 5) * 10 for i in range(20)],
        "pipeline_id": ["test-pipeline"] * 20,
        "extract_id": ["test-extract"] * 20,
        "ts": [base_date + timedelta(hours=i) for i in range(20)],
    }
    df = pd.DataFrame(data)
    csv_path = tmp_path / "sales.csv"
    df.to_csv(csv_path, index=False)
    return csv_path, df


def test_etl_pipeline_top10(tmp_path: Path, sample_sales_csv):
    csv_path, input_df = sample_sales_csv
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    db_path = tmp_path / "test.duckdb"

    etl_pipeline(
        csv_path=str(csv_path),
        out_dir=str(out_dir),
        db_path=str(db_path),
    )

    parquet_files = list(out_dir.glob("*.parquet"))
    assert len(parquet_files) == 1, "Expected one parquet file"
    output_df = pd.read_parquet(parquet_files[0])
    output_slim = (
    output_df[["product_type", "total_revenue"]]
    .rename(columns={"product_type": "Item Type"})
    .reset_index(drop=True)
)


    input_df["total_revenue"] = input_df["Units Sold"] * input_df["Unit Price"]
    expected = (
        input_df.groupby("Item Type")["total_revenue"]
        .sum()
        .reset_index()
        .sort_values("total_revenue", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(
        output_slim.reset_index(drop=True),
        expected,
        check_dtype=False,
    )
