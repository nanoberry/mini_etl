CREATE TABLE IF NOT EXISTS sales (
    order_id TEXT,
    order_date DATE,
    ship_date DATE,
    product_type TEXT,
    country TEXT,
    region TEXT,
    sales_channel TEXT,
    order_priority TEXT,
    units_sold DOUBLE,
    unit_price DOUBLE,
    pipeline_id TEXT,
    extract_id TEXT,
    ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS top10 (
    product_type TEXT,
    total_revenue DOUBLE,
    pipeline_id TEXT,
    transform_id TEXT,
    source_extract_id TEXT,
    ts TIMESTAMP
);
