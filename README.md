# Mini ETL Pipeline – Sales Data

## Task Overview
This project implements a mini-ETL pipeline that ingests, transforms, and stores sales data.  

## To Run Project
For easy set up the project uses Docker to run.
```bash
# Build the Docker image
docker compose build

# Run the ETL pipeline
docker compose run --rm etl <path-to-input-csv> <output-directory>

#to run the project with the sample input and store the results in a directory named output:
docker compose run --rm etl data/sales-orders.csv  output 
```


## Requirements Implemented
1. **Ingest** raw sales data from a CSV file.  
2. **Transform** the data by computing the **top 10 product types by total revenue**.  
   - Revenue = `units_sold × unit_price` aggregated by `product_type`.  
3. **Load** the transformed results into a **Parquet file** for efficient columnar storage.  

---

## Design Overview

- **Language**: Python 3.10+  
- **Task Orchestration**: [Prefect](https://docs.prefect.io/)  
  - Each ETL phase (`extract → transform → load`) is implemented as a Prefect task.  
  - Tasks are chained into a single `@flow` function. Prefect was chosen since it is a simple orchestration tool with very little setup. Airflow, for example, definitely has more features and has prebuilt hooks for aws and other popular tools, however for this local set up it wasn't really necessary. In a production environment, Airflow should be considered as needs grow and the etl becomes a little more complex 
- **Database**: [DuckDB](https://duckdb.org/) for more details on why this further below. *  
- **Storage**: Output written to Parquet.  
- **Logging**: Prefect’s built-in structured logging.  
- **Containerization**: Docker for reproducible environment.  
-**Package Manager**: uv, it handles packages really nicely in python. 
- **Config**: CLI arguments via [Typer](https://typer.tiangolo.com/) and environment variables via Pydantic `BaseSettings`.  

---
*
doing the transform part could've been done in pandas or in any sql db.
Since I was strictly requested for an  ETL ("Each task should trigger the next one (E → T → L).")
this means that If I had wanted to do the transform in Pandas, I would have to load the whole CSV into memory.
If I wanted to only load chunk of the csv into memory with Pandas I would have to combine the E and T part.

I decided to go down the db path. then the E part moves the data into the db, the T is done in sql and then it is loaded into a parquet file. It is worth noticing that since this project is being set up and run purely locally, the db still implies loading everything into memory :). However, theoretically speaking if I had a proper db with its own engine, the db solution would be more scalable. If the file is too big it can be saved in batches and then once it is in the db, the transform part becomes easy. 

## **Extract**
the posts are extracted into duckdb. I chose duckdb since it is a simple inmemory db and has an easy setup. again since any db I would set up would essentially remain in memory, it does not particularly matter which one was chosen. In a production environment I would probably pick Clickhouse since it is a fast analytical db. For the purpose of the task the input is expected to be a file path, meaning the file is supposed to exist locally. However in a real production environment the file could be located in S3 for example.

## **Transform**
Now that we have all the data located within the db we just get the top 10 by querying it and save it into another table.

## **Load**
Load is simple, we just dump the data into a parquet. In this case it will be saved locally with the pipeline_id and the transform_id 


