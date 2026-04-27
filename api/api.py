import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.pop("SPARK_HOME", None)
os.environ.pop("PYTHONPATH", None)
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum as spark_sum, when, round as spark_round
from consumer.metadata_store import query_by_run_id, query_by_delta_version

DELTA_RAW   = "./delta/wafer_raw"
DELTA_YIELD = "./delta/wafer_yield_summary"

spark = SparkSession.builder \
    .appName("YieldAPI") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

app = FastAPI(
    title="Yield Data Platform API",
    description="Query wafer yield summaries, defect analysis and lineage",
    version="1.0.0"
)

@app.get("/")
def root():
    return {"status": "ok", "message": "Yield Data Platform API is running"}

@app.get("/yield/{lot_id}")
def get_yield_by_lot(lot_id: str):
    try:
        df = spark.read.format("delta").load(DELTA_YIELD)
        lot_df = df.filter(col("lot_id") == lot_id)
        if lot_df.count() == 0:
            raise HTTPException(status_code=404, detail=f"lot_id '{lot_id}' not found")

        summary = lot_df.groupBy("lot_id", "wafer_id", "process_node") \
            .agg(
                spark_round(avg("yield_pct"), 2).alias("avg_yield_pct"),
                spark_sum("total_dies").alias("total_dies"),
                spark_sum("passed_dies").alias("passed_dies"),
                spark_sum("defect_count").alias("total_defects")
            ).collect()

        return {
            "lot_id": lot_id,
            "wafers": [
                {
                    "wafer_id":     r["wafer_id"],
                    "process_node": r["process_node"],
                    "avg_yield_pct": r["avg_yield_pct"],
                    "total_dies":   r["total_dies"],
                    "passed_dies":  r["passed_dies"],
                    "total_defects": r["total_defects"]
                } for r in summary
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/wafer/{wafer_id}/defects")
def get_defects_by_wafer(wafer_id: str):
    try:
        df = spark.read.format("delta").load(DELTA_RAW)
        wafer_df = df.filter(
            (col("wafer_id") == wafer_id) & col("defect_code").isNotNull()
        )
        if wafer_df.count() == 0:
            raise HTTPException(
                status_code=404,
                detail=f"No defects found for wafer '{wafer_id}'"
            )

        defects = wafer_df.groupBy("defect_code") \
            .agg(count("*").alias("count")) \
            .orderBy(col("count").desc()) \
            .collect()

        failed_dies = wafer_df.select("die_x", "die_y").collect()

        return {
            "wafer_id": wafer_id,
            "defect_summary": [
                {"defect_code": r["defect_code"], "count": r["count"]}
                for r in defects
            ],
            "failed_die_coordinates": [
                {"x": r["die_x"], "y": r["die_y"]}
                for r in failed_dies
            ]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/lineage/{run_id}")
def get_lineage(run_id: str):
    rows = query_by_run_id(run_id)
    if not rows:
        raise HTTPException(status_code=404, detail=f"run_id '{run_id}' not found")
    return {
        "run_id": run_id,
        "results": [
            {
                "run_id":        r[0],
                "git_sha":       r[1],
                "delta_version": r[2],
                "row_count":     r[3],
                "processed_at":  r[4]
            } for r in rows
        ]
    }

@app.get("/datasets/{delta_version}")
def get_runs_by_version(delta_version: int):
    rows = query_by_delta_version(delta_version)
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"delta_version '{delta_version}' not found"
        )
    return {
        "delta_version": delta_version,
        "results": [
            {
                "run_id":        r[0],
                "git_sha":       r[1],
                "delta_version": r[2],
                "row_count":     r[3],
                "processed_at":  r[4]
            } for r in rows
        ]
    }