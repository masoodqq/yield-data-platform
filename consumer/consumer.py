import os
import sys

os.environ.pop("SPARK_HOME", None)
os.environ.pop("PYTHONPATH", None)
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

VENV_PYSPARK = os.path.join(
    os.path.dirname(sys.executable), "..", "lib",
    "python{}.{}".format(*sys.version_info[:2]),
    "site-packages"
)
sys.path.insert(0, VENV_PYSPARK)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp,
    avg, count, sum as spark_sum,
    when, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType, IntegerType, MapType
)
from metadata_store import init_db, insert_lineage

KAFKA_BROKER   = "localhost:9092"
KAFKA_TOPIC    = "wafer-test-results"
DELTA_RAW      = "./delta/wafer_raw"
DELTA_YIELD    = "./delta/wafer_yield_summary"
CHECKPOINT_RAW = "./delta/checkpoints/wafer_raw"

for path in [DELTA_RAW, DELTA_YIELD, CHECKPOINT_RAW]:
    os.makedirs(path, exist_ok=True)

spark = SparkSession.builder \
    .appName("YieldDataPlatform") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
init_db()

schema = StructType([
    StructField("lot_id",       StringType(),  True),
    StructField("wafer_id",     StringType(),  True),
    StructField("process_node", StringType(),  True),
    StructField("die_x",        IntegerType(), True),
    StructField("die_y",        IntegerType(), True),
    StructField("cell_type",    StringType(),  True),
    StructField("passed",       BooleanType(), True),
    StructField("defect_code",  StringType(),  True),
    StructField("test_results", MapType(StringType(), DoubleType()), True),
    StructField("tested_at",    StringType(),  True),
    StructField("run_id",       StringType(),  True),
    StructField("git_sha",      StringType(),  True),
    StructField("ingested_at",  StringType(),  True),
])

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

parsed = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

def process_batch(batch_df, batch_id):
    count_records = batch_df.count()
    if count_records == 0:
        return

    print(f"\n--- Batch {batch_id} | {count_records} die records ---")

    batch_df.write \
        .format("delta") \
        .mode("append") \
        .save(DELTA_RAW)

    yield_summary = batch_df.groupBy("lot_id", "wafer_id", "process_node", "cell_type") \
        .agg(
            count("*").alias("total_dies"),
            spark_sum(when(col("passed"), 1).otherwise(0)).alias("passed_dies"),
            spark_round(
                spark_sum(when(col("passed"), 1).otherwise(0)) * 100.0 / count("*"), 2
            ).alias("yield_pct"),
            count(when(col("defect_code").isNotNull(), True)).alias("defect_count"),
            current_timestamp().alias("summarized_at")
        )

    yield_summary.write \
        .format("delta") \
        .mode("append") \
        .save(DELTA_YIELD)

    yield_summary.select(
        "lot_id", "wafer_id", "yield_pct", "total_dies", "defect_count"
    ).show(truncate=False)

    run_ids  = [r.run_id  for r in batch_df.select("run_id").distinct().collect()]
    git_shas = [r.git_sha for r in batch_df.select("git_sha").distinct().collect()]
    git_sha  = git_shas[0] if git_shas else "unknown"
    processed_at = str(batch_df.select(current_timestamp()).first()[0])

    from delta import DeltaTable
    delta_version = DeltaTable.forPath(spark, DELTA_RAW) \
        .history(1).collect()[0]["version"]

    for run_id in run_ids:
        insert_lineage(
            batch_id=str(batch_id),
            run_id=run_id,
            git_sha=git_sha,
            row_count=count_records,
            delta_version=delta_version,
            processed_at=processed_at
        )
        print(f"  run_id={run_id} → delta_version={delta_version} git_sha={git_sha}")

query = parsed.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", CHECKPOINT_RAW) \
    .trigger(processingTime="15 seconds") \
    .start()

print("Yield consumer started. Listening every 15 seconds. Ctrl+C to stop.")
query.awaitTermination()