import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.pop("SPARK_HOME", None)
os.environ.pop("PYTHONPATH", None)
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
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
@app.get("/wafer/{wafer_id}/heatmap", response_class=HTMLResponse)
def get_wafer_heatmap(wafer_id: str):
    try:
        from fastapi.responses import HTMLResponse
        df = spark.read.format("delta").load(DELTA_RAW)
        wafer_df = df.filter(col("wafer_id") == wafer_id)

        if wafer_df.count() == 0:
            raise HTTPException(
                status_code=404,
                detail=f"wafer '{wafer_id}' not found"
            )

        dies = wafer_df.select(
            "die_x", "die_y", "passed", "defect_code", "cell_type"
        ).collect()

        lot_id = wafer_df.select("lot_id").first()["lot_id"]
        total = len(dies)
        passed = sum(1 for d in dies if d["passed"])
        yield_pct = round(passed / total * 100, 1) if total > 0 else 0

        grid = {}
        for d in dies:
            grid[(d["die_x"], d["die_y"])] = {
                "passed":      d["passed"],
                "defect_code": d["defect_code"],
                "cell_type":   d["cell_type"]
            }

        DEFECT_COLORS = {
            "particle":     "#e67e22",
            "scratch":      "#9b59b6",
            "void":         "#e74c3c",
            "bridge":       "#c0392b",
            "open_circuit": "#8e44ad",
            None:           "#27ae60"
        }

        cells_html = ""
        for y in range(10):
            for x in range(10):
                die = grid.get((x, y), {})
                passed_die  = die.get("passed", False)
                defect      = die.get("defect_code")
                cell_type   = die.get("cell_type", "unknown")
                color = DEFECT_COLORS.get(defect, "#27ae60") if not passed_die else "#27ae60"
                label = defect if defect else "pass"
                cells_html += f"""
                <div class="die {'pass' if passed_die else 'fail'}"
                     style="background:{color}"
                     title="{cell_type} | {label} | ({x},{y})">
                    <span class="coord">{x},{y}</span>
                </div>"""

        legend_html = ""
        for defect, color in DEFECT_COLORS.items():
            label = defect if defect else "pass"
            legend_html += f"""
            <div class="legend-item">
                <div class="legend-color" style="background:{color}"></div>
                <span>{label}</span>
            </div>"""

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Wafer Heatmap — {wafer_id}</title>
    <style>
        body {{
            font-family: -apple-system, sans-serif;
            background: #0f1117;
            color: #e0e0e0;
            padding: 32px;
            margin: 0;
        }}
        h1 {{ font-size: 22px; font-weight: 500; margin-bottom: 4px; }}
        .meta {{ color: #888; font-size: 13px; margin-bottom: 24px; }}
        .stats {{
            display: flex;
            gap: 24px;
            margin-bottom: 28px;
        }}
        .stat {{
            background: #1e2130;
            border: 1px solid #2a2d3e;
            border-radius: 8px;
            padding: 14px 20px;
            min-width: 120px;
        }}
        .stat-value {{
            font-size: 28px;
            font-weight: 500;
            color: #fff;
        }}
        .stat-label {{
            font-size: 12px;
            color: #888;
            margin-top: 2px;
        }}
        .yield-value {{
            color: {'#27ae60' if yield_pct >= 80 else '#e67e22' if yield_pct >= 60 else '#e74c3c'};
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(10, 52px);
            grid-template-rows: repeat(10, 52px);
            gap: 3px;
            margin-bottom: 28px;
        }}
        .die {{
            border-radius: 4px;
            display: flex;
            align-items: flex-end;
            justify-content: flex-start;
            padding: 3px;
            cursor: pointer;
            transition: opacity 0.15s;
            position: relative;
        }}
        .die:hover {{ opacity: 0.75; }}
        .coord {{
            font-size: 9px;
            color: rgba(255,255,255,0.6);
        }}
        .legend {{
            display: flex;
            flex-wrap: wrap;
            gap: 16px;
            margin-top: 8px;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 13px;
        }}
        .legend-color {{
            width: 16px;
            height: 16px;
            border-radius: 3px;
        }}
        h2 {{ font-size: 14px; font-weight: 500; margin-bottom: 12px; color: #aaa; }}
    </style>
</head>
<body>
    <h1>Wafer Heatmap</h1>
    <div class="meta">{lot_id} / {wafer_id} — hover over any die for details</div>

    <div class="stats">
        <div class="stat">
            <div class="stat-value yield-value">{yield_pct}%</div>
            <div class="stat-label">Yield</div>
        </div>
        <div class="stat">
            <div class="stat-value">{passed}</div>
            <div class="stat-label">Passed dies</div>
        </div>
        <div class="stat">
            <div class="stat-value">{total - passed}</div>
            <div class="stat-label">Failed dies</div>
        </div>
        <div class="stat">
            <div class="stat-value">{total}</div>
            <div class="stat-label">Total dies</div>
        </div>
    </div>

    <div class="grid">
        {cells_html}
    </div>

    <h2>Legend</h2>
    <div class="legend">
        {legend_html}
    </div>
</body>
</html>"""
        return HTMLResponse(content=html)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))