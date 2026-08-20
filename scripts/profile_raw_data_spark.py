from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


RAW_DIR = Path("data_lake_raw")
REPORT_DIR = Path("docs/data_quality")
REPORT_FILE = REPORT_DIR / "raw_data_profile_spark.csv"


NULL_LIKE_VALUES = [
    "",
    "NULL",
    "None",
    "N/A",
    "nan",
    "NaN",
]


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("BpostRawDataProfiler")
        .master("local[*]")
        .getOrCreate()
    )


def profile_table(spark: SparkSession, file_path: Path) -> dict:
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .csv(str(file_path))
    )

    row_count = df.count()
    column_count = len(df.columns)

    # ---------------------------------------------------------
    # Null-like value detection
    # ---------------------------------------------------------
    null_expressions = [
        F.sum(
            F.when(
                F.col(column).isNull()
                | F.trim(F.col(column)).isin(NULL_LIKE_VALUES),
                1
            ).otherwise(0)
        ).alias(column)
        for column in df.columns
    ]

    null_counts_row = df.agg(*null_expressions).collect()[0]

    null_like_count = sum(
        int(value or 0)
        for value in null_counts_row
    )

    total_cells = row_count * column_count

    null_like_pct = (
        (null_like_count / total_cells) * 100
        if total_cells
        else 0
    )

    # ---------------------------------------------------------
    # Exact duplicate rows
    # ---------------------------------------------------------
    distinct_count = df.distinct().count()

    duplicate_count = row_count - distinct_count

    duplicate_pct = (
        (duplicate_count / row_count) * 100
        if row_count
        else 0
    )

    # ---------------------------------------------------------
    # Result
    # ---------------------------------------------------------
    return {
        "table_name": file_path.stem,
        "row_count": row_count,
        "column_count": column_count,
        "file_size_mb": round(
            file_path.stat().st_size / (1024 * 1024),
            3,
        ),
        "null_like_count": null_like_count,
        "null_like_pct": round(null_like_pct, 3),
        "duplicate_count": duplicate_count,
        "duplicate_pct": round(duplicate_pct, 3),
    }


def main() -> None:

    if not RAW_DIR.exists():
        raise FileNotFoundError(
            f"Raw data folder not found: {RAW_DIR.resolve()}"
        )

    csv_files = sorted(
        path
        for path in RAW_DIR.glob("*.csv")
        if path.name != "MANIFEST.csv"
    )

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV files found in {RAW_DIR.resolve()}"
        )

    print(f"Found {len(csv_files)} CSV files.\n")

    spark = create_spark_session()

    results = []

    try:

        for file_path in csv_files:

            print(
                f"Profiling: {file_path.name}"
            )

            result = profile_table(
                spark,
                file_path,
            )

            results.append(result)

        report_df = spark.createDataFrame(results)

        REPORT_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        # Spark writes a directory of part files.
        output_path = str(
            REPORT_DIR / "raw_data_profile_spark"
        )

        (
            report_df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(output_path)
        )

        print("\n" + "=" * 80)
        print("RAW DATA PROFILE - PYSPARK")
        print("=" * 80)

        (
            report_df
            .orderBy("table_name")
            .show(
                100,
                truncate=False,
            )
        )

        print(
            f"Spark report written to: "
            f"{REPORT_DIR.resolve()}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()