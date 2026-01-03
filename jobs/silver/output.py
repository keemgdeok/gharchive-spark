from __future__ import annotations

import datetime as dt
import logging

from pyspark.sql import functions as F

from jobs.spark_fs import list_files_under, path_exists


def write_silver(
    *,
    df,
    output_path: str,
    verbose: bool,
    coalesce: int = 0,
    repartition: int = 0,
) -> None:
    logging.info("write silver=%s", output_path)
    logging.info("write partitions=%d", df.rdd.getNumPartitions())
    # 작은 파일 완화를 위한 파티션 조정
    if repartition and repartition > 0:
        df = df.repartition(repartition)
        logging.info("repartition 적용 partitions=%d", df.rdd.getNumPartitions())
    elif coalesce and coalesce > 0:
        df = df.coalesce(coalesce)
        logging.info("coalesce 적용 partitions=%d", df.rdd.getNumPartitions())
    if verbose:
        df.explain(True)
    df.write.mode("overwrite").partitionBy("dt").parquet(output_path)


def post_checks(
    *,
    spark,
    output_path: str,
    target_date: dt.date,
    verbose: bool,
    label: str,
    preview_cols: list[str] | None = None,
) -> None:
    if not path_exists(spark, output_path):
        raise RuntimeError(f"Silver 출력 경로가 생성되지 않았습니다: {output_path}")

    dt_partition = f"{output_path}/dt={target_date:%Y-%m-%d}"
    if path_exists(spark, dt_partition):
        files = list_files_under(spark, dt_partition)
        total_bytes = sum(size for _, size in files)
        logging.info(
            "silver(%s) 파일 개수=%d 총크기=%d bytes (dt=%s)",
            label,
            len(files),
            total_bytes,
            target_date,
        )
    else:
        logging.warning(
            "silver(%s) dt 파티션이 없습니다(데이터가 없을 수 있음): %s",
            label,
            dt_partition,
        )

    if verbose and preview_cols:
        preview = (
            spark.read.parquet(output_path)
            .filter(F.col("dt") == F.lit(target_date))
            .select(*preview_cols)
            .limit(20)
        )
        logging.info("silver(%s) 미리보기(최대 20건)", label)
        preview.show(truncate=False)


def log_type_counts(*, base_df) -> None:
    logging.info("events_base 타입별 건수(상위 20)")
    (
        base_df.groupBy("event_type")
        .count()
        .orderBy(F.desc("count"))
        .show(20, truncate=False)
    )


def log_track_stats(*, df, label: str, verbose: bool) -> None:
    if not verbose:
        return
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(F.col("payload_parse_ok"), 1).otherwise(0)).alias("payload_ok"),
    ).collect()[0]
    logging.info(
        "track(%s) rows=%d payload_ok=%d",
        label,
        int(row["rows"]),
        int(row["payload_ok"] or 0),
    )
