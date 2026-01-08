"""
실버 -> 골드 집계 파이프라인

- CLI: spark-submit jobs/gold/base.py --date 2024-01-01
- @task.pyspark: process(spark, target_date) 호출
"""

from __future__ import annotations

import datetime as dt
import logging

from pyspark.sql import SparkSession

from jobs.gold.agg.actor import build_actor_marts
from jobs.gold.agg.daily import build_daily_top_repos
from jobs.gold.agg.pr import build_pr_review_latency
from jobs.gold.agg.push import build_push_branch_ratio
from jobs.gold.agg.repo import build_repo_marts
from jobs.gold.cli import configure_logging, parse_args, resolve_date_range
from jobs.gold.io import (
    build_gold_path,
    build_silver_path,
    read_events_base,
    write_gold,
)
from jobs.spark_runtime import get_spark


def _log_runtime_context(
    spark: SparkSession,
    base_path: str,
    start_date: dt.date,
    end_date: dt.date,
) -> None:
    """런타임 컨텍스트 로깅"""
    logging.info("silver base=%s", base_path)
    logging.info("date range=%s ~ %s", start_date, end_date)
    logging.info(
        "AQE=%s shuffle.partitions=%s",
        spark.conf.get("spark.sql.adaptive.enabled", "unknown"),
        spark.conf.get("spark.sql.shuffle.partitions", "unknown"),
    )


def process(
    spark: SparkSession,
    target_date: str | dt.date,
    *,
    bucket: str = "gharchive",
    silver_prefix: str = "silver",
    gold_prefix: str = "gold",
    top_n: int = 10,
    top_event_types: int = 20,
    daily_top_n: int = 10,
    top_actors: int = 10,
    actor_metric: str = "event_count",
    output_format: str = "parquet",
    coalesce: int = 1,
    explain: bool = False,
    verbose: bool = False,
) -> None:
    """
    Gold 레이어 처리 (core logic)

    Args:
        spark: SparkSession (외부 주입 또는 내부 생성)
        target_date: 처리 대상 날짜 (YYYY-MM-DD 문자열 또는 date 객체)
        bucket: S3 버킷 이름
        silver_prefix: Silver 경로 prefix
        gold_prefix: Gold 경로 prefix
        top_n: Top repos 개수
        top_event_types: 상위 이벤트 타입 개수
        daily_top_n: 일별 Top repos 개수 (0이면 비활성화)
        top_actors: Top actors 개수
        actor_metric: actor 집계 지표 (event_count/distinct_repo)
        output_format: 출력 포맷 (parquet/csv)
        coalesce: 출력 파일 개수
        explain: 물리 실행 계획 출력
        verbose: 상세 로그 출력
    """
    # 문자열 -> date 변환
    if isinstance(target_date, str):
        target_date = dt.datetime.strptime(target_date, "%Y-%m-%d").date()

    start_date = target_date
    end_date = target_date

    base_path = build_silver_path(bucket, silver_prefix, "events_base")
    _log_runtime_context(spark, base_path, start_date, end_date)

    events_df = read_events_base(
        spark=spark, base_path=base_path, start_date=start_date, end_date=end_date
    )

    # 간단한 args-like 객체 생성
    class Args:
        pass

    args = Args()
    args.bucket = bucket  # type: ignore[attr-defined]
    args.silver_prefix = silver_prefix  # type: ignore[attr-defined]
    args.gold_prefix = gold_prefix  # type: ignore[attr-defined]
    args.top_n = top_n  # type: ignore[attr-defined]
    args.top_event_types = top_event_types  # type: ignore[attr-defined]
    args.daily_top_n = daily_top_n  # type: ignore[attr-defined]
    args.top_actors = top_actors  # type: ignore[attr-defined]
    args.actor_metric = actor_metric  # type: ignore[attr-defined]
    args.output_format = output_format  # type: ignore[attr-defined]
    args.coalesce = coalesce  # type: ignore[attr-defined]
    args.explain = explain  # type: ignore[attr-defined]
    args.verbose = verbose  # type: ignore[attr-defined]

    events_df, repo_marts = build_repo_marts(
        events_df=events_df,
        args=args,
        start_date=start_date,
        end_date=end_date,
        explain=explain,
    )
    actor_marts = build_actor_marts(
        events_df=events_df,
        args=args,
        start_date=start_date,
        end_date=end_date,
        explain=explain,
    )
    daily_top_repos = build_daily_top_repos(
        events_df=events_df,
        args=args,
        start_date=start_date,
        end_date=end_date,
        explain=explain,
    )
    push_branch_ratio = build_push_branch_ratio(
        spark=spark,
        args=args,
        start_date=start_date,
        end_date=end_date,
        explain=explain,
    )
    pr_review_latency = build_pr_review_latency(
        spark=spark,
        args=args,
        start_date=start_date,
        end_date=end_date,
        explain=explain,
    )

    # 결과 저장
    write_gold(
        df=repo_marts["top_repos"],
        output_path=build_gold_path(bucket, gold_prefix, "top_repos"),
        fmt=output_format,
        coalesce=coalesce,
        partition_cols=None,
    )
    write_gold(
        df=repo_marts["event_type_counts"],
        output_path=build_gold_path(bucket, gold_prefix, "event_type_counts"),
        fmt=output_format,
        coalesce=coalesce,
        partition_cols=None,
    )
    write_gold(
        df=repo_marts["top_repo_event_types"],
        output_path=build_gold_path(bucket, gold_prefix, "top_repo_event_types"),
        fmt=output_format,
        coalesce=coalesce,
        partition_cols=None,
    )
    write_gold(
        df=actor_marts["top_actors"],
        output_path=build_gold_path(bucket, gold_prefix, "top_actors"),
        fmt=output_format,
        coalesce=coalesce,
        partition_cols=None,
    )
    write_gold(
        df=actor_marts["top_actor_event_types"],
        output_path=build_gold_path(bucket, gold_prefix, "top_actor_event_types"),
        fmt=output_format,
        coalesce=coalesce,
        partition_cols=None,
    )
    if daily_top_repos is not None:
        write_gold(
            df=daily_top_repos,
            output_path=build_gold_path(bucket, gold_prefix, "daily_top_repos"),
            fmt=output_format,
            coalesce=coalesce,
            partition_cols=["dt"],
        )
    write_gold(
        df=push_branch_ratio,
        output_path=build_gold_path(bucket, gold_prefix, "push_branch_ratio"),
        fmt=output_format,
        coalesce=coalesce,
        partition_cols=["dt"],
    )
    write_gold(
        df=pr_review_latency,
        output_path=build_gold_path(bucket, gold_prefix, "pull_request_review_latency"),
        fmt=output_format,
        coalesce=coalesce,
        partition_cols=["dt"],
    )

    logging.info("Gold 집계 완료: %s", target_date)


def main() -> None:
    """CLI 엔트리포인트"""
    args = parse_args()
    configure_logging(args.verbose)

    spark = get_spark("gh-archive-gold")
    try:
        start_date, end_date = resolve_date_range(args)

        # CLI에서는 날짜 범위 지원, @task.pyspark에서는 단일 날짜만
        # 여기서는 기존 로직 유지
        base_path = build_silver_path(args.bucket, args.silver_prefix, "events_base")
        _log_runtime_context(spark, base_path, start_date, end_date)

        events_df = read_events_base(
            spark=spark, base_path=base_path, start_date=start_date, end_date=end_date
        )

        events_df, repo_marts = build_repo_marts(
            events_df=events_df,
            args=args,
            start_date=start_date,
            end_date=end_date,
            explain=args.explain,
        )
        actor_marts = build_actor_marts(
            events_df=events_df,
            args=args,
            start_date=start_date,
            end_date=end_date,
            explain=args.explain,
        )
        daily_top_repos = build_daily_top_repos(
            events_df=events_df,
            args=args,
            start_date=start_date,
            end_date=end_date,
            explain=args.explain,
        )
        push_branch_ratio = build_push_branch_ratio(
            spark=spark,
            args=args,
            start_date=start_date,
            end_date=end_date,
            explain=args.explain,
        )
        pr_review_latency = build_pr_review_latency(
            spark=spark,
            args=args,
            start_date=start_date,
            end_date=end_date,
            explain=args.explain,
        )

        write_gold(
            df=repo_marts["top_repos"],
            output_path=build_gold_path(args.bucket, args.gold_prefix, "top_repos"),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        write_gold(
            df=repo_marts["event_type_counts"],
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "event_type_counts"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        write_gold(
            df=repo_marts["top_repo_event_types"],
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "top_repo_event_types"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        write_gold(
            df=actor_marts["top_actors"],
            output_path=build_gold_path(args.bucket, args.gold_prefix, "top_actors"),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        write_gold(
            df=actor_marts["top_actor_event_types"],
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "top_actor_event_types"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        if daily_top_repos is not None:
            write_gold(
                df=daily_top_repos,
                output_path=build_gold_path(
                    args.bucket, args.gold_prefix, "daily_top_repos"
                ),
                fmt=args.output_format,
                coalesce=args.coalesce,
                partition_cols=["dt"],
            )
        write_gold(
            df=push_branch_ratio,
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "push_branch_ratio"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=["dt"],
        )
        write_gold(
            df=pr_review_latency,
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "pull_request_review_latency"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=["dt"],
        )

        logging.info("Gold 집계 완료")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
