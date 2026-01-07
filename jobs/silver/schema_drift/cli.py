from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone

from jobs.spark_runtime import get_spark
from jobs.silver.schema_drift.detector import SchemaDriftDetector
from jobs.silver.schema_drift.registry import VariantRegistry


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")
    if verbose:
        logging.getLogger("py4j").setLevel(logging.WARN)
        logging.getLogger("pyspark").setLevel(logging.WARN)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver 레이어 스키마 드리프트 감지")
    parser.add_argument(
        "--date",
        required=True,
        help="대상 날짜 (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--bucket",
        default="gharchive",
        help="S3 버킷 이름",
    )
    parser.add_argument(
        "--failure-threshold",
        type=float,
        default=0.05,
        help="파싱 실패율 임계값 (기본: 0.05 = 5%%)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="상세 로그 출력",
    )
    return parser.parse_args()


def write_result_to_s3(spark, bucket: str, target_date: str, result: dict) -> str:
    """결과를 S3에 JSON으로 저장"""
    output_path = f"s3a://{bucket}/metadata/schema_drift_results/{target_date}.json"

    # Hadoop FileSystem API로 직접 쓰기
    jvm = spark._jvm  # pyright: ignore[reportAttributeAccessIssue]
    jsc = spark.sparkContext._jsc  # pyright: ignore[reportAttributeAccessIssue]
    hconf = jsc.hadoopConfiguration()
    jpath = jvm.org.apache.hadoop.fs.Path(output_path)
    fs = jpath.getFileSystem(hconf)

    # JSON 직렬화
    result["written_at"] = datetime.now(timezone.utc).isoformat()
    content = json.dumps(result, indent=2, ensure_ascii=False)

    # 쓰기
    output_stream = fs.create(jpath, True)  # overwrite=True
    output_stream.write(content.encode("utf-8"))
    output_stream.close()

    logging.info("결과 저장 완료: %s", output_path)
    return output_path


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)

    logging.info("스키마 드리프트 감지 시작: date=%s", args.date)

    spark = get_spark("schema-drift-cli")
    try:
        # 기존 variant 레지스트리 로드
        registry = VariantRegistry(spark, bucket=args.bucket)
        known_variants = registry.get_known_variants()
        logging.info("알려진 variant 수: %d", len(known_variants))

        # 드리프트 감지 실행
        detector = SchemaDriftDetector(
            spark=spark,
            target_date=args.date,
            bucket=args.bucket,
            failure_rate_threshold=args.failure_threshold,
        )
        result = detector.run_all_checks(known_variants=known_variants)

        # 신규 variant 등록
        if result.total_new_variants > 0:
            all_new = []
            for track_result in result.track_results:
                all_new.extend(track_result.new_variants)
            registry.register_new_variants(all_new)
            logging.info("신규 variant %d개 등록 완료", len(all_new))

        # 결과를 S3에 저장
        result_dict = result.to_dict()
        write_result_to_s3(spark, args.bucket, args.date, result_dict)

        # 요약 로그
        if result.drift_detected:
            logging.warning(
                "⚠️ 드리프트 감지! new_variants=%d, max_failure=%.2f%%",
                result.total_new_variants,
                result.max_failure_rate * 100,
            )
        else:
            logging.info(
                "✅ 드리프트 없음 (max_failure=%.2f%%)",
                result.max_failure_rate * 100,
            )

        logging.info("스키마 드리프트 감지 완료")

    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logging.exception("스키마 드리프트 감지 실패: %s", exc)
        sys.exit(1)
