"""
스키마 드리프트 감지 모듈

Silver 레이어의 payload 파싱 실패율 및 신규 variant 해시를 분석하여
스키마 드리프트를 감지합니다.

주요 기능:
- payload_parse_ok 기반 파싱 실패율 계산
- payload_variant 해시 기반 신규 스키마 탐지
- 종합 드리프트 판정 (임계값 기반)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# 기본 임계값 설정
DEFAULT_FAILURE_RATE_THRESHOLD = 0.05  # 5% 이상 파싱 실패 시 드리프트 경고
DEFAULT_NEW_VARIANT_THRESHOLD = 1  # 1개 이상 신규 variant 발견 시 드리프트 경고


@dataclass
class SchemaDriftResult:
    """스키마 드리프트 감지 결과 DTO"""

    target_date: str
    track_name: str
    total_records: int = 0
    parse_failure_count: int = 0
    parse_failure_rate: float = 0.0
    new_variants: list[str] = field(default_factory=list)
    known_variants: list[str] = field(default_factory=list)
    drift_detected: bool = False
    drift_reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "target_date": self.target_date,
            "track_name": self.track_name,
            "total_records": self.total_records,
            "parse_failure_count": self.parse_failure_count,
            "parse_failure_rate": round(self.parse_failure_rate, 4),
            "new_variants": self.new_variants,
            "known_variants": self.known_variants,
            "drift_detected": self.drift_detected,
            "drift_reasons": self.drift_reasons,
        }


@dataclass
class AggregatedDriftResult:
    """전체 트랙 드리프트 결과 집계"""

    target_date: str
    track_results: list[SchemaDriftResult] = field(default_factory=list)
    drift_detected: bool = False
    total_new_variants: int = 0
    max_failure_rate: float = 0.0

    def to_dict(self) -> dict:
        return {
            "target_date": self.target_date,
            "drift_detected": self.drift_detected,
            "total_new_variants": self.total_new_variants,
            "max_failure_rate": round(self.max_failure_rate, 4),
            "track_results": [r.to_dict() for r in self.track_results],
        }


class SchemaDriftDetector:
    """Silver 레이어 스키마 드리프트 감지기"""

    # 검사 대상 트랙 목록 (payload_parse_ok/payload_variant 컬럼이 있는 트랙)
    TRACKS_TO_CHECK = [
        "events_push",
        "events_pull_request",
        "events_issues",
        "events_issue_comment",
        "events_pull_request_review",
        "events_create",
        "events_fork",
        "events_watch",
    ]

    def __init__(
        self,
        spark: SparkSession,
        target_date: str,
        bucket: str = "gharchive",
        silver_prefix: str = "silver",
        failure_rate_threshold: float = DEFAULT_FAILURE_RATE_THRESHOLD,
        new_variant_threshold: int = DEFAULT_NEW_VARIANT_THRESHOLD,
    ) -> None:
        self.spark = spark
        self.target_date = target_date
        self.bucket = bucket
        self.silver_prefix = silver_prefix
        self.failure_rate_threshold = failure_rate_threshold
        self.new_variant_threshold = new_variant_threshold
        self._logger = logging.getLogger(__name__)

    def _build_silver_path(self, track_name: str) -> str:
        return f"s3a://{self.bucket}/{self.silver_prefix}/{track_name}"

    def check_track(
        self,
        track_name: str,
        known_variants: set[str],
    ) -> SchemaDriftResult:
        """단일 트랙에 대한 드리프트 검사 수행"""
        result = SchemaDriftResult(
            target_date=self.target_date,
            track_name=track_name,
        )

        silver_path = self._build_silver_path(track_name)

        try:
            df = self.spark.read.parquet(silver_path).filter(
                F.col("dt") == F.lit(self.target_date)
            )

            # 기본 통계 수집
            stats = df.agg(
                F.count(F.lit(1)).alias("total"),
                F.sum(F.when(~F.col("payload_parse_ok"), 1).otherwise(0)).alias(
                    "failures"
                ),
            ).collect()[0]

            result.total_records = int(stats["total"])
            result.parse_failure_count = int(stats["failures"] or 0)

            if result.total_records > 0:
                result.parse_failure_rate = (
                    result.parse_failure_count / result.total_records
                )

            # variant 해시 분포 수집
            variant_rows = (
                df.filter(F.col("payload_variant").isNotNull())
                .filter(F.col("payload_variant") != F.lit("parse_fail"))
                .groupBy("payload_variant")
                .count()
                .collect()
            )

            current_variants = {row["payload_variant"] for row in variant_rows}
            result.known_variants = list(current_variants & known_variants)
            result.new_variants = list(current_variants - known_variants)

            # 드리프트 판정
            if result.parse_failure_rate >= self.failure_rate_threshold:
                result.drift_detected = True
                result.drift_reasons.append(
                    f"파싱 실패율 {result.parse_failure_rate:.2%} >= "
                    f"임계값 {self.failure_rate_threshold:.2%}"
                )

            if len(result.new_variants) >= self.new_variant_threshold:
                result.drift_detected = True
                result.drift_reasons.append(
                    f"신규 variant {len(result.new_variants)}개 발견: "
                    f"{result.new_variants[:3]}..."
                )

        except Exception as exc:  # noqa: BLE001
            self._logger.warning(
                "트랙 %s 검사 실패 (데이터 없음 가능): %s", track_name, exc
            )
            result.drift_reasons.append(f"검사 실패: {exc}")

        return result

    def run_all_checks(
        self,
        known_variants: set[str] | None = None,
    ) -> AggregatedDriftResult:
        """모든 트랙에 대한 드리프트 검사 수행"""
        if known_variants is None:
            known_variants = set()

        agg_result = AggregatedDriftResult(target_date=self.target_date)

        for track_name in self.TRACKS_TO_CHECK:
            self._logger.info("트랙 검사 시작: %s", track_name)
            track_result = self.check_track(track_name, known_variants)
            agg_result.track_results.append(track_result)

            if track_result.drift_detected:
                agg_result.drift_detected = True

            agg_result.total_new_variants += len(track_result.new_variants)
            agg_result.max_failure_rate = max(
                agg_result.max_failure_rate, track_result.parse_failure_rate
            )

        self._logger.info(
            "드리프트 검사 완료: detected=%s, new_variants=%d, max_failure=%.2f%%",
            agg_result.drift_detected,
            agg_result.total_new_variants,
            agg_result.max_failure_rate * 100,
        )

        return agg_result
