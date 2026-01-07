"""
스키마 드리프트 알림 모듈

드리프트 감지 시 로깅 및 Airflow Variable 기록을 담당
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from jobs.silver.schema_drift.detector import AggregatedDriftResult


def send_drift_alert(result: AggregatedDriftResult) -> None:
    """
    드리프트 감지 결과 알림 전송

    현재 구현:
    - 로깅 (WARNING 레벨)
    - Airflow Variable 기록 (가능한 경우)

    향후 확장 가능:
    - Slack webhook
    - Email 알림
    """
    logger = logging.getLogger(__name__)

    if not result.drift_detected:
        logger.info(
            "[Schema Drift] 드리프트 없음 (date=%s, max_failure=%.2f%%)",
            result.target_date,
            result.max_failure_rate * 100,
        )
        return

    # 상세 로그 출력
    logger.warning(
        "[Schema Drift] ⚠️ 드리프트 감지! date=%s, new_variants=%d, max_failure=%.2f%%",
        result.target_date,
        result.total_new_variants,
        result.max_failure_rate * 100,
    )

    for track_result in result.track_results:
        if track_result.drift_detected:
            logger.warning(
                "  - %s: failures=%d/%.2f%%, new_variants=%s, reasons=%s",
                track_result.track_name,
                track_result.parse_failure_count,
                track_result.parse_failure_rate * 100,
                track_result.new_variants[:3] if track_result.new_variants else [],
                track_result.drift_reasons,
            )

    # Airflow Variable 기록 (Airflow 환경에서만)
    _record_to_airflow_variable(result)


def _record_to_airflow_variable(result: AggregatedDriftResult) -> None:
    """Airflow Variable에 드리프트 이력 기록"""
    try:
        from airflow.models import Variable

        key = "schema_drift_history"
        history = json.loads(Variable.get(key, default_var="[]"))

        # 새 이벤트 추가
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "target_date": result.target_date,
            "drift_detected": result.drift_detected,
            "total_new_variants": result.total_new_variants,
            "max_failure_rate": round(result.max_failure_rate, 4),
            "affected_tracks": [
                r.track_name for r in result.track_results if r.drift_detected
            ],
        }
        history.append(event)

        # 최근 100개만 유지
        if len(history) > 100:
            history = history[-100:]

        Variable.set(key, json.dumps(history, ensure_ascii=False))
        logging.getLogger(__name__).info("Airflow Variable에 드리프트 이력 기록 완료")

    except ImportError:
        logging.getLogger(__name__).debug("Airflow 환경이 아님, Variable 기록 스킵")
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).warning("Airflow Variable 기록 실패: %s", exc)


def format_drift_summary(result: AggregatedDriftResult) -> str:
    """드리프트 결과 요약 문자열 생성"""
    if not result.drift_detected:
        return f"✅ 스키마 드리프트 없음 (date={result.target_date})"

    affected = [r.track_name for r in result.track_results if r.drift_detected]
    return (
        f"⚠️ 스키마 드리프트 감지!\n"
        f"  - 날짜: {result.target_date}\n"
        f"  - 신규 variant: {result.total_new_variants}개\n"
        f"  - 최대 파싱 실패율: {result.max_failure_rate:.2%}\n"
        f"  - 영향 트랙: {', '.join(affected)}"
    )
