"""
Variant 레지스트리 모듈

알려진 payload_variant 해시를 S3에 저장/로드하여
신규 스키마 버전 탐지에 사용합니다.

저장 위치: s3a://{bucket}/metadata/variant_registry.json
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass
class VariantRegistryData:
    """레지스트리 데이터 구조"""

    variants: set[str] = field(default_factory=set)
    last_updated: str = ""
    version: int = 1

    def to_dict(self) -> dict:
        return {
            "variants": sorted(self.variants),
            "last_updated": self.last_updated,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, data: dict) -> VariantRegistryData:
        return cls(
            variants=set(data.get("variants", [])),
            last_updated=data.get("last_updated", ""),
            version=data.get("version", 1),
        )


class VariantRegistry:
    """S3 기반 variant 해시 레지스트리"""

    def __init__(
        self,
        spark: SparkSession,
        bucket: str = "gharchive",
        registry_path: str = "metadata/variant_registry.json",
    ) -> None:
        self.spark = spark
        self.bucket = bucket
        self.registry_path = registry_path
        self._logger = logging.getLogger(__name__)

    @property
    def full_path(self) -> str:
        return f"s3a://{self.bucket}/{self.registry_path}"

    def _get_hadoop_fs(self):
        """Hadoop FileSystem 객체 획득"""
        jvm = self.spark._jvm  # pyright: ignore[reportAttributeAccessIssue]
        jsc = self.spark.sparkContext._jsc  # pyright: ignore[reportAttributeAccessIssue]
        hconf = jsc.hadoopConfiguration()
        jpath = jvm.org.apache.hadoop.fs.Path(self.full_path)
        return jpath.getFileSystem(hconf), jpath

    def load(self) -> VariantRegistryData:
        """레지스트리 로드 (없으면 빈 레지스트리 반환)"""
        try:
            fs, jpath = self._get_hadoop_fs()
            if not fs.exists(jpath):
                self._logger.info("레지스트리 파일 없음, 빈 레지스트리 반환")
                return VariantRegistryData()

            # Hadoop InputStream으로 읽기
            input_stream = fs.open(jpath)
            reader = self.spark._jvm.java.io.BufferedReader(  # pyright: ignore
                self.spark._jvm.java.io.InputStreamReader(  # pyright: ignore
                    input_stream, "UTF-8"
                )
            )

            lines = []
            line = reader.readLine()
            while line is not None:
                lines.append(line)
                line = reader.readLine()
            reader.close()

            content = "\n".join(lines)
            data = json.loads(content)
            registry = VariantRegistryData.from_dict(data)
            self._logger.info(
                "레지스트리 로드 완료: %d variants", len(registry.variants)
            )
            return registry

        except Exception as exc:  # noqa: BLE001
            self._logger.warning("레지스트리 로드 실패: %s", exc)
            return VariantRegistryData()

    def save(self, registry: VariantRegistryData) -> bool:
        """레지스트리 저장"""
        try:
            from datetime import datetime, timezone

            registry.last_updated = datetime.now(timezone.utc).isoformat()
            registry.version += 1

            fs, jpath = self._get_hadoop_fs()
            output_stream = fs.create(jpath, True)  # overwrite=True

            content = json.dumps(registry.to_dict(), indent=2, ensure_ascii=False)
            output_stream.write(content.encode("utf-8"))
            output_stream.close()

            self._logger.info(
                "레지스트리 저장 완료: %d variants, version=%d",
                len(registry.variants),
                registry.version,
            )
            return True

        except Exception as exc:  # noqa: BLE001
            self._logger.error("레지스트리 저장 실패: %s", exc)
            return False

    def register_new_variants(self, new_variants: list[str]) -> bool:
        """신규 variant 등록"""
        if not new_variants:
            return True

        registry = self.load()
        registry.variants.update(new_variants)
        return self.save(registry)

    def get_known_variants(self) -> set[str]:
        """알려진 variant 세트 반환"""
        return self.load().variants
