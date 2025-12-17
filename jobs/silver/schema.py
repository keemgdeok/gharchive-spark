"""
Silver 변환에 필요한 입력 스키마 정의
- Challenge #3: 중첩/드리프트 대응을 위한 Superset 스키마
"""

from __future__ import annotations

from pyspark.sql import types as T

BRONZE_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), True),
        T.StructField("type", T.StringType(), True),
        T.StructField(
            "actor",
            T.StructType(
                [
                    T.StructField("id", T.LongType(), True),
                    T.StructField("login", T.StringType(), True),
                    T.StructField("display_login", T.StringType(), True),
                    T.StructField("url", T.StringType(), True),
                ]
            ),
            True,
        ),
        T.StructField(
            "repo",
            T.StructType(
                [
                    T.StructField("id", T.LongType(), True),
                    T.StructField("name", T.StringType(), True),
                    T.StructField("url", T.StringType(), True),
                ]
            ),
            True,
        ),
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField("push_id", T.LongType(), True),
                    T.StructField("size", T.IntegerType(), True),
                    T.StructField("distinct_size", T.IntegerType(), True),
                    T.StructField(
                        "commits",
                        T.ArrayType(
                            T.StructType(
                                [
                                    T.StructField("sha", T.StringType(), True),
                                    T.StructField(
                                        "author",
                                        T.StructType(
                                            [
                                                T.StructField(
                                                    "email", T.StringType(), True
                                                ),
                                                T.StructField(
                                                    "name", T.StringType(), True
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    T.StructField("message", T.StringType(), True),
                                    T.StructField("distinct", T.BooleanType(), True),
                                    T.StructField("url", T.StringType(), True),
                                ]
                            ),
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        T.StructField("public", T.BooleanType(), True),
        T.StructField("created_at", T.StringType(), True),
        T.StructField("_corrupt_record", T.StringType(), True),
    ]
)
