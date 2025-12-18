"""
GHArchive HTTP 다운로드 유틸
- 재시도/타임아웃/스트리밍 다운로드를 담당
"""

from __future__ import annotations

import asyncio
import io

import aiohttp

DEFAULT_CHUNK_SIZE_BYTES = 1024 * 1024


async def download_bytes_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    *,
    attempts: int,
    timeout: aiohttp.ClientTimeout,
    chunk_size_bytes: int = DEFAULT_CHUNK_SIZE_BYTES,
) -> bytes:
    # tenacity 없이 최소 재시도(지수 백오프)만 구현
    delay_sec = 1.0
    last_error: Exception | None = None

    for attempt_no in range(1, max(1, attempts) + 1):
        try:
            buf = io.BytesIO()
            async with session.get(url, timeout=timeout) as resp:
                if resp.status == 404:
                    raise FileNotFoundError(f"404 Not Found: {url}")
                resp.raise_for_status()
                async for chunk in resp.content.iter_chunked(chunk_size_bytes):
                    buf.write(chunk)
            return buf.getvalue()
        except FileNotFoundError:
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_error = exc
            if attempt_no >= attempts:
                raise
            await asyncio.sleep(min(30.0, delay_sec))
            delay_sec = min(30.0, delay_sec * 2.0)

    if last_error is not None:
        raise last_error
    return b""
