import subprocess
import time


def drop_os_caches(drop_cache_command: str | None) -> dict:
    """尝试执行外部 cache drop 命令。"""
    if not drop_cache_command:
        return {
            "cache_drop_supported": False,
            "cache_drop_success": False,
            "cache_drop_error": "drop cache command not provided",
            "cache_drop_time_ms": 0.0,
        }

    started_at = time.perf_counter()
    try:
        subprocess.run(drop_cache_command, shell=True, check=True)
    except subprocess.CalledProcessError as exc:
        return {
            "cache_drop_supported": True,
            "cache_drop_success": False,
            "cache_drop_error": str(exc),
            "cache_drop_time_ms": (time.perf_counter() - started_at) * 1000.0,
        }
    except Exception as exc:  # pragma: no cover - defensive branch
        return {
            "cache_drop_supported": True,
            "cache_drop_success": False,
            "cache_drop_error": str(exc),
            "cache_drop_time_ms": (time.perf_counter() - started_at) * 1000.0,
        }

    return {
        "cache_drop_supported": True,
        "cache_drop_success": True,
        "cache_drop_error": "",
        "cache_drop_time_ms": (time.perf_counter() - started_at) * 1000.0,
    }