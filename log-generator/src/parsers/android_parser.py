import re
import os
from datetime import datetime
from typing import Iterator
from .base import BaseParser, LogEntry

# Format LogHub Android :
# 01-01 00:00:00.000  1234  5678 I ActivityManager: message
_PATTERN = re.compile(
    r"^(\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)\s+\d+\s+\d+\s+([VDIWEF])\s+(\S+?)\s*:\s+(.*)$"
)

_LEVEL_MAP = {
    "V": "DEBUG",
    "D": "DEBUG",
    "I": "INFO",
    "W": "WARN",
    "E": "ERROR",
    "F": "FATAL",
}


def _parse_ts(raw_ts: str) -> str:
    try:
        dt = datetime.strptime(raw_ts.strip(), "%m-%d %H:%M:%S.%f")
        return dt.replace(year=datetime.now().year).isoformat()
    except ValueError:
        return raw_ts.strip()


class AndroidParser(BaseParser):
    source_name = "android"

    def parse(self, data_dir: str) -> Iterator[LogEntry]:
        android_dir = os.path.join(data_dir, "Android")
        candidates = [os.path.join(data_dir, "Android.log")]

        # Cherche récursivement tous les .log dans Android/ et ses sous-dossiers
        if os.path.isdir(android_dir):
            for root, _, files in os.walk(android_dir):
                for fname in sorted(files):
                    if fname.endswith(".log"):
                        candidates.append(os.path.join(root, fname))

        for log_path in candidates:
            if not os.path.isfile(log_path):
                continue
            issue = os.path.basename(os.path.dirname(log_path))
            host = f"android-{issue}" if issue.startswith("issue") else "android-device"
            with open(log_path, "r", errors="replace") as f:
                for line in f:
                    line = line.rstrip()
                    if not line:
                        continue
                    m = _PATTERN.match(line)
                    if m:
                        ts, level_char, component, message = m.groups()
                        yield LogEntry(
                            timestamp=_parse_ts(ts),
                            source="android",
                            host=host,
                            level=_LEVEL_MAP.get(level_char, "INFO"),
                            component=component,
                            message=message,
                            raw=line,
                        )
