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
        # Cherche Android.log ou le premier .log dans le dossier Android/
        candidates = [
            os.path.join(data_dir, "Android.log"),
            os.path.join(data_dir, "Android", "Android.log"),
        ]
        # Cherche aussi tous les .log dans Android/
        android_dir = os.path.join(data_dir, "Android")
        if os.path.isdir(android_dir):
            for fname in sorted(os.listdir(android_dir)):
                if fname.endswith(".log"):
                    candidates.append(os.path.join(android_dir, fname))

        for log_path in candidates:
            if not os.path.isfile(log_path):
                continue
            try:
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
                                host="android-device",
                                level=_LEVEL_MAP.get(level_char, "INFO"),
                                component=component,
                                message=message,
                                raw=line,
                            )
                return  # premier fichier trouvé suffit
            except FileNotFoundError:
                continue
