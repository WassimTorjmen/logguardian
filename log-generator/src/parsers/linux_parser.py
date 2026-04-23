import re
from datetime import datetime
from typing import Iterator
from .base import BaseParser, LogEntry

# Jun  9 06:06:20 combo kernel: message
_PATTERN = re.compile(
    r"^(\w+\s+\d+\s+\d+:\d+:\d+)\s+(\S+)\s+(\S+?)(?:\[\d+\])?:\s+(.*)$"
)
_LEVEL_KEYWORDS = {
    "error": "ERROR", "fail": "ERROR", "critical": "FATAL",
    "warn": "WARN", "warning": "WARN",
    "debug": "DEBUG",
}


def _infer_level(message: str) -> str:
    low = message.lower()
    for kw, lvl in _LEVEL_KEYWORDS.items():
        if kw in low:
            return lvl
    return "INFO"


def _parse_ts(raw_ts: str) -> str:
    try:
        dt = datetime.strptime(raw_ts.strip(), "%b %d %H:%M:%S")
        return dt.replace(year=datetime.now().year).isoformat()
    except ValueError:
        return raw_ts.strip()


class LinuxParser(BaseParser):
    source_name = "linux"

    def parse(self, data_dir: str) -> Iterator[LogEntry]:
        log_path = f"{data_dir}/Linux.log"
        try:
            with open(log_path, "r", errors="replace") as f:
                for line in f:
                    line = line.rstrip()
                    if not line:
                        continue
                    m = _PATTERN.match(line)
                    if m:
                        ts, host, component, message = m.groups()
                        yield LogEntry(
                            timestamp=_parse_ts(ts),
                            source="linux",
                            host=host,
                            level=_infer_level(message),
                            component=component,
                            message=message,
                            raw=line,
                        )
        except FileNotFoundError:
            pass
