import re
from typing import Iterator
from .base import BaseParser, LogEntry

# - 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.50.363779 R02-M1-N0-C:J12-U11 RAS KERNEL INFO message
_PATTERN = re.compile(
    r"^-\s+\d+\s+\S+\s+\S+\s+(\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d+)\s+"
    r"(\S+)\s+\S+\s+\S+\s+(INFO|WARN|WARNING|ERROR|FATAL|SEVERE)\s+(.*)$",
    re.IGNORECASE,
)
_LEVEL_NORM = {"WARNING": "WARN", "SEVERE": "ERROR"}


def _parse_ts(raw: str) -> str:
    # 2005-06-03-15.42.50.363779 -> 2005-06-03T15:42:50.363779
    parts = raw.split("-", 3)
    if len(parts) == 4:
        time_part = parts[3].replace(".", ":", 2)
        return f"{parts[0]}-{parts[1]}-{parts[2]}T{time_part}"
    return raw


class SupercomputerParser(BaseParser):
    source_name = "supercomputer"

    def parse(self, data_dir: str) -> Iterator[LogEntry]:
        import os
        base = os.path.join(data_dir, "supercomputer")
        if not os.path.isdir(base):
            return
        log_file = os.path.join(base, "BGL.log")
        if not os.path.isfile(log_file):
            return
        try:
            with open(log_file, "r", errors="replace") as f:
                for line in f:
                    line = line.rstrip()
                    if not line:
                        continue
                    m = _PATTERN.match(line)
                    if m:
                        ts, node, level, message = m.groups()
                        level = _LEVEL_NORM.get(level.upper(), level.upper())
                        yield LogEntry(
                            timestamp=_parse_ts(ts),
                            source="supercomputer",
                            host=node,
                            level=level,
                            component="RAS_KERNEL",
                            message=message,
                            raw=line,
                        )
        except (OSError, UnicodeDecodeError):
            pass
