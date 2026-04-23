import os
import re
from typing import Iterator
from .base import BaseParser, LogEntry

# Same format as Hadoop YARN logs
_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+)\s+(INFO|WARN|ERROR|FATAL|DEBUG)\s+"
    r"\[([^\]]+)\]\s+(\S+):\s+(.*)$"
)


def _parse_ts(raw: str) -> str:
    return raw.replace(",", ".").replace(" ", "T")


class SparkParser(BaseParser):
    source_name = "spark"

    def parse(self, data_dir: str) -> Iterator[LogEntry]:
        base = os.path.join(data_dir, "spark")
        if not os.path.isdir(base):
            return
        for fname in os.listdir(base):
            fpath = os.path.join(base, fname)
            if not os.path.isfile(fpath):
                continue
            try:
                with open(fpath, "r", errors="replace") as f:
                    for line in f:
                        line = line.rstrip()
                        if not line:
                            continue
                        m = _PATTERN.match(line)
                        if m:
                            ts, level, thread, cls, message = m.groups()
                            yield LogEntry(
                                timestamp=_parse_ts(ts),
                                source="spark",
                                host=fname,
                                level=level,
                                component=cls,
                                message=message,
                                raw=line,
                            )
            except (OSError, UnicodeDecodeError):
                continue
