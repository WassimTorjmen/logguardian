import os
import re
from typing import Iterator
from .base import BaseParser, LogEntry

# 2015-10-17 15:37:56,547 INFO [thread] org.apache.hadoop.Class: message
_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+)\s+(INFO|WARN|ERROR|FATAL|DEBUG)\s+"
    r"\[([^\]]+)\]\s+(\S+):\s+(.*)$"
)


def _parse_ts(raw: str) -> str:
    return raw.replace(",", ".").replace(" ", "T")


def _walk_logs(base_dir: str) -> Iterator[str]:
    for root, _, files in os.walk(base_dir):
        for fname in files:
            if fname.endswith(".log"):
                yield os.path.join(root, fname)


class HadoopParser(BaseParser):
    source_name = "hadoop"

    def parse(self, data_dir: str) -> Iterator[LogEntry]:
        base = os.path.join(data_dir, "hadoop")
        if not os.path.isdir(base):
            return
        for log_file in _walk_logs(base):
            app_id = os.path.basename(os.path.dirname(log_file))
            try:
                with open(log_file, "r", errors="replace") as f:
                    for line in f:
                        line = line.rstrip()
                        if not line:
                            continue
                        m = _PATTERN.match(line)
                        if m:
                            ts, level, thread, cls, message = m.groups()
                            yield LogEntry(
                                timestamp=_parse_ts(ts),
                                source="hadoop",
                                host=app_id,
                                level=level,
                                component=cls,
                                message=message,
                                raw=line,
                            )
            except (OSError, UnicodeDecodeError):
                continue
