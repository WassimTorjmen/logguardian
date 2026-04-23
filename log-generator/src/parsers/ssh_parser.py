import re
from datetime import datetime
from typing import Iterator
from .base import BaseParser, LogEntry

# Dec 10 06:55:46 LabSZ sshd[24200]: message
_PATTERN = re.compile(
    r"^(\w+\s+\d+\s+\d+:\d+:\d+)\s+(\S+)\s+sshd\[(\d+)\]:\s+(.*)$"
)
_LEVEL_MAP = {
    "invalid user": "WARN", "failed password": "WARN", "break-in attempt": "ERROR",
    "error": "ERROR", "fatal": "FATAL", "disconnect": "INFO",
    "accepted": "INFO", "session opened": "INFO", "session closed": "INFO",
}


def _infer_level(message: str) -> str:
    low = message.lower()
    for kw, lvl in _LEVEL_MAP.items():
        if kw in low:
            return lvl
    return "INFO"


def _parse_ts(raw_ts: str) -> str:
    try:
        dt = datetime.strptime(raw_ts.strip(), "%b %d %H:%M:%S")
        return dt.replace(year=datetime.now().year).isoformat()
    except ValueError:
        return raw_ts.strip()


class SSHParser(BaseParser):
    source_name = "ssh"

    def parse(self, data_dir: str) -> Iterator[LogEntry]:
        log_path = f"{data_dir}/SSH.log"
        try:
            with open(log_path, "r", errors="replace") as f:
                for line in f:
                    line = line.rstrip()
                    if not line:
                        continue
                    m = _PATTERN.match(line)
                    if m:
                        ts, host, pid, message = m.groups()
                        yield LogEntry(
                            timestamp=_parse_ts(ts),
                            source="ssh",
                            host=host,
                            level=_infer_level(message),
                            component=f"sshd[{pid}]",
                            message=message,
                            raw=line,
                        )
        except FileNotFoundError:
            pass
