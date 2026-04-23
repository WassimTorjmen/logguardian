import csv
import os
from datetime import datetime
from typing import Iterator
from .base import BaseParser, LogEntry


class HDFSParser(BaseParser):
    source_name = "hdfs"

    def parse(self, data_dir: str) -> Iterator[LogEntry]:
        base = os.path.join(data_dir, "HDFS TraceBench", "preprocessed")
        if not os.path.isdir(base):
            return

        for fname, is_failure in [("normal_trace.csv", False), ("failure_trace.csv", True)]:
            fpath = os.path.join(base, fname)
            if not os.path.isfile(fpath):
                continue
            try:
                with open(fpath, "r", errors="replace") as f:
                    reader = csv.reader(f)
                    headers = next(reader, None)
                    if headers is None:
                        continue
                    event_cols = headers[1:]
                    for row in reader:
                        if not row:
                            continue
                        task_id = row[0]
                        counts = row[1:]
                        non_zero = [
                            (event_cols[i], int(float(v)))
                            for i, v in enumerate(counts)
                            if i < len(event_cols) and v and float(v) > 0
                        ]
                        summary = "; ".join(f"{e}={c}" for e, c in non_zero[:5])
                        level = "ERROR" if is_failure else "INFO"
                        yield LogEntry(
                            timestamp=datetime.utcnow().isoformat(),
                            source="hdfs",
                            host="hdfs-cluster",
                            level=level,
                            component=f"task:{task_id}",
                            message=summary or "empty trace",
                            raw=",".join(row),
                        )
            except (OSError, StopIteration):
                continue
