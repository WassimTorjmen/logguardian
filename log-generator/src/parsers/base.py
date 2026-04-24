from abc import ABC, abstractmethod
from typing import Iterator
import json


class LogEntry:
    __slots__ = ("timestamp", "source", "host", "level", "component", "message", "raw")

    def __init__(self, timestamp, source, host, level, component, message, raw):
        self.timestamp = timestamp
        self.source = source
        self.host = host
        self.level = level
        self.component = component
        self.message = message
        self.raw = raw

    def to_json(self) -> str:
        return json.dumps({
            "timestamp": self.timestamp,
            "source": self.source,
            "host": self.host,
            "level": self.level,
            "component": self.component,
            "message": self.message,
            "raw": self.raw,
        })


class BaseParser(ABC):
    source_name: str = ""

    @abstractmethod
    def parse(self, data_dir: str) -> Iterator[LogEntry]:
        pass
