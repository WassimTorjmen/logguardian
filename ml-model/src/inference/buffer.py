"""
Buffer glissant par (source, host).

Accumule les logs au fil du stream Kafka.
Quand le buffer atteint seq_len → yield la séquence → glisse d'un cran.
"""
from collections import deque


class SlidingBuffer:
    def __init__(self, seq_len: int):
        self.seq_len = seq_len
        self._buffers: dict[tuple, deque] = {}

    def add(self, record: dict) -> list[dict] | None:
        """
        Ajoute un log au buffer de sa clé (source, host).
        Retourne la séquence complète si le buffer est plein, sinon None.
        """
        key = (record.get("source", ""), record.get("host", ""))
        if key not in self._buffers:
            self._buffers[key] = deque(maxlen=self.seq_len)

        self._buffers[key].append(record)

        if len(self._buffers[key]) == self.seq_len:
            return list(self._buffers[key])
        return None

    def __len__(self):
        return len(self._buffers)
