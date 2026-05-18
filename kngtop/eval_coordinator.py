"""WS-driven evaluation: dirty-flag + debounce; heartbeat when quiet."""

from __future__ import annotations

import threading
import time


class EvalCoordinator:
    """Wake main eval on any price feed update, with burst coalescing."""

    def __init__(self, *, debounce_sec: float, heartbeat_sec: float) -> None:
        self._lock = threading.Lock()
        self._dirty = False
        self._wake = threading.Event()
        self.debounce_sec = max(0.0, float(debounce_sec))
        self.heartbeat_sec = max(0.05, float(heartbeat_sec))

    def notify(self) -> None:
        with self._lock:
            self._dirty = True
        self._wake.set()

    def wait_for_turn(self) -> None:
        """Block until heartbeat or WS activity; debounce after WS bursts only."""
        signaled = self._wake.wait(timeout=self.heartbeat_sec)
        self._wake.clear()
        with self._lock:
            dirty = self._dirty
            self._dirty = False
        return
