from threading import Lock

class LamportClock:
    def __init__(self) -> None:
        self._value = 0
        self._lock = Lock()

    def tick(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def on_send(self) -> int:
        return self.tick()

    def on_receive(self, remote_ts: int) -> int:
        with self._lock:
            self._value = max(self._value, int(remote_ts)) + 1
            return self._value

    @property
    def value(self) -> int:
        return self._value

