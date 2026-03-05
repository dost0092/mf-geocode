import time
from dataclasses import dataclass

@dataclass
class RateLimiter:
    rps: float = 0.769
    _last: float = 0.0

    def wait(self):
        if self.rps <= 0:
            return
        interval = 1.0 / self.rps
        now = time.monotonic()
        delta = now - self._last
        if delta < interval:
            time.sleep(interval - delta)
        self._last = time.monotonic()
