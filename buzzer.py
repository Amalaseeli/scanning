import queue
import threading
import time
from typing import Any, Optional

try:
    import RPi.GPIO as GPIO  # type: ignore
except Exception:
    GPIO = None


def cfg_get(cfg: dict, *keys: str, default=None):
    for key in keys:
        if key in cfg and cfg[key] is not None:
            return cfg[key]
    return default


class BuzzerService:
    def __init__(self, cfg: dict, stop_event, max_queue_size: int = 50):
        self.cfg = cfg
        self.stop_event = stop_event
        self.queue: "queue.Queue[str]" = queue.Queue(maxsize=max_queue_size)
        self.thread: Optional[threading.Thread] = None

        self.enabled = bool(cfg_get(cfg, "BUZZER_ENABLED", "buzzer_enabled", default=False))
        self.pin = int(cfg_get(cfg, "buzzer_pin", default=18))
        self.pause = float(cfg_get(cfg, "beep_pause_sec", "buzzer_pause", default=0.06))

        patterns = cfg_get(cfg, "beep_patterns", default=None) or {}
        self.patterns = patterns if isinstance(patterns, dict) else {}

    def init_gpio(self) -> None:
        if not self.enabled or GPIO is None:
            return
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(self.pin, GPIO.OUT, initial=GPIO.LOW)

    def start(self) -> None:
        if self.thread is not None:
            return
        self.init_gpio()
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()

    def enqueue(self, event_name: str) -> None:
        """Non-blocking: request a beep pattern by name."""
        if not self.enabled:
            return
        try:
            self.queue.put_nowait(event_name)
        except queue.Full:
            pass

    def _play_pattern(self, pattern: Any) -> None:
        if not self.enabled or GPIO is None:
            return
        for dur in pattern:
            GPIO.output(self.pin, GPIO.HIGH)
            time.sleep(float(dur))
            GPIO.output(self.pin, GPIO.LOW)
            time.sleep(self.pause)

    def _worker(self) -> None:
        while not self.stop_event.is_set():
            try:
                name = self.queue.get(timeout=0.2)
            except queue.Empty:
                continue

            pattern = self.patterns.get(name)
            if pattern is None:
                pattern = [0.05]

            try:
                self._play_pattern(pattern)
            except Exception:
                pass

    def cleanup(self) -> None:
        if GPIO is None:
            return
        try:
            GPIO.cleanup()
        except Exception:
            pass


class Buzzer:
    """Legacy blocking buzzer helper (kept for compatibility)."""

    def __init__(self, pin: int):
        self.pin = pin
        if GPIO is None:
            raise RuntimeError("RPi.GPIO is not available on this platform.")
        GPIO.setmode(GPIO.BCM)
        GPIO.setup(self.pin, GPIO.OUT, initial=GPIO.LOW)

    def beep(self, pin: int, duration=0.1, repeat=1, pause=0.05):
        for _ in range(repeat):
            GPIO.output(pin, GPIO.HIGH)
            time.sleep(duration)
            GPIO.output(pin, GPIO.LOW)
            time.sleep(pause)

            
