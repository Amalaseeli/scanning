import queue
import threading
import logging
from typing import Optional

try:
    import simpleaudio  
except Exception:
    simpleaudio = None

logger = logging.getLogger("speaker")

def config_get(config: dict, *keys: str, default=None):
    for key in keys:
        if key in config and config[key] is not None:
            return config[key]
    return default


class SpeakerService:    
    def __init__(self, config: dict, stop_event, max_queue_size: int = 50):
        self.config = config
        self.stop_event = stop_event
        self.queue: "queue.Queue[str]" = queue.Queue(maxsize=max_queue_size)
        self.thread: Optional[threading.Thread] = None

        self.enabled = bool(config_get(config, "SPEAKER_ENABLED", "speaker_enabled", default=True))
        self.voice_files = config_get(config, "voice_files", "speaker_voice_files", default=None) or {}
        if not isinstance(self.voice_files, dict):
            self.voice_files = {}

        self.audio_available = simpleaudio is not None and bool(self.voice_files)

    def start(self) -> None:
        if self.thread is not None:
            return
        if not self.enabled:
            logger.info("Speaker disabled in config; skipping start.")
            return
        if not self.audio_available:
            logger.warning("Speaker audio unavailable (simpleaudio missing or no voice files); skipping start.")
            return
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()

    def enqueue(self, event_name: str) -> None:
        if not self.enabled or not self.audio_available:
            return
        path = self.voice_files.get(event_name)
        if not path:
            logger.warning("No voice file mapped for event=%s", event_name)
            return
        try:
            self.queue.put_nowait(event_name)
            logger.info("Queued voice event=%s path=%s", event_name, path)
        except queue.Full:
            logger.warning("Voice queue full; dropping event=%s", event_name)

    def _play_audio(self, path: str) -> None:
        if not simpleaudio:
            return
        try:
            wav = simpleaudio.WaveObject.from_wave_file(path)
            play_obj = wav.play()
            play_obj.wait_done()
            logger.info("Played voice file %s", path)
        except Exception as e:
            logger.error("Voice playback failed for %s: %s", path, e)

    def _worker(self) -> None:
        while not self.stop_event.is_set():
            try:
                name = self.queue.get(timeout=0.2)
            except queue.Empty:
                continue

            path = self.voice_files.get(name)
            if not path:
                logger.warning("No file path for queued voice event=%s", name)
                continue
            try:
                self._play_audio(path)
            except Exception as e:
                logger.error("Voice worker error for %s: %s", path, e)

    def cleanup(self) -> None:
        return
