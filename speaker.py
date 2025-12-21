import queue
import threading
from typing import Optional

try:
    import simpleaudio  # type: ignore
except Exception:
    simpleaudio = None


def config_get(config: dict, *keys: str, default=None):
    for key in keys:
        if key in config and config[key] is not None:
            return config[key]
    return default


class SpeakerService:
    """
    Simple audio playback worker for short voice prompts.
    Expects WAV files mapped by event name in config["voice_files"].
    """

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
        if not self.enabled or self.thread is not None:
            return
        self.thread = threading.Thread(target=self._worker, daemon=True)
        self.thread.start()

    def enqueue(self, event_name: str) -> None:
        if not self.enabled or not self.audio_available:
            return
        try:
            self.queue.put_nowait(event_name)
        except queue.Full:
            pass

    def _play_audio(self, path: str) -> None:
        if not simpleaudio:
            return
        try:
            wav = simpleaudio.WaveObject.from_wave_file(path)
            play_obj = wav.play()
            play_obj.wait_done()
        except Exception:
            pass

    def _worker(self) -> None:
        while not self.stop_event.is_set():
            try:
                name = self.queue.get(timeout=0.2)
            except queue.Empty:
                continue

            path = self.voice_files.get(name)
            if not path:
                continue
            try:
                self._play_audio(path)
            except Exception:
                pass

    def cleanup(self) -> None:
        # simpleaudio does not require cleanup; nothing to do.
        return
