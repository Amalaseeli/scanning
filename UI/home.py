import tkinter as tk
from tkinter import messagebox, ttk
import threading
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from config_utils import load_config
from main import scanner_worker, network_monitor_worker
from speaker import SpeakerService
from sql_connection import db_flush_worker, stop_event
import pyautogui

screen_width, screen_height = pyautogui.size()

class ScannerUI:
    def __init__(self, root):
        self.root = root
        self.running = False
        self.count = 0

        # Colors
        bg = "#0f172a"          # page background
        panel = "#111827"
        btn_bg = "#2563eb"
        btn_bg_disabled = "#1e3a8a"
        btn_fg = "#ffffff"

        root.configure(bg=bg)
        root.title("Scanner UI")

        # ttk style overrides
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TFrame", background=panel)
        style.configure("TLabel", background=panel, foreground="#e5e7eb")
        style.configure(
            "Primary.TButton",
            background=btn_bg,
            foreground=btn_fg,
            padding=10,
            font=("Segoe UI Semibold", 11),
            borderwidth=0,
            focusthickness=0,
        )
        style.map(
            "Primary.TButton",
            background=[("disabled", btn_bg_disabled), ("active", "#1d4ed8")],
            foreground=[("disabled", "#cbd5e1")],
        )

        container = ttk.Frame(root, padding=20, style="TFrame")
        container.pack(fill=tk.BOTH, expand=True)

        self.live_count = tk.StringVar(value="Live Count: 0")
        self.count_label = tk.Label(
            container,
            textvariable=self.live_count,
            font=("Segoe UI Black", 28),
            bg="#000000",
            fg="#ffffff",
            padx=20,
            pady=20,
        )
        self.count_label.pack(pady=(0, 24), fill=tk.X)

        btns = ttk.Frame(container, style="TFrame")
        btns.pack()

        self.start_button = ttk.Button(btns, text="Start", style="Primary.TButton", command=self.start_scanning)
        self.start_button.pack(side=tk.LEFT, padx=8)

        self.stop_button = ttk.Button(btns, text="Stop", style="Primary.TButton",
                                      command=self.stop_scanning, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=8)

        # Wiring
        self.config = load_config()
        self.speaker = SpeakerService(self.config, stop_event)
        self.speaker.start()
        self.db_thread = threading.Thread(target=db_flush_worker, args=(self.config, self.speaker), daemon=True)
        self.db_thread.start()
        self.scan_thread = None
        self.net_thread = None

    def start_scanning(self):
        if self.running:
            return
        if not messagebox.askyesno("Start Scanning", "Are you sure you want to start scanning?"):
            return
        stop_event.clear()
        self.running = True
        self.count = 0
        self.live_count.set("Live Count: 0")
        self.start_button.state(["disabled"])
        self.stop_button.state(["!disabled"])

        self.scan_thread = threading.Thread(target=scanner_worker,
                                            args=(self.config, self.speaker, self._on_scan),
                                            daemon=True)
        self.scan_thread.start()
        self.net_thread = threading.Thread(target=network_monitor_worker,
                                           args=(self.config, self.speaker),
                                           daemon=True)
        self.net_thread.start()

    def stop_scanning(self):
        if not self.running:
            return
        if not messagebox.askyesno("Stop Scanning", "Are you sure you want to stop scanning?"):
            return
        self.running = False
        stop_event.set()
        self.start_button.state(["!disabled"])
        self.stop_button.state(["disabled"])

    def _on_scan(self, entry_no):
        self.count += 1
        self.root.after(0, lambda c=self.count: self.live_count.set(f"Live Count: {c}"))

    def on_close(self):
        if messagebox.askyesno("Quit", "Stop scanning and exit?"):
            self.running = False
            stop_event.set()
            try:
                self.speaker.cleanup()
            except Exception:
                pass
            self.root.destroy()

def main():
    root = tk.Tk()
    root.geometry(f"{screen_width}x{screen_height}")
    app = ScannerUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_close)
    root.mainloop()

if __name__ == "__main__":
    main()
