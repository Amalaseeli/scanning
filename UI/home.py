import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
import threading
import time
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from config_utils import load_config
from main import scanner_worker, network_monitor_worker
from speaker import SpeakerService
from sql_connection import db_flush_worker, stop_event


class ScannerUI:
    def __init__(self, root):
        self.root = root       
        self.running = False
        self.count = 0
        self.scan_thread = None
        self.net_thread = None

        self.config = load_config()
        self.speaker = SpeakerService(self.config, stop_event)
        self.speaker.start()

        self.db_thread = threading.Thread(target=db_flush_worker, args=(self.config, self.speaker), daemon=True)    
        self.db_thread.start()

        self.live_count = tk.StringVar(value = "Live Count : 0")

        self.count_label = ttk.Label (root, textvariable= self.live_count, font=("Helvetica", 16))
        self.count_label.pack(pady = 20)

        self.start_button = ttk.Button(root, text="Start", command=self.start_scanning)
        self.start_button.pack(side = tk.LEFT, padx = 10, pady = 10)

        self.stop_button = ttk.Button(root, text="Stop", command=self.stop_scanning, state = tk.DISABLED)
        self.stop_button.pack(side = tk.RIGHT, padx = 10, pady = 10)

    def start_scanning(self):
        if self.running:
            return
        
        if not messagebox.askyesno("Start Scanning", "Are you sure you want to start scanning?"):
            return
        
        stop_event.clear()
        self.running = True
        self.count = 0
        self.live_count.set("Live Count : 0")
        self.start_button.state(['disabled'])
        self.stop_button.state(['!disabled'])

        self.scan_thread = threading.Thread(target=scanner_worker, args = (self.config, self.speaker, self._on_scan),daemon=True)
        self.scan_thread.start()

        self.net_thread = threading.Thread(target=network_monitor_worker, args=(self.config, self.speaker), daemon=True)
        self.net_thread.start()

   
    def stop_scanning(self):
        if not self.running:
            return
        if not messagebox.askyesno("Stop Scanning", "Are you sure you want to stop scanning?"):
            return
        
        self.running = False
        stop_event.set()
        self.start_button.state(['!disabled'])
        self.stop_button.state(['disabled'])

    def _on_scan(self, entry_no):
        self.count += 1
        self.root.after(0, lambda c=self.count: self.live_count.set(f"Live Count : {c}"))

def main():
    root = tk.Tk()
    root.title("Scanner UI")
    app = ScannerUI(root)
    root.protocol("WM_DELETE_WINDOW", app.stop_scanning)
    root.mainloop()

if __name__ == "__main__":
    main()









