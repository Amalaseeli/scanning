from evdev import InputDevice, categorize, ecodes
from datetime import datetime
import log_config
import logging
import os 
import json
import threading
from config_utils import load_config
import re
import time  
import pathlib
from datetime import date

from sql_connection import append_spool, cfg_get, db_flush_worker, load_entry_no, log, save_entry_no, stop_event
from buzzer import BuzzerService
from scanner_device_resolver import resolve_scanner_device, resolve_user

base_dir = pathlib.Path(__file__).parent.resolve()
config_path = base_dir / 'config.json'

logger = logging.getLogger("scanner_service")

KEYMAP = {
    "KEY_0": "0", "KEY_1": "1", "KEY_2": "2", "KEY_3": "3", "KEY_4": "4",
    "KEY_5": "5", "KEY_6": "6", "KEY_7": "7", "KEY_8": "8", "KEY_9": "9",
    "KEY_A": "a", "KEY_B": "b", "KEY_C": "c", "KEY_D": "d", "KEY_E": "e",
    "KEY_F": "f", "KEY_G": "g", "KEY_H": "h", "KEY_I": "i", "KEY_J": "j",
    "KEY_K": "k", "KEY_L": "l", "KEY_M": "m", "KEY_N": "n", "KEY_O": "o",
    "KEY_P": "p", "KEY_Q": "q", "KEY_R": "r", "KEY_S": "s", "KEY_T": "t",
    "KEY_U": "u", "KEY_V": "v", "KEY_W": "w", "KEY_X": "x", "KEY_Y": "y",
    "KEY_Z": "z",
    "KEY_MINUS": "-",
    "KEY_EQUAL": "=",
    "KEY_SPACE": " ",
    "KEY_SLASH": "/",
    "KEY_DOT": ".",
}

def keycode_to_char(keycode:str, shift:bool) -> str:
    """convert a keycode like 'KEY_A' to 'a' or 'A' ."""
    character = KEYMAP.get(keycode, "")
    if not character:
        return ""
    if character.isalpha():
        return character.upper() if shift else character.lower()
    return character

def format_parent_child_record(raw_barcode: str) -> str:
    raw = raw_barcode.strip()  
    start = re.search(
        r"-(?=[A-Za-z]{2,}\d+-\d+[A-Za-z]{2,}\d+-\d+)",
        raw,
        flags=re.IGNORECASE,
    )
    if not start:
        return raw.rstrip("-").strip()

    parent_code = raw[:start.start()].rstrip("- ").strip()

    # Children start from here
    child_code = raw[start.start():].lstrip("- ")
    child_code = child_code.replace("~", "|")
    child_code = re.sub(r"(\d)(?=[A-Za-z]{2,}\d+-\d+)", r"\1|", child_code, flags=re.IGNORECASE)

    formatted_children = []
    for token in child_code.split("|"):
        token = token.strip("- ").strip()
        mm = re.fullmatch(r"([A-Za-z]{2,}\d+)-(\d+)", token, flags=re.IGNORECASE)
        if mm:
            item, qty = mm.group(1).upper(), mm.group(2)
            formatted_children.append(f"{item}_{qty}")

    return parent_code if not formatted_children else f"{parent_code} [{'|'.join(formatted_children)}]"

def fetch_barcode_segments(parent_barcode: str) -> dict:
    segments = parent_barcode.split('-')
    output = {
        "Stowage": None,
        "FlightNo": None,
        "OrderDate": None,
        "DACS_CLASS": None,
        "Leg": None,
        "Gally": None,
        "BlockNo": None,
        "ContainerCode": None,
        "DES": None,
        "DACS_ACType": None,        
    }

    if len(segments) >= 1: output["Stowage"] = segments[0]
    if len(segments) >= 2: output["FlightNo"] = segments[1]
    if len(segments) >= 3: 
        try:
            dd, mm, yy = segments[2].split('.')
            yy = int(yy)
            yyyy = 2000 + yy if yy <= 79 else 1900 + yy
            output["OrderDate"] = date(yyyy, int(mm), int(dd)).isoformat()
        except Exception: 
            output["OrderDate"] = None
    if len(segments) >= 4: output["DACS_CLASS"] = segments[3]
    if len(segments) >= 5: output["Leg"] = segments[4]  
    if len(segments) >= 6: output["Gally"] = segments[5]
    if len(segments) >= 7: output["BlockNo"] = segments[6]
    if len(segments) >= 8: output["ContainerCode"] = segments[7]
    if len(segments) >= 9: output["DES"] = segments[8]
    if len(segments) >= 10: output["DACS_ACType"] = segments[9]

    return output

def split_parent_barcode(raw_barcode: str) -> str:
    if "[" in raw_barcode:
        return raw_barcode.split("[", 1)[0].strip()
    return raw_barcode.strip()

def format_children_in_brackets(raw_barcode: str) -> str:
    return format_parent_child_record(raw_barcode)


def split_parent_from_formatted(formatted_barcode: str) -> str:
    return split_parent_barcode(formatted_barcode)


def parse_parent_fields(parent_barcode: str) -> dict:
    return fetch_barcode_segments(parent_barcode)



def scanner_worker(cfg: dict, buzzer: BuzzerService | None = None) -> None:
    dev_path = resolve_scanner_device(cfg)
    device_id = cfg_get(cfg, "device_id", "Device_id")
    user_id = resolve_user(cfg, dev_path)
    preferred_user = user_id

    entry_no = load_entry_no(cfg)
    buffer = ""
    shift = False

    while not stop_event.is_set():
        try:
            dev = InputDevice(dev_path)
            log(cfg, f"Scanner opened: {dev_path}")
            if buzzer is not None:
                buzzer.enqueue("READY")

            for event in dev.read_loop():
                if stop_event.is_set():
                    break

                if event.type != ecodes.EV_KEY:
                    continue

                key_event = categorize(event)
                if key_event.keystate != key_event.key_down:
                    continue

                keycode = key_event.keycode
                if isinstance(keycode, list):
                    keycode = keycode[0]

                if keycode in ("KEY_LEFTSHIFT", "KEY_RIGHTSHIFT"):
                    shift = True
                    continue

                if keycode == "KEY_ENTER":
                    if buffer:
                        raw_barcode = buffer
                        buffer = ""
                        shift = False

                        barcode_formatted = format_children_in_brackets(raw_barcode)
                        parent_text = split_parent_from_formatted(barcode_formatted)
                        parent_fields = parse_parent_fields(parent_text)

                        now = datetime.now()
                        rec = {
                            "DeviceID": device_id,
                            "ScannerName": os.path.basename(dev_path),
                            "PreferredUser": preferred_user,
                            "EntryNo": entry_no,
                            "Barcode": barcode_formatted,
                            "ScanDate": now.date().isoformat(),
                            "ScanTime": now.time().strftime("%H:%M:%S"),
                            "UserID": user_id,
                            **parent_fields,
                        }

                        append_spool(cfg, rec)
                        save_entry_no(cfg, entry_no + 1)

                        log(cfg, f"SCAN saved to spool: EntryNo={entry_no} Barcode={barcode_formatted}")
                        if buzzer is not None:
                            buzzer.enqueue("scan_ok")
                        entry_no += 1

                    continue

                ch = keycode_to_char(keycode, shift)
                if ch:
                    buffer += ch
                shift = False

        except FileNotFoundError:
            log(cfg, f"Scanner device not found: {dev_path}. Retrying in 2s.")
            time.sleep(2)

        except Exception as e:
            log(cfg, f"Scanner error: {e}. Retrying in 2s.")
            time.sleep(2)


def main():
    cfg = load_config()

    buzzer = BuzzerService(cfg, stop_event)
    buzzer.start()

    dev_path = resolve_scanner_device(cfg)
    log(cfg, f"Scanner device resolved to: {dev_path}")

    db_thread = threading.Thread(target=db_flush_worker, args=(cfg,), daemon=True)
    scan_thread = threading.Thread(target=scanner_worker, args=(cfg, buzzer), daemon=True)

    db_thread.start()
    scan_thread.start()

    try:
        while scan_thread.is_alive():
            scan_thread.join(timeout=0.5)
    except KeyboardInterrupt:
        stop_event.set()
        log(cfg, "Stopping...")

    stop_event.set()
    scan_thread.join(timeout=2)
    db_thread.join(timeout=5)
    buzzer.cleanup()
    
if __name__ == "__main__":
    main()





            


