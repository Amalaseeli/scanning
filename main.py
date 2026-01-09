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
import socket
from datetime import date

from sql_connection import append_spool, config_get, db_flush_worker, load_entry_no, log, save_entry_no, stop_event
from speaker import SpeakerService
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

def _is_network_up(host: str = "8.8.8.8", timeout: float = 3.0) -> bool:
    """Basic reachability check to detect network loss."""
    try:
        socket.create_connection((host, 53), timeout=timeout).close()
        return True
    except OSError:
        return False


def network_monitor_worker(config: dict, speaker: SpeakerService | None = None) -> None:
    host = config_get(config, "network_check_host", default="8.8.8.8")
    interval = float(config_get(config, "network_check_interval_sec", default=5.0))
    threshold = int(config_get(config, "network_check_fail_threshold", default=2))
    fail_count = 0
    alerted = False

    while not stop_event.is_set():
        ok = _is_network_up(host)
        if ok:
            fail_count = 0
            alerted = False
        else:
            fail_count += 1
            if fail_count >= threshold and not alerted:
                log(config, f"Network check failed to {host}; triggering network_lost alert.")
                try:
                    if speaker is not None:
                        speaker.enqueue("network_lost")
                except Exception as ex:
                    log(config, f"Failed to enqueue network_lost (network monitor): {ex}")
                alerted = True
        time.sleep(interval)


def scanner_worker(config: dict, speaker: SpeakerService | None = None, on_scan = None) -> None:
    dev_path = resolve_scanner_device(config)
    device_id = config_get(config, "device_id", "Device_id")
    config_user = config_get(config, "user_id", "User_id")
    resolved_user = resolve_user(config, dev_path)
    scanner_name = resolved_user or os.path.basename(dev_path)
    user_id = config_user or scanner_name

    entry_no = load_entry_no(config)
    buffer = ""
    shift = False

    while not stop_event.is_set():
        try:
            dev = InputDevice(dev_path)
            log(config, f"Scanner opened: {dev_path}")
            if speaker is not None:
                speaker.enqueue("device_ready")

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
                            "ScannerName": scanner_name,
                            "EntryNo": entry_no,
                            "Barcode": barcode_formatted,
                            "ScanDate": now.date().isoformat(),
                            "ScanTime": now.time().strftime("%H:%M:%S"),
                            "UserID": user_id or scanner_name,
                            **parent_fields,
                        }

                        append_spool(config, rec)
                        save_entry_no(config, entry_no + 1)

                        log(config, f"SCAN saved to spool: EntryNo={entry_no} Barcode={barcode_formatted}")

                        if on_scan:
                            try:
                                
                                try:
                                    on_scan(entry_no, barcode_formatted)
                                except TypeError:
                                    on_scan(entry_no)
                            except Exception as ex:
                                log(config, f"on_scan callback failed: {ex}")
                        entry_no += 1

                    continue

                ch = keycode_to_char(keycode, shift)
                if ch:
                    buffer += ch
                shift = False

        except FileNotFoundError:
            log(config, f"Scanner device not found: {dev_path}. Retrying in 2s.")
            time.sleep(2)

        except Exception as e:
            log(config, f"Scanner error: {e}. Retrying in 2s.")
            time.sleep(2)


def main():
    config = load_config()

    speaker = SpeakerService(config, stop_event)
    speaker.start()

    dev_path = resolve_scanner_device(config)
    log(config, f"Scanner device resolved to: {dev_path}")

    db_thread = threading.Thread(target=db_flush_worker, args=(config, speaker), daemon=True)
    scan_thread = threading.Thread(target=scanner_worker, args=(config, speaker), daemon=True)
    net_thread = threading.Thread(target=network_monitor_worker, args=(config, speaker), daemon=True)

    db_thread.start()
    scan_thread.start()
    net_thread.start()

    try:
        while scan_thread.is_alive():
            scan_thread.join(timeout=0.5)
    except KeyboardInterrupt:
        stop_event.set()
        log(config, "Stopping...")

    stop_event.set()
    scan_thread.join(timeout=2)
    db_thread.join(timeout=5)
    net_thread.join(timeout=2)
    speaker.cleanup()
    
if __name__ == "__main__":
    main()





            
