from evdev import InputDevice, categorize, ecodes
from datetime import datetime
import log_config
import logging
import os 
import json
import threading
import queue
from config_utils import load_config
import re


logger = logging.getLogger(__name__)

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

def format_parent_child_record(raw_barcode:str) -> str:
    """Format barcode for parent-child relationship."""
    raw_barcode = raw_barcode.strip()

    # Pattern match itemcode-quantity
    match = re.match(r"([A-za-z]{2,}\d+)-(\d+)", raw_barcode)

    # If child barcode not found.
    if not match:
        return raw_barcode
    
    parent_code = raw_barcode[:match.start()].rstrip("-")

    child_code = raw_barcode[match.start():]

    child_code= re.sub(r"(\d)(?=[A-Za-z]{2,}\d+-\d+)", r"\1|", child_code)

    # Split token with '|'
    tokens = child_code.split("|")

    formatted_children = []
    for token in tokens:
        token.strip("-").strip()
        mm = re.fullmatch(r"([A-za-z]{2,}\d+)-(\d+)", token)
        if not mm: 
            continue
        item, quantity = mm.group(1).upper(), mm.group(2)
        formatted_children.append(f"{item}-{quantity}")

    if not formatted_children:
        return parent_code
    
    return f"{parent_code}|{'|'.join(formatted_children)}"




def main():
    config = load_config()
    scanner_device = config.get("Scanner_input_device", "/dev/input/by-id/usb-Newtologic_4010E_XXXXXX-event-kbd")  

    # Open Scanner device
    device = InputDevice(scanner_device)
    device_id = config.get("Device_id")
    entry_no = config.get("Starting_entry_no", 1)

    buffer = ""
    shift = False

    # * Device.read_loop() give us stream of events from the scanner.
    for event in device.read_loop():
        # check the event type, we only care about key events.
        if event.type != ecodes.EV_KEY:
            continue

        # * pressed/released/held
        key_event = categorize(event)

        if key_event.keystate != key_event.key_down:
            continue       

        keycode = key_event.keycode

        # * Some devices send list like ["KEY_LEFTSHIFT", "KEY_1"]
        if isinstance(keycode, list):
            keycode = keycode[0]
        
        if keycode in ("KEY_LEFTSHIFT", "KEY_RIGHTSHIFT"):
            shift = True
            continue

        # End of barcode
        if keycode == "KEY_ENTER":
            if buffer:
                #barcode = buffer
                raw_barcode = buffer
                buffer = ""
                shift = False
                formatted_barcode = format_parent_child_record(raw_barcode)

                logger.info(
                    "Entry_no = %s, Device_id = %s, Scanner = %s, Barcode= %s",
                    entry_no,
                    device_id,
                    scanner_device,
                    formatted_barcode,
                )
                entry_no += 1
            continue

        char = keycode_to_char(keycode, shift)
        if char:
            buffer += char

        shift = False
    
if __name__ == "__main__":
    main()





            


