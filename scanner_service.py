from evdev import InputDevice, categorize, ecodes
from datetime import datetime
import log_config
import logging
import os 
import json
import threading
import queue
from config_utils import load_config

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
                barcode = buffer
                buffer = ""
                shift = False
                entry_no += 1
                logger.info(
                    "Entry_no = %s, Device_id = %s, Scanner = %s, Barcode= %s",
                    entry_no,
                    device_id,
                    scanner_device,
                    barcode,)
            continue

        char = keycode_to_char(keycode, shift)
        if char:
            buffer += char

        shift = False
    
if __name__ == "__main__":
    main()





            



