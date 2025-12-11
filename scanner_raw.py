#!/usr/bin/env python3
from evdev import InputDevice, categorize, ecodes
import os
import sys


DEFAULT_DEVICE : "/dev/input/by-id/usb-Newtologic_4010E_XXXXXX-event-kbd"


def main():
   
    if len(sys.argv) > 1:
        dev_path = sys.argv[1]
    else:
        dev_path = DEFAULT_DEVICE

    if not os.path.exists(dev_path):
        print(f"Device not found: {dev_path}")
        print("Use:  python scan_debug_raw.py /dev/input/by-id/your-scanner-here")
        sys.exit(1)

    print(f"Opening input device: {dev_path}")
    print("Press Ctrl+C to stop.\n")

    device = InputDevice(dev_path)

    for event in device.read_loop():
    
        if event.type != ecodes.EV_KEY:
            continue

        key_event = categorize(event)

        keycode = key_event.keycode
        if isinstance(keycode, list):
            keycode = keycode[0]

        state_name = {
            key_event.key_up: "UP",
            key_event.key_down: "DOWN",
            key_event.key_hold: "HOLD",
        }.get(key_event.keystate, str(key_event.keystate))

        print(f"code={keycode:>15}  state={state_name}")
       

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")
