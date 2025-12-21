import pathlib, pyttsx3

voice_text = {
    "device_ready": "Device is ready",
    "network_lost": "Internet connection lost, check your network",
}

sounds = pathlib.Path("sounds")
sounds.mkdir(exist_ok=True)

engine = pyttsx3.init()
for name, text in voice_text.items():
    out = sounds / f"{name}.wav"
    print(f"Generating {out}...")
    engine.save_to_file(text, str(out))
engine.runAndWait()
engine.stop()
print("Done.")
