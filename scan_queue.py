import threading, queue
from sql_connection import get_connection  # adjust to your helper

scan_queue = queue.Queue()
STOP = object()

def db_worker():
    conn = get_connection()
    cur = conn.cursor()
    batch = []
    while True:
        item = scan_queue.get()
        if item is STOP:
            break
        batch.append(item)
        # flush when batch hits 20 or after timeout (see below)
        if len(batch) >= 20:
            cur.executemany("INSERT INTO scans (barcode, device_id, entry_no) VALUES (%s, %s, %s)", batch)
            conn.commit()
            batch.clear()
    if batch:
        cur.executemany("INSERT ...", batch)
        conn.commit()
    conn.close()

# start workass er
threading.Thread(target=db_worker, daemon=True).start()

# in your KEY_ENTER block
entry_no += 1
scan_queue.put((barcode, device_id, entry_no))
