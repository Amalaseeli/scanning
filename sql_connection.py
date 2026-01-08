import json
import logging
import os
import threading
import time

import pyodbc
from db_utils import DatabaseConnector

logger = logging.getLogger("sql_connection")

stop_event = threading.Event()

CREATE_TABLE_TEMPLATE = """
CREATE TABLE {table_name} (
    ID BIGINT IDENTITY(1,1) NOT NULL,
    DeviceID NVARCHAR(50) NOT NULL,
    ScannerName NVARCHAR(255) NULL,
    EntryNo INT NOT NULL,
    Barcode NVARCHAR(MAX) NOT NULL,
    ScanDate DATE NOT NULL,
    ScanTime TIME(0) NOT NULL,
    UserID NVARCHAR(50) NULL,

    Stowage NVARCHAR(255) NULL,
    FlightNo NVARCHAR(MAX) NULL,
    OrderDate DATE NULL,
    DACS_CLASS NVARCHAR(255) NULL,
    Leg NVARCHAR(255) NULL,
    Gally NVARCHAR(255) NULL,
    BlockNo NVARCHAR(255) NULL,
    ContainerCode NVARCHAR(255) NULL,
    DES NVARCHAR(255) NULL,
    DACS_ACType NVARCHAR(255) NULL,

    CONSTRAINT PK_Do_Co_Scanning_Data PRIMARY KEY CLUSTERED (DeviceID, EntryNo)
);
""".strip()


def config_get(config: dict, *keys: str, default=None):
    for key in keys:
        if key in config and config[key] is not None:
            return config[key]
    return default


def log(config: dict, message: str) -> None:
    logger.info(message)


def connect_db(config: dict):
    connection_string = config_get(config, "sql_connection_string", "Sql_connection_credentials")
    if connection_string:
        log(config, "Connecting with config.json connection string.")
        return pyodbc.connect(connection_string, autocommit=False)
    
    db = DatabaseConnector()
    log(config, "Connecting with db_cred.yaml settings.")
    return db.create_connection()


def _quote_table_name(table: str) -> str:
    table = (table or "").strip()
    if not table:
        raise ValueError("Empty table name")

    parts = [p.strip() for p in table.split(".") if p.strip()]
    quoted_parts = []
    for part in parts:
        part = part.strip("[]")
        part = part.replace("]", "]]")
        quoted_parts.append(f"[{part}]")
    return ".".join(quoted_parts)


def ensure_table_exists(conn, table: str) -> bool:
    """
    Ensure the target table exists. Returns True if created, False if already existed.
   .
    """
    if conn is None:
        raise ValueError("No DB connection")
    quoted_table = _quote_table_name(table)

    cur = conn.cursor()
    try:
      
        try:
            cur.execute("SELECT OBJECT_ID(?, 'U')", table)
            row = cur.fetchone()
            if row and row[0] is not None:
               
                try:
                    cur.execute(
                        "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = 'ScannerName'",
                        table.split(".")[-1],
                    )
                    if not cur.fetchone():
                        cur.execute(f"ALTER TABLE {quoted_table} ADD ScannerName NVARCHAR(255) NULL")
                        conn.commit()
                except Exception:
                    pass

                return False
        except Exception:
            
            cur.execute(f"SELECT 1 FROM {quoted_table} WHERE 1=0")
            return False

        create_sql = CREATE_TABLE_TEMPLATE.format(table_name=quoted_table)
        cur.execute(create_sql)
        conn.commit()
        return True
    finally:
        try:
            cur.close()
        except Exception:
            pass


def load_entry_no(config: dict) -> int:
    state_file = config_get(config, "state_file")
    start_entry_no = int(config_get(config, "Starting_entry_no", "starting_entry_no", default=1))
    if not state_file:
        return start_entry_no

    try:
        with open(state_file, "r", encoding="utf-8") as f:
            state_data = json.load(f)
            return int(state_data.get("last_entry_no", start_entry_no))
    except (FileNotFoundError, json.JSONDecodeError):
        return start_entry_no
    except Exception as e:
        logger.error("Error loading state file: %s", e)
        return start_entry_no


def save_entry_no(config: dict, next_entry_no: int) -> None:
    state_file = config_get(config, "state_file")
    if not state_file:
        return

    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    tmp = state_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"last_entry_no": next_entry_no}, f)
    os.replace(tmp, state_file)


def load_spool_offset(config: dict) -> int:
    spool_offset_file = config_get(config, "spool_offset_file")
    if not spool_offset_file:
        return 0
    try:
        with open(spool_offset_file, "r", encoding="utf-8") as f:
            return int(f.read().strip() or "0")
    except FileNotFoundError:
        return 0
    except Exception as e:
        logger.error("Error loading spool offset file: %s", e)
        return 0


def save_spool_offset(config: dict, offset: int) -> None:
    spool_offset_file = config_get(config, "spool_offset_file")
    if not spool_offset_file:
        return

    os.makedirs(os.path.dirname(spool_offset_file), exist_ok=True)
    tmp = spool_offset_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str(offset))
    os.replace(tmp, spool_offset_file)


def append_spool(config: dict, record: dict) -> None:
    spool_file = config_get(config, "spool_file")
    if not spool_file:
        return

    os.makedirs(os.path.dirname(spool_file), exist_ok=True)
    line = json.dumps(record, ensure_ascii=False)
    with open(spool_file, "a", encoding="utf-8", buffering=1) as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())


def db_flush_worker(config: dict, speaker=None) -> None:
    Summary_post_entry = int(config.get(config, "Summary_post_entry", default=0)) == 1
    table = config_get(config, "table_name", "Table_name")
    if not table:
        raise ValueError("Missing table name: table_name/Table_name")
    quoted_table = _quote_table_name(table)

    flush_interval = float(config_get(config, "db_flush_interval_sec", "db_save_interval", default=1.0))
    heartbeat_interval = float(config_get(config, "db_heartbeat_interval_sec", default=10.0))
    conn = None
    offset = load_spool_offset(config)
    last_heartbeat = 0.0

    if Summary_post_entry:
        insert_sql = f"""
        INSERT INTO {quoted_table}
        (DeviceID, ScannerName, EntryNo, Barcode, ScanDate, ScanTime, UserID)
        VALUES (?, ?, ?, ?, ?, ?, ?)"""
    else:
        insert_sql = f"""
            INSERT INTO {quoted_table}
            (DeviceID, ScannerName, EntryNo, Barcode, ScanDate, ScanTime, UserID,
            Stowage, FlightNo, OrderDate, DACS_CLASS, Leg, Gally, BlockNo, ContainerCode, DES, DACS_ACType)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

    network_alerted = False

    def _params(rec, Summary_post_entry):
        if Summary_post_entry:
            return (
                rec["DeviceID"], rec.get("ScannerName"), rec["EntryNo"], rec["Barcode"],
                rec["ScanDate"], rec["ScanTime"], rec.get("UserID"),
            )
        return (
            rec["DeviceID"], rec.get("ScannerName"), rec["EntryNo"], rec["Barcode"],
            rec["ScanDate"], rec["ScanTime"], rec.get("UserID"),
            rec.get("Stowage"), rec.get("FlightNo"), rec.get("OrderDate"),
            rec.get("DACS_CLASS"), rec.get("Leg"), rec.get("Gally"),
            rec.get("BlockNo"), rec.get("ContainerCode"), rec.get("DES"), rec.get("DACS_ACType"),
        ) 

    while not stop_event.is_set():
        batch = []
        new_offset = offset

        try:
            if conn is None:
                conn = connect_db(config)
                if conn is None:
                    log(config, "DB connect returned None; retry in 5s.")
                    if speaker is not None and not network_alerted:
                        log(config, "Enqueueing network_lost (connect returned None).")
                        try:
                            speaker.enqueue("network_lost")
                        except Exception as ex:
                            log(config, f"Failed to enqueue network_lost (connect): {ex}")
                        network_alerted = True
                    time.sleep(5)
                    continue
                try:
                    created = ensure_table_exists(conn, table)
                    if created:
                        log(config, f"Created missing table: {table}")
                except Exception as e:
                    log(config, f"Table check/create failed for {table}: {e}")
                log(config, "DB connected.")
                network_alerted = False

            time.sleep(flush_interval)

            spool_path = config_get(config, "spool_file")
            if not spool_path or not os.path.exists(spool_path):
                continue

            with open(spool_path, "r", encoding="utf-8") as f:
                f.seek(offset)
                while True:
                    line = f.readline()
                    if not line:
                        break
                    line = line.strip()
                    if not line:
                        new_offset = f.tell()
                        continue
                    try:
                        rec = json.loads(line)
                        batch.append(rec)
                        new_offset = f.tell()
                    except Exception:
                        new_offset = f.tell()

            if not batch:
                offset = new_offset
                save_spool_offset(config, offset)
                if conn is not None and (time.time() - last_heartbeat) >= heartbeat_interval:
                    try:
                        cur = conn.cursor()
                        try:
                            cur.execute("SELECT 1")
                            cur.fetchone()
                        finally:
                            try:
                                cur.close()
                            except Exception:
                                pass
                        network_alerted = False
                    except pyodbc.Error as e:
                        log(config, f"DB heartbeat failed: {e}. Reconnecting in 5s.")
                        if speaker is not None and not network_alerted:
                            log(config, "Enqueueing network_lost (heartbeat failed).")
                            try:
                                speaker.enqueue("network_lost")
                            except Exception:
                                log(config, "Failed to enqueue network_lost (heartbeat): %s", e)
                            network_alerted = True
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = None
                        time.sleep(5)
                    last_heartbeat = time.time()
                continue

            cur = conn.cursor()
            try:
                for rec in batch:
                    cur.execute(
                        insert_sql,
                        *(_params(rec, Summary_post_entry)) 
                    )
                conn.commit()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass

            offset = new_offset
            save_spool_offset(config, offset)
            log(config, f"DB flush: inserted {len(batch)} rows. offset={offset}")
            network_alerted = False

        except pyodbc.IntegrityError as e:
            log(config, f"DB integrity error: {e}. Trying row-by-row.")
            try:
                conn.rollback()
            except Exception:
                pass

            try:
                cur = conn.cursor()
                try:
                    ok = 0
                    for rec in batch:
                        try:
                            cur.execute(
                                insert_sql,
                                rec["DeviceID"],
                                rec.get("ScannerName"),
                                rec["EntryNo"],
                                rec["Barcode"],
                                rec["ScanDate"],
                                rec["ScanTime"],
                                rec.get("UserID"),
                                rec.get("Stowage"),
                                rec.get("FlightNo"),
                                rec.get("OrderDate"),
                                rec.get("DACS_CLASS"),
                                rec.get("Leg"),
                                rec.get("Gally"),
                                rec.get("BlockNo"),
                                rec.get("ContainerCode"),
                                rec.get("DES"),
                                rec.get("DACS_ACType"),
                            )
                            ok += 1
                        except pyodbc.IntegrityError:
                            continue
                    conn.commit()
                finally:
                    try:
                        cur.close()
                    except Exception:
                        pass

                offset = new_offset
                save_spool_offset(config, offset)
                log(config, f"DB flush row-by-row: inserted {ok}/{len(batch)}. offset={offset}")
            except Exception as e2:
                log(config, f"DB row-by-row failed: {e2}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                time.sleep(5)

        except pyodbc.Error as e:
            log(config, f"DB error: {e}. Reconnecting in 5s.")
            if speaker is not None and not network_alerted:
                log(config, "Enqueueing network_lost (DB error).")
                try:
                    speaker.enqueue("network_lost")
                except Exception as ex:
                    log(config, f"Failed to enqueue network_lost (DB error): {ex}")
                network_alerted = True
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(5)

        except Exception as e:
            log(config, f"DB worker error: {e}")
            time.sleep(5)

    if conn is not None:
        try:
            conn.commit()
            conn.close()
        except Exception:
            pass
