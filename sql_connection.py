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
    PreferredUser NVARCHAR(100) NULL,
    EntryNo INT NOT NULL,
    Barcode NVARCHAR(255) NOT NULL,
    ScanDate DATE NOT NULL,
    ScanTime TIME(0) NOT NULL,
    UserID NVARCHAR(50) NULL,

    Stowage NVARCHAR(50) NULL,
    FlightNo NVARCHAR(MAX) NULL,
    OrderDate DATE NULL,
    DACS_CLASS NVARCHAR(50) NULL,
    Leg NVARCHAR(50) NULL,
    Gally NVARCHAR(50) NULL,
    BlockNo NVARCHAR(50) NULL,
    ContainerCode NVARCHAR(50) NULL,
    DES NVARCHAR(50) NULL,
    DACS_ACType NVARCHAR(50) NULL,

    CONSTRAINT PK_Do_Co_Scanning_Data PRIMARY KEY CLUSTERED (DeviceID, EntryNo)
);
""".strip()


def cfg_get(cfg: dict, *keys: str, default=None):
    for key in keys:
        if key in cfg and cfg[key] is not None:
            return cfg[key]
    return default


def log(cfg: dict, message: str) -> None:
    logger.info(message)


def connect_db(cfg: dict):
    connection_string = cfg_get(cfg, "sql_connection_string", "Sql_connection_credentials")
    if connection_string:
        log(cfg, "Connecting with config.json connection string.")
        return pyodbc.connect(connection_string, autocommit=False)

    # Default: use db_cred.yaml via DatabaseConnector
    db = DatabaseConnector()
    log(cfg, "Connecting with db_cred.yaml settings.")
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
    Intended for SQL Server (via FreeTDS / ODBC).
    """
    quoted_table = _quote_table_name(table)

    cur = conn.cursor()
    try:
        # SQL Server fast-path
        try:
            cur.execute("SELECT OBJECT_ID(?, 'U')", table)
            row = cur.fetchone()
            if row and row[0] is not None:
                # Ensure ScannerName column exists; add if missing.
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

                # Ensure PreferredUser column exists; add if missing.
                try:
                    cur.execute(
                        "SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = 'PreferredUser'",
                        table.split(".")[-1],
                    )
                    if not cur.fetchone():
                        cur.execute(f"ALTER TABLE {quoted_table} ADD PreferredUser NVARCHAR(100) NULL")
                        conn.commit()
                except Exception:
                    pass
                return False
        except Exception:
            # Fallback: probe table
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


def load_entry_no(cfg: dict) -> int:
    state_file = cfg_get(cfg, "state_file")
    start_entry_no = int(cfg_get(cfg, "Starting_entry_no", "starting_entry_no", default=1))
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


def save_entry_no(cfg: dict, next_entry_no: int) -> None:
    state_file = cfg_get(cfg, "state_file")
    if not state_file:
        return

    os.makedirs(os.path.dirname(state_file), exist_ok=True)
    tmp = state_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"last_entry_no": next_entry_no}, f)
    os.replace(tmp, state_file)


def load_spool_offset(cfg: dict) -> int:
    spool_offset_file = cfg_get(cfg, "spool_offset_file")
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


def save_spool_offset(cfg: dict, offset: int) -> None:
    spool_offset_file = cfg_get(cfg, "spool_offset_file")
    if not spool_offset_file:
        return

    os.makedirs(os.path.dirname(spool_offset_file), exist_ok=True)
    tmp = spool_offset_file + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(str(offset))
    os.replace(tmp, spool_offset_file)


def append_spool(cfg: dict, record: dict) -> None:
    spool_file = cfg_get(cfg, "spool_file")
    if not spool_file:
        return

    os.makedirs(os.path.dirname(spool_file), exist_ok=True)
    line = json.dumps(record, ensure_ascii=False)
    with open(spool_file, "a", encoding="utf-8", buffering=1) as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())


def db_flush_worker(cfg: dict) -> None:
    table = cfg_get(cfg, "table_name", "Table_name")
    if not table:
        raise ValueError("Missing table name: table_name/Table_name")
    quoted_table = _quote_table_name(table)

    flush_interval = float(cfg_get(cfg, "db_flush_interval_sec", "db_save_interval", default=1.0))
    conn = None
    offset = load_spool_offset(cfg)

    insert_sql = f"""
        INSERT INTO {quoted_table}
        (DeviceID, ScannerName, PreferredUser, EntryNo, Barcode, ScanDate, ScanTime, UserID,
         Stowage, FlightNo, OrderDate, DACS_CLASS, Leg, Gally, BlockNo, ContainerCode, DES, DACS_ACType)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    while not stop_event.is_set():
        batch = []
        new_offset = offset

        try:
            if conn is None:
                conn = connect_db(cfg)
                try:
                    created = ensure_table_exists(conn, table)
                    if created:
                        log(cfg, f"Created missing table: {table}")
                except Exception as e:
                    log(cfg, f"Table check/create failed for {table}: {e}")
                log(cfg, "DB connected.")

            time.sleep(flush_interval)

            spool_path = cfg_get(cfg, "spool_file")
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
                save_spool_offset(cfg, offset)
                continue

            cur = conn.cursor()
            try:
                for rec in batch:
                    cur.execute(
                        insert_sql,
                        rec["DeviceID"],
                        rec.get("ScannerName"),
                        rec.get("PreferredUser"),
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
                conn.commit()
            finally:
                try:
                    cur.close()
                except Exception:
                    pass

            offset = new_offset
            save_spool_offset(cfg, offset)
            log(cfg, f"DB flush: inserted {len(batch)} rows. offset={offset}")

        except pyodbc.IntegrityError as e:
            log(cfg, f"DB integrity error: {e}. Trying row-by-row.")
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
                                rec.get("PreferredUser"),
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
                save_spool_offset(cfg, offset)
                log(cfg, f"DB flush row-by-row: inserted {ok}/{len(batch)}. offset={offset}")
            except Exception as e2:
                log(cfg, f"DB row-by-row failed: {e2}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                time.sleep(5)

        except pyodbc.Error as e:
            log(cfg, f"DB error: {e}. Reconnecting in 5s.")
            try:
                conn.rollback()
                conn.close()
            except Exception:
                pass
            conn = None
            time.sleep(5)

        except Exception as e:
            log(cfg, f"DB worker error: {e}")
            time.sleep(5)

    if conn is not None:
        try:
            conn.commit()
            conn.close()
        except Exception:
            pass
