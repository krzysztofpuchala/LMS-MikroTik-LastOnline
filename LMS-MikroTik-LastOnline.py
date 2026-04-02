#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# PRO-Admin.pl Puchala Krzysztof
# LastOnline 4 LMS
#

import argparse
import configparser
import concurrent.futures
import gzip
import logging
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# DB
try:
    import pymysql
except ImportError:
    pymysql = None

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    psycopg2 = None

# MikroTik
try:
    from routeros_api import RouterOsApiPool
except ImportError:
    RouterOsApiPool = None


APP_NAME = "LastOnlinePyt"
APP_VERSION = "0.2"
BASE_DIR = Path(__file__).resolve().parent


@dataclass
class DBConfig:
    db_type: str
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class PingConfig:
    binary: str
    count: int
    interface: str


@dataclass
class PortTestConfig:
    mode: str
    timeout: int
    hping_binary: str
    hping_count: int
    hping_interface: str
    netcat_binary: str
    netcat_gateway: str
    ncat_binary: str


@dataclass
class LogsConfig:
    enabled: bool
    log_dir: Path
    what: str
    old_remove_days: Optional[int]


@dataclass
class DeviceConfig:
    section: str
    host: str
    ssl: bool
    port: int
    login: str
    password: str


def yes(v: str) -> bool:
    return str(v).strip().lower() in ("yes", "tak", "true", "1")


def cfg_new() -> configparser.ConfigParser:
    return configparser.ConfigParser(inline_comment_prefixes=(";", "#"))


def cfg_get_clean(cfg: configparser.ConfigParser, section: str, option: str, fallback: str = "") -> str:
    try:
        val = cfg.get(section, option, fallback=fallback)
    except Exception:
        return fallback

    if val is None:
        return fallback

    val = val.strip()

    if len(val) >= 2 and ((val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'")):
        val = val[1:-1].strip()

    return val


def cfg_getint_clean(cfg: configparser.ConfigParser, section: str, option: str, fallback: int = 0) -> int:
    val = cfg_get_clean(cfg, section, option, str(fallback))
    return int(val)


def setup_logging(log_cfg: LogsConfig, quiet: bool = False) -> logging.Logger:
    logger = logging.getLogger(APP_NAME)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")

    if not quiet:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

    if log_cfg.enabled:
        log_cfg.log_dir.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(log_cfg.log_dir / f"{APP_NAME}.log", encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def rotate_and_compress_log(log_path: Path, max_size: int = 1267206) -> None:
    if not log_path.exists():
        return
    if log_path.stat().st_size < max_size:
        return

    idx = 1
    while (log_path.parent / f"{APP_NAME}.{idx}.log.gz").exists():
        idx += 1

    rotated = log_path.parent / f"{APP_NAME}.{idx}.log"
    shutil.move(str(log_path), str(rotated))

    with open(rotated, "rb") as f_in, gzip.open(f"{rotated}.gz", "wb", compresslevel=9) as f_out:
        shutil.copyfileobj(f_in, f_out)

    rotated.unlink(missing_ok=True)


def cleanup_old_logs(log_dir: Path, days: Optional[int], logger: logging.Logger) -> None:
    if not days or days <= 0:
        return

    threshold = time.time() - days * 86400
    removed = 0

    for path in log_dir.glob("*"):
        if path.is_file() and path.stat().st_mtime < threshold:
            path.unlink(missing_ok=True)
            removed += 1

    if removed:
        logger.info("done\tremoved old logs: %s file(s)", removed)


def load_main_config(path: str) -> Tuple[DBConfig, PingConfig, PortTestConfig, LogsConfig]:
    cfg = cfg_new()
    if not cfg.read(path, encoding="utf-8"):
        raise FileNotFoundError(f"Cannot read config: {path}")

    db_type = cfg_get_clean(cfg, "database", "type", "mysql").lower()

    db = DBConfig(
        db_type=db_type,
        host=cfg_get_clean(cfg, "database", "host", "localhost"),
        port=cfg_getint_clean(cfg, "database", "port", 5432 if db_type == "postgres" else 3306),
        user=cfg_get_clean(cfg, "database", "user", "root"),
        password=cfg_get_clean(cfg, "database", "password", ""),
        database=cfg_get_clean(cfg, "database", "database", ""),
    )

    ping = PingConfig(
        binary=cfg_get_clean(cfg, "ping", "binary", "/bin/ping"),
        count=cfg_getint_clean(cfg, "ping", "count", 3),
        interface=cfg_get_clean(cfg, "ping", "interface", ""),
    )

    pt = PortTestConfig(
        mode=cfg_get_clean(cfg, "port-test", "port-test", "socket").lower(),
        timeout=cfg_getint_clean(cfg, "netcat", "timeout", 5),
        hping_binary=cfg_get_clean(cfg, "hping", "binary", "/usr/sbin/hping3"),
        hping_count=cfg_getint_clean(cfg, "hping", "count", 3),
        hping_interface=cfg_get_clean(cfg, "hping", "interface", ""),
        netcat_binary=cfg_get_clean(cfg, "netcat", "binary", "/bin/netcat"),
        netcat_gateway=cfg_get_clean(cfg, "netcat", "gateway", ""),
        ncat_binary=cfg_get_clean(cfg, "ncat", "binary", "/bin/ncat"),
    )

    logs_raw = cfg_get_clean(cfg, "logs", "logs", "no")
    logs_dir_raw = cfg_get_clean(cfg, "logs", "dir", str(BASE_DIR / "log"))
    logs_what = cfg_get_clean(cfg, "logs", "what", "all")
    old_remove_raw = cfg_get_clean(cfg, "logs", "old-remove", "no").lower()

    old_remove_days = int(old_remove_raw) if old_remove_raw.isdigit() else None

    logs = LogsConfig(
        enabled=yes(logs_raw),
        log_dir=Path(logs_dir_raw),
        what=logs_what,
        old_remove_days=old_remove_days,
    )

    if not db.database:
        raise ValueError("database.database cannot be empty")

    return db, ping, pt, logs


def load_devices_config(path: str) -> List[DeviceConfig]:
    cfg = cfg_new()
    if not cfg.read(path, encoding="utf-8"):
        raise FileNotFoundError(f"Cannot read devices config: {path}")

    devices = []
    for section in cfg.sections():
        ssl_raw = cfg_get_clean(cfg, section, "ssl", "no")
        ssl_enabled = yes(ssl_raw)

        devices.append(
            DeviceConfig(
                section=section,
                host=cfg_get_clean(cfg, section, "host", "192.168.88.1"),
                ssl=ssl_enabled,
                port=cfg_getint_clean(cfg, section, "port", 8729 if ssl_enabled else 8728),
                login=cfg_get_clean(cfg, section, "login", "admin"),
                password=cfg_get_clean(cfg, section, "password", ""),
            )
        )
    return devices


def db_connect(db_cfg: DBConfig):
    if db_cfg.db_type == "mysql":
        if pymysql is None:
            raise RuntimeError("Brak modułu pymysql. Zainstaluj: pip install pymysql")
        conn = pymysql.connect(
            host=db_cfg.host,
            port=db_cfg.port,
            user=db_cfg.user,
            password=db_cfg.password,
            database=db_cfg.database,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
        return conn

    if db_cfg.db_type == "postgres":
        if psycopg2 is None:
            raise RuntimeError("Brak modułu psycopg2-binary. Zainstaluj: pip install psycopg2-binary")
        conn = psycopg2.connect(
            host=db_cfg.host,
            port=db_cfg.port,
            user=db_cfg.user,
            password=db_cfg.password,
            dbname=db_cfg.database,
        )
        conn.autocommit = True
        return conn

    raise ValueError(f"Unsupported DB type: {db_cfg.db_type}")


def db_fetchone(conn, sql: str, params=None):
    if psycopg2 is not None and isinstance(conn, psycopg2.extensions.connection):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    else:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()


def db_fetchall(conn, sql: str, params=None):
    if psycopg2 is not None and isinstance(conn, psycopg2.extensions.connection):
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    else:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def db_execute(conn, sql: str, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)


def detect_customers_view(conn, db_type: str) -> str:
    if db_type == "mysql":
        row = db_fetchone(conn, "SHOW TABLES LIKE 'customersview'")
        return "customersview" if row else "customerview"

    row = db_fetchone(conn, "SELECT viewname FROM pg_views WHERE viewname='customersview'")
    return "customersview" if row else "customerview"


def build_node_maps(conn, db_type: str) -> Tuple[Dict[str, int], Dict[str, int]]:
    nodes_key = {}
    netdevices_key = {}

    if db_type == "mysql":
        sql_nodes_loc = "SELECT id, CONCAT(INET_NTOA(ipaddr), mac) AS keyx FROM vmacs WHERE ownerid > 0"
        sql_nodes_pub = "SELECT id, CONCAT(INET_NTOA(ipaddr_pub), mac) AS keyx FROM vmacs WHERE ownerid > 0"
        sql_net_loc = "SELECT id, CONCAT(ipaddr, mac) AS keyx FROM vmacs WHERE ownerid = 0"
        sql_net_pub = "SELECT id, CONCAT(ipaddr_pub, mac) AS keyx FROM vmacs WHERE ownerid = 0"
    else:
        sql_nodes_loc = "SELECT id, CONCAT(ipaddr::text, mac) AS keyx FROM vmacs WHERE ownerid > 0"
        sql_nodes_pub = "SELECT id, CONCAT(ipaddr_pub::text, mac) AS keyx FROM vmacs WHERE ownerid > 0"
        sql_net_loc = "SELECT id, CONCAT(ipaddr::text, mac) AS keyx FROM vmacs WHERE ownerid = 0"
        sql_net_pub = "SELECT id, CONCAT(ipaddr_pub::text, mac) AS keyx FROM vmacs WHERE ownerid = 0"

    for row in db_fetchall(conn, sql_nodes_loc):
        nodes_key[str(row["keyx"])] = int(row["id"])
    for row in db_fetchall(conn, sql_nodes_pub):
        nodes_key[str(row["keyx"])] = int(row["id"])
    for row in db_fetchall(conn, sql_net_loc):
        netdevices_key[str(row["keyx"])] = int(row["id"])
    for row in db_fetchall(conn, sql_net_pub):
        netdevices_key[str(row["keyx"])] = int(row["id"])

    return nodes_key, netdevices_key


def update_lastonline(conn, db_type: str, node_ids: List[int], ts: int) -> None:
    if not node_ids:
        return

    placeholders = ",".join(["%s"] * len(node_ids))
    sql = f"UPDATE nodes SET lastonline=%s WHERE id IN ({placeholders})"
    params = [ts] + node_ids
    db_execute(conn, sql, params)


def ping_host(binary: str, count: int, interface: str, host: str) -> Optional[float]:
    cmd = [binary, "-c", str(count), "-w", "1"]
    if interface:
        cmd += ["-I", interface]
    cmd.append(host)

    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=10)
    except Exception:
        return None

    for line in out.splitlines():
        if "time=" in line:
            try:
                frag = line.split("time=")[1].split()[0]
                if frag.endswith("ms"):
                    frag = frag[:-2]
                return float(frag)
            except Exception:
                pass

    return None


def test_tcp_port(host: str, port: int, timeout: int = 5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def mikrotik_login(host: str, port: int, username: str, password: str, use_ssl: bool):
    if RouterOsApiPool is None:
        raise RuntimeError("Brak modułu routeros-api. Zainstaluj: pip install routeros-api")

    pool = RouterOsApiPool(
        host=host,
        username=username,
        password=password,
        port=port,
        use_ssl=use_ssl,
        ssl_verify=False,
        plaintext_login=True,
    )
    api = pool.get_api()
    return pool, api


def mikrotik_get_dynamic_complete_arp(api) -> List[dict]:
    resource = api.get_resource("/ip/arp")
    rows = resource.get(dynamic="true", complete="yes")
    return rows


def process_device(
    device: DeviceConfig,
    db_cfg: DBConfig,
    ping_cfg: PingConfig,
    port_cfg: PortTestConfig,
    nodes_key: Dict[str, int],
    netdevices_key: Dict[str, int],
    quiet: bool = False,
) -> Tuple[str, bool, List[int]]:
    if not quiet:
        print(f"\n  Logowanie do {device.host}...")

    latency = ping_host(ping_cfg.binary, ping_cfg.count, ping_cfg.interface, device.host)
    if latency is None:
        if not quiet:
            print("   - ping               [błąd: host nieosiągalny]")
        return device.host, False, []

    if not quiet:
        print(f"   - ping               [{latency:.3f}ms]")

    if not test_tcp_port(device.host, device.port, timeout=port_cfg.timeout):
        if not quiet:
            print(f"   - stan portu API     [błąd: port {device.port} zamknięty]")
        return device.host, False, []

    if not quiet:
        print(f"   - stan portu API     [otwarty: {device.port}]")

    try:
        pool, api = mikrotik_login(
            device.host,
            device.port,
            device.login,
            device.password,
            device.ssl,
        )
    except Exception as e:
        if not quiet:
            print(f"   - logowanie API      [błąd: {e}]")
        return device.host, False, []

    online_ids: List[int] = []

    try:
        arp_rows = mikrotik_get_dynamic_complete_arp(api)
        now_ts = int(time.time())

        for row in arp_rows:
            ip = str(row.get("address", "")).strip()
            mac = str(row.get("mac-address", "")).strip()

            if not ip or not mac:
                continue

            key = f"{ip}{mac}"

            node_id = nodes_key.get(key)
            if node_id:
                online_ids.append(node_id)
                if not quiet:
                    print(f"\t{node_id:04d}\t{mac}\t{ip}")

            netdev_id = netdevices_key.get(key)
            if netdev_id:
                if not quiet:
                    print(f"\t{netdev_id:04d}\t{mac}\t{ip}")

        if online_ids:
            conn = db_connect(db_cfg)
            try:
                update_lastonline(conn, db_cfg.db_type, sorted(set(online_ids)), now_ts)
            finally:
                conn.close()

        return device.host, True, sorted(set(online_ids))

    except Exception as e:
        if not quiet:
            print(f"   - pobranie ARP       [błąd: {e}]")
        return device.host, False, []

    finally:
        try:
            pool.disconnect()
        except Exception:
            pass


def validate_environment(db_cfg: DBConfig, ping_cfg: PingConfig, port_cfg: PortTestConfig) -> None:
    if not Path(ping_cfg.binary).exists():
        raise FileNotFoundError(f"Ping binary not found: {ping_cfg.binary}")

    if ping_cfg.count <= 0:
        raise ValueError("ping.count must be > 0")

    if port_cfg.mode not in ("hping", "hping3", "netcat", "ncat", "socket"):
        raise ValueError("port-test.port-test must be one of: hping, hping3, netcat, ncat, socket")

    if db_cfg.db_type == "mysql" and pymysql is None:
        raise RuntimeError("Brak pymysql")

    if db_cfg.db_type == "postgres" and psycopg2 is None:
        raise RuntimeError("Brak psycopg2-binary")

    if RouterOsApiPool is None:
        raise RuntimeError("Brak routeros-api")


def parse_args():
    parser = argparse.ArgumentParser(description=APP_NAME)
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument("-C", "--config-script", default=str(BASE_DIR / "config.ini"))
    parser.add_argument("-D", "--config-devices", default=str(BASE_DIR / "devices.ini"))
    parser.add_argument("-m", "--multitasking", type=int, default=1)
    parser.add_argument("-np", "--no-run-if-it-run-script", action="store_true")
    parser.add_argument("-v", "--version", action="store_true")
    return parser.parse_args()


def already_running(script_name: str) -> bool:
    try:
        out = subprocess.check_output(["ps", "-eo", "pid,cmd"], text=True)
    except Exception:
        return False

    current_pid = os.getpid()
    for line in out.splitlines():
        if script_name in line and "grep" not in line:
            parts = line.strip().split(maxsplit=1)
            if not parts:
                continue
            try:
                pid = int(parts[0])
            except ValueError:
                continue
            if pid != current_pid:
                return True
    return False


def main():
    args = parse_args()

    if args.version:
        print(f"{APP_NAME} {APP_VERSION}")
        return 0

    if args.no_run_if_it_run_script and already_running(Path(sys.argv[0]).name):
        return 0

    db_cfg, ping_cfg, port_cfg, logs_cfg = load_main_config(args.config_script)
    devices = load_devices_config(args.config_devices)
    logger = setup_logging(logs_cfg, quiet=args.quiet)

    validate_environment(db_cfg, ping_cfg, port_cfg)

    if not args.quiet:
        print(f" {APP_NAME} v{APP_VERSION}")
        print("   CTRL+C - anuluj")
        print()

    logger.info("done\tstart %s", APP_VERSION)

    conn = db_connect(db_cfg)
    try:
        detect_customers_view(conn, db_cfg.db_type)
        nodes_key, netdevices_key = build_node_maps(conn, db_cfg.db_type)
    finally:
        conn.close()

    workers = max(1, args.multitasking)

    ok_count = 0
    err_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                process_device,
                dev,
                db_cfg,
                ping_cfg,
                port_cfg,
                nodes_key,
                netdevices_key,
                args.quiet,
            )
            for dev in devices
        ]

        for fut in concurrent.futures.as_completed(futures):
            host, success, online_ids = fut.result()
            if success:
                ok_count += 1
                logger.info("done\t[%s]\tupdated lastonline for %s node(s)", host, len(online_ids))
            else:
                err_count += 1
                logger.error("error\t[%s]\tdevice processing failed", host)

    if logs_cfg.enabled:
        rotate_and_compress_log(logs_cfg.log_dir / f"{APP_NAME}.log")
        cleanup_old_logs(logs_cfg.log_dir, logs_cfg.old_remove_days, logger)

    logger.info("done\tfinished ok=%s error=%s", ok_count, err_count)
    return 0 if err_count == 0 else 1


if __name__ == "__main__":
    signal.signal(signal.SIGCHLD, signal.SIG_IGN)
    sys.exit(main())

