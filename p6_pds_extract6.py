#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
P6 PDS -> OCI (dict-driven config version)

- No CSVs in Object Storage
- Config comes from p6_config.TABLE_CONFIG and p6_config.ENV_VARS
- Designed for OCI Data Flow (Instance Principals) but easy to run locally
"""

import os
import time
import traceback
from typing import Dict, List, Optional

from pyspark.sql import SparkSession

from p6_config import TABLE_CONFIG, ENV_VARS


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def bool_env_from_dict(key: str, default: bool = False) -> bool:
    val = ENV_VARS.get(key)
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("1", "true", "yes", "y")

def int_env_from_dict(key: str, default: int) -> int:
    val = ENV_VARS.get(key)
    if val is None:
        return default
    try:
        return int(val)
    except Exception:
        return default


# ---------------------------------------------------------------------------
# Config derivation from dictionaries
# ---------------------------------------------------------------------------

def derive_table_filters_from_config() -> (List[str], List[str], Optional[str], Dict[str, str]):
    """
    Build:
      - env_whitelist (tables to include)
      - env_blacklist (tables to exclude)
      - global_since (from ENV_VARS["SINCE_DATE"])
      - per_table_since (from TABLE_CONFIG[table]["since_date"])
    """

    # Start from TABLE_CONFIG allow/deny flags
    whitelist = []
    blacklist = []
    per_table_since: Dict[str, str] = {}

    for tname, cfg in TABLE_CONFIG.items():
        allow = bool(cfg.get("allow", False))
        deny = bool(cfg.get("deny", False))
        since = cfg.get("since_date")

        if allow and not deny:
            whitelist.append(tname)
        if deny:
            blacklist.append(tname)
        if since:
            per_table_since[tname] = since

    # Merge with optional explicit whitelist/blacklist from ENV_VARS
    extra_whitelist = [
        x.strip()
        for x in str(ENV_VARS.get("TABLE_WHITELIST", "")).split(",")
        if x.strip()
    ]
    extra_blacklist = [
        x.strip()
        for x in str(ENV_VARS.get("TABLE_BLACKLIST", "")).split(",")
        if x.strip()
    ]

    whitelist.extend(extra_whitelist)
    blacklist.extend(extra_blacklist)

    whitelist = sorted(set(whitelist))
    blacklist = sorted(set(blacklist))

    global_since = ENV_VARS.get("SINCE_DATE")

    print(f"[CFG] whitelist={whitelist}")
    print(f"[CFG] blacklist={blacklist}")
    print(f"[CFG] global_since={global_since}")
    print(f"[CFG] per_table_since={per_table_since}")

    return whitelist, blacklist, global_since, per_table_since


# ---------------------------------------------------------------------------
# Spark helpers
# ---------------------------------------------------------------------------

def create_spark(app_name: str = "P6_PDS_Extract") -> SparkSession:
    print(f"[SPARK] Creating SparkSession: {app_name}")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Main P6 extraction hook (you plug your logic here)
# ---------------------------------------------------------------------------

def run_p6_extract(
    spark: SparkSession,
    env_cfg: Dict[str, object],
    env_whitelist: List[str],
    env_blacklist: List[str],
    global_since: Optional[str],
    per_table_since: Dict[str, str],
) -> None:
    """
    This is where you'd put the real P6 logic.

    Right now it just logs the config so you can confirm the dictionary-driven
    approach is wired correctly.
    """
    print("[RUN] P6 extract configuration summary:")
    print("      ENV_VARS:", env_cfg)
    print("      ENV whitelist:", env_whitelist)
    print("      ENV blacklist:", env_blacklist)
    print("      GLOBAL_SINCE:", global_since)
    print("      per_table_since keys:", list(per_table_since.keys()))
    print("      TABLE_CONFIG tables:", list(TABLE_CONFIG.keys()))

    # TODO: For each table in env_whitelist (minus env_blacklist),
    #       call your P6 REST API or DB layer, apply since dates, write to OCI.

    print("[RUN] P6 extract stub completed (no-op).")


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main():
    print(f"[MAIN] Starting P6 PDS extract at {_now_ts()}")

    # Pull key values from ENV_VARS
    OS_BUCKET = ENV_VARS.get("OS_BUCKET")
    OS_PREFIX_META = ENV_VARS.get("OS_PREFIX_META")
    OS_PREFIX_DATA = ENV_VARS.get("OS_PREFIX_DATA")
    OS_PREFIX_INDEX = ENV_VARS.get("OS_PREFIX_INDEX")

    print("[MAIN] OS_BUCKET       =", OS_BUCKET)
    print("[MAIN] OS_PREFIX_META  =", OS_PREFIX_META)
    print("[MAIN] OS_PREFIX_DATA  =", OS_PREFIX_DATA)
    print("[MAIN] OS_PREFIX_INDEX =", OS_PREFIX_INDEX)

    env_whitelist, env_blacklist, global_since, per_table_since = (
        derive_table_filters_from_config()
    )

    spark = create_spark("P6_PDS_Extract_DictConfig")

    try:
        run_p6_extract(
            spark,
            ENV_VARS,
            env_whitelist,
            env_blacklist,
            global_since,
            per_table_since,
        )
    finally:
        print("[MAIN] Stopping SparkSession")
        spark.stop()

    print(f"[MAIN] Completed P6 PDS extract at {_now_ts()}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[FATAL] Unhandled exception in main:")
        traceback.print_exc()
        raise
