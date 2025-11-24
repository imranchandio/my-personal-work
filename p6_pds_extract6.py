#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
P6 PDS -> OCI Object Storage (PySpark)
CSV-only pipeline with diagnostics, pre-checks, local-file fallback, hardened HTTP, header validation,
heuristic downloads and single-file uploads for index/audit/perf/config/monitor.
"""
import os
import json
import time
import ssl
import traceback
import csv
import tempfile
from urllib import request
from base64 import b64encode, b64decode
from typing import List, Dict, Any, Tuple, Optional

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    regexp_replace,
    trim,
    upper,
    lower,
    concat_ws,
    when,
    input_file_name,
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# OCI imports
import oci
from oci.object_storage import ObjectStorageClient

# ----------------- Environment defaults -----------------
# These defaults are used when running in OCI Data Flow unless overridden
# by environment variables set on the application/run.

# Object Storage namespace & bucket
os.environ.setdefault("OS_NAMESPACE", "axjj8sdvrg1w")
os.environ.setdefault("OS_BUCKET", "bkt-neom-enowa-des-dev-data-landing")
os.environ.setdefault("OS_PREFIX_META", "common/API/P6/metadata")
os.environ.setdefault("OS_PREFIX_DATA", "common/API/P6/data")
os.environ.setdefault("OS_PREFIX_INDEX", "common/API/P6/logs")

# P6 configuration
os.environ.setdefault("PDS_BASE", "https://ksa1.p6.oraclecloud.com/neom/uat")
os.environ.setdefault("PDS_CONFIG_CODE", "ds_p6adminuser")

# General behaviour
os.environ.setdefault("WRITE_MODE", "overwrite")
os.environ.setdefault("PAGE_SIZE", "1000")
os.environ.setdefault("MAX_PAGES", "0")  # 0 = no limit
os.environ.setdefault("FAIL_FAST", "true")
os.environ.setdefault("ENABLE_BUCKET_PRECHECK", "true")
os.environ.setdefault("ENABLE_HEADER_VALIDATION", "true")
os.environ.setdefault("ENABLE_LOCAL_FALLBACK", "true")
os.environ.setdefault("ENABLE_PERF_LOG", "true")
os.environ.setdefault("ENABLE_CONFIG_LOG", "true")
os.environ.setdefault("ENABLE_AUDIT_LOG", "true")
os.environ.setdefault("ENABLE_MONITOR_LOG", "true")
os.environ.setdefault("ENABLE_INDEX_UPLOAD", "true")
os.environ.setdefault("ENABLE_P6_AUTH_VAULT", "false")
os.environ.setdefault("PROCESS_ALL_TABLES", "false")
os.environ.setdefault("ENABLE_VERSIONED_PATHS", "true")

# Default primary CSV URIs (useful when env vars are not externally set)
os.environ.setdefault(
    "P6_ENV_CSV_OCI_URI",
    "oci://bkt-neom-enowa-des-dev-data-landing@axjj8sdvrg1w/common/config/p6/env_variables.csv",
)
os.environ.setdefault(
    "P6_TABLE_CSV_OCI_URI",
    "oci://bkt-neom-enowa-des-dev-data-landing@axjj8sdvrg1w/common/config/p6/table_config_template.csv",
)

# ----------------- Helpers -----------------
def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def _mask(val: str, show: int = 4) -> str:
    if not val:
        return "<empty>"
    if len(val) <= show:
        return "*" * len(val)
    return "*" * (len(val) - show) + val[-show:]

def bool_env(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y")

def int_env(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return int(default)

# ----------------- URI & HTTP helpers -----------------
def is_oci_uri(uri: str) -> bool:
    return uri.strip().lower().startswith("oci://")

def parse_oci_uri(uri: str) -> Tuple[str, str, str]:
    if not is_oci_uri(uri):
        raise ValueError(f"Not an OCI URI: {uri}")
    s = uri.strip()[6:]
    bucket_part, rest = s.split("@", 1)
    namespace, object_name = rest.split("/", 1)
    return namespace, bucket_part, object_name

def hardened_https_opener() -> request.OpenerDirector:
    ctx = ssl.create_default_context()
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    https_handler = request.HTTPSHandler(context=ctx)
    opener = request.build_opener(https_handler)
    return opener

def http_get_json(url: str, headers: Dict[str, str] = None, timeout: int = 30) -> Any:
    if headers is None:
        headers = {}
    opener = hardened_https_opener()
    req = request.Request(url, headers=headers, method="GET")
    with opener.open(req, timeout=timeout) as resp:
        payload = resp.read().decode("utf-8")
        return json.loads(payload)

# ----------------- OCI signer & identity helpers -----------------
def get_oci_signer() -> Any:
    try:
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        print("[SIGNER] Using Instance Principals signer")
        return signer
    except Exception:
        cfg = os.path.expanduser("~/.oci/config")
        try:
            config = oci.config.from_file(cfg, "DEFAULT")
            print(f"[SIGNER] Using local config signer from {cfg}")
            return oci.signer.Signer(
                tenancy=config["tenancy"],
                user=config["user"],
                fingerprint=config["fingerprint"],
                private_key_file_location=config["key_file"],
                pass_phrase=config.get("pass_phrase"),
            )
        except Exception as e:
            print("[SIGNER][FATAL] Failed to create signer:", e)
            raise

def get_object_storage_client() -> ObjectStorageClient:
    signer = get_oci_signer()
    return ObjectStorageClient(config={}, signer=signer)

# ----------------- Object Storage helpers -----------------
def check_bucket_access(namespace: str, bucket: str, client: ObjectStorageClient) -> Tuple[bool, Optional[str]]:
    try:
        client.get_bucket(namespace_name=namespace, bucket_name=bucket)
        return True, None
    except Exception as e:
        try:
            import oci.exceptions as oci_exc

            if isinstance(e, oci_exc.ServiceError):
                msg = {
                    "status": getattr(e, "status", None),
                    "code": getattr(e, "code", None),
                    "message": getattr(e, "message", None),
                    "request_endpoint": getattr(e, "request_endpoint", None),
                    "opc_request_id": getattr(e, "opc_request_id", None),
                }
                return False, json.dumps(msg, default=str)
        except Exception:
            pass
        return False, str(e)

def download_oci_object_to_local(
    namespace: str,
    bucket: str,
    object_name: str,
    target_path: str,
    client: ObjectStorageClient,
) -> None:
    print(f"[DIAG][OCI] namespace={namespace} bucket={bucket} object_name={object_name}")
    resp = client.get_object(
        namespace_name=namespace, bucket_name=bucket, object_name=object_name
    )
    with open(target_path, "wb") as f:
        for chunk in resp.data.raw.stream(1024 * 1024, decode_content=False):
            if chunk:
                f.write(chunk)
    print(f"[DIAG][OCI] Downloaded to {target_path}")

def upload_local_file_to_oci(
    namespace: str,
    bucket: str,
    object_name: str,
    local_path: str,
    client: ObjectStorageClient,
    content_type: str = "text/csv",
) -> None:
    print(f"[UPLOAD] namespace={namespace} bucket={bucket} object={object_name} file={local_path}")
    with open(local_path, "rb") as f:
        data = f.read()
    client.put_object(
        namespace_name=namespace,
        bucket_name=bucket,
        object_name=object_name,
        put_object_body=data,
        content_type=content_type,
    )
    print("[UPLOAD] Completed")

# ----------------- CSV config download with diagnostics -----------------
def download_single_oci_file_diagnostic_or_local(
    uri: str,
    local_target_dir: str = "/tmp",
    list_on_error: bool = True,
) -> str:
    """
    Download a single OCI URI to a local temp path, with diagnostics on failure.
    If download fails and ENABLE_LOCAL_FALLBACK is true, attempts to interpret uri as a local path.
    """
    if not uri:
        raise RuntimeError("Empty OCI URI provided")

    print(f"[DIAG][OCI] Requested URI: {uri}")
    namespace, bucket, object_name = parse_oci_uri(uri)
    client = get_object_storage_client()

    if bool_env("ENABLE_BUCKET_PRECHECK", "true"):
        ok, err = check_bucket_access(namespace, bucket, client)
        if not ok:
            friendly = (
                f"Bucket access check failed for bucket '{bucket}' in namespace '{namespace}'. "
                f"Details: {err}. Ensure bucket exists, namespace is correct, and caller has OBJECT_READ permission."
            )
            print("[DIAG][OCI][PRE-CHECK] " + friendly)
            raise RuntimeError(friendly)

    local_path = os.path.join(local_target_dir, os.path.basename(object_name))
    try:
        download_oci_object_to_local(namespace, bucket, object_name, local_path, client)
        return local_path
    except Exception as e:
        print("[DIAG][OCI][ERROR] Failed to download object:", e)
        if list_on_error:
            try:
                print(f"[DIAG][OCI] Listing objects in bucket '{bucket}' prefix '{os.path.dirname(object_name)}'")
                resp = client.list_objects(
                    namespace_name=namespace,
                    bucket_name=bucket,
                    prefix=os.path.dirname(object_name),
                    fields="name",
                    limit=20,
                )
                for obj in resp.data.objects:
                    print(f"[DIAG][OCI] Found object: {obj.name}")
            except Exception as e2:
                print("[DIAG][OCI][LIST-ERROR] Failed to list objects:", e2)

        if bool_env("ENABLE_LOCAL_FALLBACK", "true"):
            print("[FALLBACK] ENABLE_LOCAL_FALLBACK is true; trying local path interpretation")
            if os.path.exists(uri):
                print(f"[FALLBACK] Using local path: {uri}")
                return uri
            else:
                print(f"[FALLBACK] Local path does not exist: {uri}")

        raise

def load_config_from_oci_csvs_with_fallback(
    primary_env_oci: str,
    primary_tbl_oci: str,
    fallback_env_oci: Optional[str] = None,
    fallback_tbl_oci: Optional[str] = None,
) -> Tuple[List[str], List[str], Optional[str], Dict[str, str], Dict[str, str]]:
    """
    Download env + table config CSVs, with optional fallback URIs, returning:
    - env_whitelist
    - env_blacklist
    - global_since
    - per_table_since
    - env_config dict
    """

    def try_download(uri: str) -> str:
        return download_single_oci_file_diagnostic_or_local(uri, local_target_dir="/tmp", list_on_error=True)

    # --- ENV CSV ---
    print(f"[CFG] Attempting primary env CSV: {primary_env_oci}")
    try:
        local_env = try_download(primary_env_oci)
    except Exception as e_primary_env:
        print("[CFG][WARN] Primary env CSV download failed:", e_primary_env)
        if fallback_env_oci:
            print(f"[CFG] Attempting fallback env CSV: {fallback_env_oci}")
            try:
                local_env = try_download(fallback_env_oci)
            except Exception as e_fallback_env:
                print("[CFG][FATAL] Fallback env CSV download failed:", e_fallback_env)
                raise RuntimeError(f"Failed to download env CSV from primary URI: {e_primary_env}")
        else:
            raise RuntimeError(f"Failed to download env CSV from primary URI: {e_primary_env}")

    # --- TABLE CSV ---
    print(f"[CFG] Attempting primary table CSV: {primary_tbl_oci}")
    try:
        local_tbl = try_download(primary_tbl_oci)
    except Exception as e_primary_tbl:
        print("[CFG][WARN] Primary table CSV download failed:", e_primary_tbl)
        if fallback_tbl_oci:
            print(f"[CFG] Attempting fallback table CSV: {fallback_tbl_oci}")
            try:
                local_tbl = try_download(fallback_tbl_oci)
            except Exception as e_fallback_tbl:
                print("[CFG][FATAL] Fallback table CSV download failed:", e_fallback_tbl)
                raise RuntimeError(f"Failed to download table CSV from primary URI: {e_primary_tbl}")
        else:
            raise RuntimeError(f"Failed to download table CSV from primary URI: {e_primary_tbl}")

    # --- Parse ENV CSV ---
    env_whitelist: List[str] = []
    env_blacklist: List[str] = []
    global_since: Optional[str] = None
    per_table_since: Dict[str, str] = {}
    env_config: Dict[str, str] = {}

    print(f"[CFG] Parsing ENV CSV: {local_env}")
    with open(local_env, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row.get("key") or "").strip()
            value = (row.get("value") or "").strip()
            if not key:
                continue
            env_config[key] = value

            if key == "ENV_WHITELIST":
                env_whitelist = [p.strip() for p in value.split(",") if p.strip()]
            elif key == "ENV_BLACKLIST":
                env_blacklist = [p.strip() for p in value.split(",") if p.strip()]
            elif key == "GLOBAL_SINCE":
                global_since = value
            elif key.startswith("TABLE_SINCE_"):
                table_name = key[len("TABLE_SINCE_") :].strip()
                if table_name and value:
                    per_table_since[table_name] = value

    print(f"[CFG] ENV whitelist: {env_whitelist}")
    print(f"[CFG] ENV blacklist: {env_blacklist}")
    print(f"[CFG] GLOBAL_SINCE: {global_since}")
    print(f"[CFG] per_table_since entries: {len(per_table_since)}")

    # --- Parse TABLE CSV ---
    table_config: Dict[str, str] = {}
    print(f"[CFG] Parsing TABLE CSV: {local_tbl}")
    with open(local_tbl, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            table_name = (row.get("TABLE_NAME") or "").strip()
            if not table_name:
                continue
            table_config[table_name] = json.dumps(row, default=str)

    print(f"[CFG] Loaded table config rows: {len(table_config)}")
    env_config["TABLE_CONFIG_JSON"] = json.dumps(table_config, default=str)

    return env_whitelist, env_blacklist, global_since, per_table_since, env_config

# ----------------- Spark helpers -----------------
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

# ----------------- Main ETL logic -----------------
def run_p6_extract(
    spark: SparkSession,
    env_cfg: Dict[str, str],
    env_whitelist: List[str],
    env_blacklist: List[str],
    global_since: Optional[str],
    per_table_since: Dict[str, str],
) -> None:
    """
    Placeholder for the actual P6 extraction logic.
    At the moment this is just a stub with logging hooks so you can plug your
    real table-by-table extraction using env_cfg + since parameters.
    """
    print("[RUN] P6 extract configuration summary:")
    print("      ENV whitelist:", env_whitelist)
    print("      ENV blacklist:", env_blacklist)
    print("      GLOBAL_SINCE:", global_since)
    print("      per_table_since keys:", list(per_table_since.keys()))
    print("      SAMPLE env_cfg keys:", list(env_cfg.keys())[:20])

    # You would implement the actual extraction logic here, e.g.:
    # - iterate over table_config
    # - perform HTTP calls to P6 PDS (using env_cfg credentials / base URL)
    # - transform the data into DataFrames
    # - write them out to Object Storage under OS_BUCKET / prefixes

    print("[RUN] P6 extract stub completed (no-op).")

# ----------------- Main entrypoint -----------------
def main():
    print(f"[MAIN] Starting P6 PDS extract at {_now_ts()}")

    OS_NAMESPACE = os.getenv("OS_NAMESPACE", "").strip()
    OS_BUCKET = os.getenv("OS_BUCKET", "").strip()
    OS_PREFIX_META = os.getenv("OS_PREFIX_META", "").strip()
    OS_PREFIX_DATA = os.getenv("OS_PREFIX_DATA", "").strip()
    OS_PREFIX_INDEX = os.getenv("OS_PREFIX_INDEX", "").strip()

    env_csv_oci = os.getenv("P6_ENV_CSV_OCI_URI", "").strip()
    tbl_csv_oci = os.getenv("P6_TABLE_CSV_OCI_URI", "").strip()
    env_csv_fallback = os.getenv("P6_ENV_CSV_OCI_URI_FALLBACK", "").strip() or None
    tbl_csv_fallback = os.getenv("P6_TABLE_CSV_OCI_URI_FALLBACK", "").strip() or None

    print("[MAIN] OS_NAMESPACE      =", OS_NAMESPACE)
    print("[MAIN] OS_BUCKET         =", OS_BUCKET)
    print("[MAIN] OS_PREFIX_META    =", OS_PREFIX_META)
    print("[MAIN] OS_PREFIX_DATA    =", OS_PREFIX_DATA)
    print("[MAIN] OS_PREFIX_INDEX   =", OS_PREFIX_INDEX)
    print("[MAIN] P6_ENV_CSV_OCI_URI        =", env_csv_oci)
    print("[MAIN] P6_TABLE_CSV_OCI_URI      =", tbl_csv_oci)
    print("[MAIN] P6_ENV_CSV_OCI_URI_FALLBACK   =", env_csv_fallback)
    print("[MAIN] P6_TABLE_CSV_OCI_URI_FALLBACK =", tbl_csv_fallback)

    if not env_csv_oci or not tbl_csv_oci:
        raise RuntimeError(
            "Both P6_ENV_CSV_OCI_URI and P6_TABLE_CSV_OCI_URI must be set to OCI URIs "
            "or configured via defaults / fallbacks. Check your Data Flow environment variables "
            "or ensure the script defaults are correct."
        )

    try:
        env_whitelist, env_blacklist, global_since, per_table_since, env_config = load_config_from_oci_csvs_with_fallback(
            env_csv_oci, tbl_csv_oci, fallback_env_oci=env_csv_fallback, fallback_tbl_oci=tbl_csv_fallback
        )
    except Exception as e:
        print("[CFG][FATAL] Failed to load CSV configs:", e)
        raise

    spark = create_spark("P6_PDS_Extract")

    try:
        run_p6_extract(
            spark,
            env_config,
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
