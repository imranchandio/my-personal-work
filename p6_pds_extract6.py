#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
P6 PDS -> OCI Object Storage (PySpark)

- Loads env + table configuration from CSVs stored in Object Storage
- Discovers the OCI namespace at runtime (no hard-coded namespace)
- Ignores namespace in oci:// URIs and uses runtime namespace instead
- Designed to run in OCI Data Flow using Instance Principals
"""

import os
import csv
import json
import time
import traceback
from typing import List, Dict, Tuple, Optional, Any

from pyspark.sql import SparkSession

import oci
from oci.object_storage import ObjectStorageClient

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def bool_env(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y")

def int_env(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default))
    except Exception:
        return int(default)

# ---------------------------------------------------------------------------
# Defaults – can be overridden by Data Flow environment variables
# ---------------------------------------------------------------------------

os.environ.setdefault("OS_BUCKET", "bkt-neom-enowa-des-dev-data-landing")
os.environ.setdefault("OS_PREFIX_META", "common/API/P6/metadata")
os.environ.setdefault("OS_PREFIX_DATA", "common/API/P6/data")
os.environ.setdefault("OS_PREFIX_INDEX", "common/API/P6/logs")

# Primary CSV locations – these *URIs* can be overridden too
os.environ.setdefault(
    "P6_ENV_CSV_OCI_URI",
    "oci://bkt-neom-enowa-des-dev-data-landing@axjj8sdvrg1w/common/config/p6/env_variables.csv",
)
os.environ.setdefault(
    "P6_TABLE_CSV_OCI_URI",
    "oci://bkt-neom-enowa-des-dev-data-landing@axjj8sdvrg1w/common/config/p6/table_config_template.csv",
)

os.environ.setdefault("ENABLE_BUCKET_PRECHECK", "true")
os.environ.setdefault("ENABLE_LOCAL_FALLBACK", "true")


# ---------------------------------------------------------------------------
# OCI auth & client helpers
# ---------------------------------------------------------------------------

def get_oci_signer():
    """Use Instance Principals in Data Flow, fall back to local config (Cloud Shell)."""
    try:
        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        print("[SIGNER] Using Instance Principals signer")
        return signer
    except Exception:
        cfg_path = os.path.expanduser("~/.oci/config")
        try:
            config = oci.config.from_file(cfg_path, "DEFAULT")
            print(f"[SIGNER] Using local config signer from {cfg_path}")
            return oci.signer.Signer(
                tenancy=config["tenancy"],
                user=config["user"],
                fingerprint=config["fingerprint"],
                private_key_file_location=config["key_file"],
                pass_phrase=config.get("pass_phrase"),
            )
        except Exception as e:
            print("[SIGNER][FATAL] Unable to create signer:", e)
            raise

def get_object_storage_client() -> ObjectStorageClient:
    """Create Object Storage client and cache runtime namespace in OS_NAMESPACE."""
    signer = get_oci_signer()
    client = ObjectStorageClient(config={}, signer=signer)
    runtime_ns = client.get_namespace().data
    os.environ["OS_NAMESPACE"] = runtime_ns
    print(f"[SIGNER] Runtime namespace from OCI: {runtime_ns}")
    return client

# ---------------------------------------------------------------------------
# OCI URI helpers
# ---------------------------------------------------------------------------

def is_oci_uri(uri: str) -> bool:
    return uri.strip().lower().startswith("oci://")

def parse_oci_uri(uri: str) -> Tuple[str, str, str]:
    """
    Parse oci://bucket@namespace/path -> (namespace, bucket, object_name)

    NOTE: we will ignore the parsed namespace and use the runtime namespace
    returned by OCI instead (safer when people copy/paste URIs between tenancies).
    """
    if not is_oci_uri(uri):
        raise ValueError(f"Not an OCI URI: {uri}")
    s = uri.strip()[6:]  # strip 'oci://'
    bucket_part, rest = s.split("@", 1)
    namespace, object_name = rest.split("/", 1)
    return namespace, bucket_part, object_name

# ---------------------------------------------------------------------------
# Object Storage helpers
# ---------------------------------------------------------------------------

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
    resp = client.get_object(namespace_name=namespace, bucket_name=bucket, object_name=object_name)
    with open(target_path, "wb") as f:
        for chunk in resp.data.raw.stream(1024 * 1024, decode_content=False):
            if chunk:
                f.write(chunk)
    print(f"[DIAG][OCI] Downloaded to {target_path}")

# ---------------------------------------------------------------------------
# High-level CSV download helper
# ---------------------------------------------------------------------------

def download_single_oci_file_diagnostic_or_local(
    uri: str,
    local_target_dir: str = "/tmp",
    list_on_error: bool = True,
) -> str:
    """
    Download a single OCI object to a local path, with diagnostics.

    - Ignores namespace in the URI
    - Uses runtime namespace from OCI
    - Optional bucket pre-check
    - Optional local-path fallback if URI fails
    """
    if not uri:
        raise RuntimeError("Empty OCI URI provided")

    print(f"[DIAG][OCI] Requested URI: {uri}")
    parsed_ns, bucket, object_name = parse_oci_uri(uri)

    client = get_object_storage_client()
    runtime_ns = os.getenv("OS_NAMESPACE")  # set in get_object_storage_client()
    if parsed_ns != runtime_ns:
        print(f"[WARN] URI namespace '{parsed_ns}' != runtime namespace '{runtime_ns}', using runtime namespace")

    namespace = runtime_ns

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
                prefix = os.path.dirname(object_name)
                print(f"[DIAG][OCI] Listing objects in bucket '{bucket}' prefix '{prefix}'")
                resp = client.list_objects(
                    namespace_name=namespace,
                    bucket_name=bucket,
                    prefix=prefix,
                    fields="name",
                    limit=20,
                )
                for obj in resp.data.objects:
                    print(f"[DIAG][OCI] Found object: {obj.name}")
            except Exception as e2:
                print("[DIAG][OCI][LIST-ERROR] Failed to list objects:", e2)

        if bool_env("ENABLE_LOCAL_FALLBACK", "true"):
            if os.path.exists(uri):
                print(f"[FALLBACK] Using local path: {uri}")
                return uri
            else:
                print(f"[FALLBACK] Local path does not exist: {uri}")

        raise

# ---------------------------------------------------------------------------
# Config loading from CSVs
# ---------------------------------------------------------------------------

def load_config_from_oci_csvs_with_fallback(
    primary_env_oci: str,
    primary_tbl_oci: str,
    fallback_env_oci: Optional[str] = None,
    fallback_tbl_oci: Optional[str] = None,
) -> Tuple[List[str], List[str], Optional[str], Dict[str, str], Dict[str, str]]:
    """
    Download + parse env & table config CSVs.

    Returns:
        env_whitelist, env_blacklist, global_since, per_table_since, env_config
    """

    def try_download(uri: str) -> str:
        return download_single_oci_file_diagnostic_or_local(uri, local_target_dir="/tmp", list_on_error=True)

    # ---- ENV CSV ----
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

    # ---- TABLE CSV ----
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

    # ---- Parse ENV CSV ----
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

    # ---- Parse TABLE CSV ----
    table_config: Dict[str, Dict[str, str]] = {}
    print(f"[CFG] Parsing TABLE CSV: {local_tbl}")
    with open(local_tbl, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            table_name = (row.get("TABLE_NAME") or "").strip()
            if not table_name:
                continue
            table_config[table_name] = {k: (v or "").strip() for k, v in row.items()}

    env_config["TABLE_CONFIG_JSON"] = json.dumps(table_config, default=str)
    print(f"[CFG] Loaded table config rows: {len(table_config)}")

    return env_whitelist, env_blacklist, global_since, per_table_since, env_config

# ---------------------------------------------------------------------------
# Spark + main processing stub
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

def run_p6_extract(
    spark: SparkSession,
    env_cfg: Dict[str, str],
    env_whitelist: List[str],
    env_blacklist: List[str],
    global_since: Optional[str],
    per_table_since: Dict[str, str],
) -> None:
    """
    Hook where you implement the actual P6 extraction.
    Right now it just logs config so you can confirm everything is wired up.
    """
    print("[RUN] P6 extract configuration summary:")
    print("      ENV whitelist:", env_whitelist)
    print("      ENV blacklist:", env_blacklist)
    print("      GLOBAL_SINCE:", global_since)
    print("      per_table_since keys:", list(per_table_since.keys()))
    print("      env_cfg keys:", list(env_cfg.keys()))

    # TODO: use env_cfg["TABLE_CONFIG_JSON"] + PDS base URL, etc.
    print("[RUN] P6 extract stub completed (no-op).")

# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def main():
    print(f"[MAIN] Starting P6 PDS extract at {_now_ts()}")

    OS_BUCKET = os.getenv("OS_BUCKET", "").strip()
    OS_PREFIX_META = os.getenv("OS_PREFIX_META", "").strip()
    OS_PREFIX_DATA = os.getenv("OS_PREFIX_DATA", "").strip()
    OS_PREFIX_INDEX = os.getenv("OS_PREFIX_INDEX", "").strip()

    env_csv_oci = os.getenv("P6_ENV_CSV_OCI_URI", "").strip()
    tbl_csv_oci = os.getenv("P6_TABLE_CSV_OCI_URI", "").strip()
    env_csv_fallback = os.getenv("P6_ENV_CSV_OCI_URI_FALLBACK", "").strip() or None
    tbl_csv_fallback = os.getenv("P6_TABLE_CSV_OCI_URI_FALLBACK", "").strip() or None

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
            "or configured via defaults."
        )

    # Load CSV configs
    env_whitelist, env_blacklist, global_since, per_table_since, env_config = (
        load_config_from_oci_csvs_with_fallback(
            env_csv_oci,
            tbl_csv_oci,
            fallback_env_oci=env_csv_fallback,
            fallback_tbl_oci=tbl_csv_fallback,
        )
    )

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
