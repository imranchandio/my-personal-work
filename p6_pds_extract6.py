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

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

import oci

# ----------------- Defaults and flags -----------------
os.environ.setdefault("OS_NAMESPACE", "axjj8sdvrg1w")
os.environ.setdefault("OS_BUCKET", "bkt-neom-enowa-des-dev-data-landing")
os.environ.setdefault("OS_PREFIX_META", "common/API/P6/metadata")
os.environ.setdefault("OS_PREFIX_DATA", "common/API/P6/data")
os.environ.setdefault("OS_PREFIX_INDEX", "common/API/P6/logs")

os.environ.setdefault("PDS_BASE", "https://ksa1.p6.oraclecloud.com/neom/uat")
os.environ.setdefault("PDS_CONFIG_CODE", "ds_p6adminuser")

os.environ.setdefault("WRITE_MODE", "overwrite")
os.environ.setdefault("PAGE_SIZE", "5000")
os.environ.setdefault("MAX_PAGES_PER_TABLE", "1000")
os.environ.setdefault("MAX_RETRIES_HTTP", "5")
os.environ.setdefault("RETRY_BACKOFF_SEC", "5")
os.environ.setdefault("SPARK_LOG_LEVEL", "WARN")

VAULT_ID = os.getenv("VAULT_ID", "")
SECRET_NAME_USER = os.getenv("SECRET_NAME_USER", "sc-des-secret-dev-username")
SECRET_NAME_PASS = os.getenv("SECRET_NAME_PASS", "sc-des-secret-dev-user-passwd")

os.environ.setdefault("ENABLE_META_COLUMNS", "true")
os.environ.setdefault("ENABLE_RUNQUERY", "true")
os.environ.setdefault("ENABLE_INDEX_CSV", "true")
os.environ.setdefault("ENABLE_ERROR_AUDIT", "true")
os.environ.setdefault("ENABLE_PERF_LOG", "true")
os.environ.setdefault("ENABLE_NORMALIZED_ROWS", "true")
os.environ.setdefault("ENABLE_P6_AUTH_VAULT", "true")
os.environ.setdefault("PROCESS_ALL_TABLES", "false")
os.environ.setdefault("ENABLE_VERSIONED_PATHS", "true")

# Default primary CSV URIs (useful when env vars are not externally set)
os.environ.setdefault("P6_ENV_CSV_OCI_URI", "oci://bkt-neom-enowa-des-dev-data-landing@axjj8sdvrg1w/common/config/p6/env_variables.csv")
os.environ.setdefault("P6_TABLE_CSV_OCI_URI", "oci://bkt-neom-enowa-des-dev-data-landing@axjj8sdvrg1w/common/config/p6/table_config_template.csv")

# ----------------- Helpers -----------------
def _now_ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

def _mask(val: str, show: int = 4) -> str:
    if not val:
        return ""
    if len(val) <= show:
        return "*" * len(val)
    return val[:show] + "*" * (len(val) - show)

def _bool_env(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y")

def _bool_str(val: str) -> bool:
    return str(val).strip().lower() in ("1", "true", "yes", "y")

def _int_env(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)

def setup_spark(app_name: str = "p6_pds_to_oci_alltables_single") -> SparkSession:
    builder = SparkSession.builder.appName(app_name).config("spark.sql.session.timeZone", "UTC")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark

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
            print(f"[SIGNER][ERROR] Failed to build signer from {cfg}: {e}")
            raise

def build_uri(bucket: str, namespace: str, *parts: str) -> str:
    path = "/".join([p.strip("/") for p in parts if p])
    return f"oci://{bucket}@{namespace}/{path}"

def is_oci_uri(path: str) -> bool:
    return isinstance(path, str) and path.strip().lower().startswith("oci://")

def parse_oci_uri(uri: str) -> Tuple[str, str, str]:
    if not is_oci_uri(uri):
        raise ValueError(f"Not an OCI URI: {uri}")
    s = uri.strip()[6:]
    bucket_part, rest = s.split("@", 1)
    namespace, object_name = rest.split("/", 1)
    return namespace, bucket_part, object_name

# ----------------- Vault helpers -----------------
def get_vault_client() -> oci.secrets.SecretsClient:
    signer = get_oci_signer()
    return oci.secrets.SecretsClient(config={}, signer=signer)

def get_secret_from_vault(vault_id: str, secret_name: str) -> str:
    client = get_vault_client()
    resp = client.get_secret_bundle_by_name(vault_id=vault_id, secret_name=secret_name, stage="CURRENT")
    secret_content = resp.data.secret_bundle_content
    b64 = getattr(secret_content, "content", None)
    if not b64:
        raise RuntimeError("Unexpected secret bundle content format")
    return b64decode(b64).decode("utf-8")

# ----------------- Bucket pre-check -----------------
def check_bucket_access(namespace: str, bucket: str, client: oci.object_storage.ObjectStorageClient) -> Tuple[bool, Optional[str]]:
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

# ----------------- Single-file upload helper -----------------
def upload_single_text_to_oci(client: oci.object_storage.ObjectStorageClient, namespace: str, bucket: str, object_path: str, text: str):
    tmp = tempfile.NamedTemporaryFile(delete=False)
    try:
        tmp.write(text.encode("utf-8"))
        tmp.flush()
        tmp.close()
        with open(tmp.name, "rb") as fh:
            client.put_object(namespace_name=namespace, bucket_name=bucket, object_name=object_path, put_object_body=fh)
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass

# ----------------- Diagnostic download + heuristic fallback + local-file fallback -----------------
def download_single_oci_file_diagnostic_or_local(uri: str, local_target_dir: str = "/tmp", list_on_error: bool = True) -> str:
    """
    If uri is a local path (starts with / or file://), return it directly (copy to /tmp if needed).
    Otherwise attempt OCI get_object with diagnostics. On bucket not found/permission errors, raise
    descriptive errors so operator can act.
    """
    if not uri:
        raise ValueError("No URI provided")
    # local-file fallback
    if uri.startswith("file://"):
        local_path = uri[7:]
        if not os.path.exists(local_path):
            raise RuntimeError(f"Local file not found: {local_path}")
        return local_path
    if uri.startswith("/"):
        if not os.path.exists(uri):
            raise RuntimeError(f"Local file not found: {uri}")
        return uri

    # OCI path
    print(f"[DIAG][OCI] Requested URI: {uri}")
    namespace, bucket, object_name = parse_oci_uri(uri)
    print(f"[DIAG][OCI] namespace={namespace} bucket={bucket} object_name={object_name}")
    signer = get_oci_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)

    # pre-check bucket
    ok, details = check_bucket_access(namespace, bucket, client)
    if not ok:
        friendly = (
            f"Bucket access check failed for bucket '{bucket}' in namespace '{namespace}'. "
            f"Details: {details}. Ensure bucket exists, namespace is correct, and caller has OBJECT_READ permission."
        )
        print(f"[DIAG][OCI][PRE-CHECK] {friendly}")
        raise RuntimeError(friendly)

    filename = os.path.basename(object_name)
    local_path = os.path.join(local_target_dir, filename)
    os.makedirs(local_target_dir, exist_ok=True)

    try:
        print(f"[DIAG][OCI] Attempting get_object -> {uri}")
        resp = client.get_object(namespace_name=namespace, bucket_name=bucket, object_name=object_name)
        if hasattr(resp.data, "raw"):
            with open(local_path, "wb") as fh:
                for chunk in resp.data.raw.stream(1024 * 1024, decode_content=True):
                    fh.write(chunk)
        else:
            data = getattr(resp.data, "content", None)
            if data is None:
                raise RuntimeError("Unexpected get_object response shape")
            with open(local_path, "wb") as fh:
                fh.write(data)
        print(f"[DIAG][OCI] Successfully downloaded to {local_path}")
        return local_path

    except Exception as exc:
        print(f"[DIAG][OCI][ERROR] get_object failed: {exc}")
        try:
            import oci.exceptions as oci_exc
            if isinstance(exc, oci_exc.ServiceError):
                details = {
                    "status": getattr(exc, "status", None),
                    "code": getattr(exc, "code", None),
                    "message": getattr(exc, "message", None),
                    "opc_request_id": getattr(exc, "opc_request_id", None),
                    "request_endpoint": getattr(exc, "request_endpoint", None),
                }
                print(f"[DIAG][OCI][ServiceError] {json.dumps(details, default=str)}")
        except Exception:
            pass

        if not list_on_error:
            raise

        # attempt listing to provide helpful diagnostics (may fail if permission denied)
        candidates: List[str] = []
        try:
            print(f"[DIAG][OCI] Listing objects in bucket='{bucket}' namespace='{namespace}' (limit=200)")
            list_resp = client.list_objects(namespace_name=namespace, bucket_name=bucket, prefix="", limit=200)
            objs = getattr(list_resp.data, "objects", []) or []
            print(f"[DIAG][OCI] Found {len(objs)} objects (first 200)")
            for o in objs[:200]:
                name = getattr(o, "name", "")
                print(f"  - {name}")
                candidates.append(name)
        except Exception as list_exc:
            print(f"[DIAG][OCI][ERROR] list_objects failed: {list_exc}")

        filename_no_ext = os.path.splitext(filename)[0]
        found_candidates = []
        for name in candidates:
            if name == object_name:
                found_candidates.append(name)
            elif name.endswith(filename):
                found_candidates.append(name)
            elif filename_no_ext and filename_no_ext in name:
                found_candidates.append(name)
        found_candidates = sorted(set(found_candidates), key=lambda x: candidates.index(x) if x in candidates else 999)

        if found_candidates:
            print(f"[DIAG][OCI] Heuristic found {len(found_candidates)} candidate(s). Attempting downloads:")
            for c in found_candidates:
                print(f"  -> {c}")
            for cand in found_candidates:
                try:
                    print(f"[DIAG][OCI] Attempting download of candidate: {cand}")
                    resp2 = client.get_object(namespace_name=namespace, bucket_name=bucket, object_name=cand)
                    local_path2 = os.path.join(local_target_dir, os.path.basename(cand))
                    if hasattr(resp2.data, "raw"):
                        with open(local_path2, "wb") as fh:
                            for chunk in resp2.data.raw.stream(1024 * 1024, decode_content=True):
                                fh.write(chunk)
                    else:
                        data2 = getattr(resp2.data, "content", None)
                        if data2 is None:
                            raise RuntimeError("Unexpected get_object response shape for candidate")
                        with open(local_path2, "wb") as fh:
                            fh.write(data2)
                    print(f"[DIAG][OCI] Successfully downloaded candidate {cand} -> {local_path2}")
                    return local_path2
                except Exception as e2:
                    print(f"[DIAG][OCI][WARN] Candidate download failed: {e2}")
        else:
            print("[DIAG][OCI] No heuristic candidates found in bucket listing.")

        raise

# ----------------- CSV loader + header validation -----------------
def _read_csv_rows(path: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            rows.append({(k if k is not None else ""): (v if v is not None else "") for k, v in r.items()})
    return rows

def _validate_headers(rows: List[Dict[str, str]], required: List[str], source: str):
    if not rows:
        raise RuntimeError(f"{source} is empty or missing required headers: {required}")
    first = rows[0]
    missing = [h for h in required if h not in first]
    if missing:
        raise RuntimeError(f"{source} missing headers: {missing}")

def load_config_from_oci_csvs_with_fallback(primary_env_oci: str, primary_tbl_oci: str,
                                             fallback_env_oci: Optional[str] = None, fallback_tbl_oci: Optional[str] = None
                                            ) -> Tuple[List[str], List[str], str, Dict[str, str], Dict[str, str]]:
    def try_download(uri: str) -> str:
        return download_single_oci_file_diagnostic_or_local(uri, local_target_dir="/tmp", list_on_error=True)

    # env CSV
    try:
        print(f"[CFG] Attempting primary env CSV: {primary_env_oci}")
        local_env = try_download(primary_env_oci)
    except Exception as e_primary_env:
        if fallback_env_oci:
            try:
                print(f"[CFG] Primary env CSV failed; attempting fallback env CSV: {fallback_env_oci}")
                local_env = try_download(fallback_env_oci)
            except Exception as e_fallback_env:
                raise RuntimeError(f"Failed to download env CSV from primary and fallback URIs. Primary error: {e_primary_env}; Fallback error: {e_fallback_env}")
        else:
            raise RuntimeError(f"Failed to download env CSV from primary URI: {e_primary_env}")

    # table CSV
    try:
        print(f"[CFG] Attempting primary table CSV: {primary_tbl_oci}")
        local_tbl = try_download(primary_tbl_oci)
    except Exception as e_primary_tbl:
        if fallback_tbl_oci:
            try:
                print(f"[CFG] Primary table CSV failed; attempting fallback table CSV: {fallback_tbl_oci}")
                local_tbl = try_download(fallback_tbl_oci)
            except Exception as e_fallback_tbl:
                raise RuntimeError(f"Failed to download table CSV from primary and fallback URIs. Primary error: {e_primary_tbl}; Fallback error: {e_fallback_tbl}")
        else:
            raise RuntimeError(f"Failed to download table CSV from primary URI: {e_primary_tbl}")

    print(f"[CFG] Loaded env CSV -> {local_env}")
    print(f"[CFG] Loaded table CSV -> {local_tbl}")

    env_rows = _read_csv_rows(local_env)
    tbl_rows = _read_csv_rows(local_tbl)

    # validate headers
    _validate_headers(env_rows, ["name", "default_value", "is_secret"], "env CSV")
    _validate_headers(tbl_rows, ["table_name", "allow", "deny", "since_date"], "table CSV")

    env_config: Dict[str, str] = {}
    global_since = ""

    for row in env_rows:
        name = str(row.get("name") or "").strip()
        default_val = "" if (row.get("default_value") is None or row.get("default_value") == "") else str(row.get("default_value"))
        is_secret = str(row.get("is_secret") or "").strip().lower() == "yes"
        if not name:
            continue
        if is_secret:
            print(f"[CFG][SECRET] Skipping secret var: {name}")
            continue
        effective_val = os.getenv(name, default_val)
        env_config[name] = effective_val
        os.environ.setdefault(name, effective_val)
        if name in ("SINCE_DATE", "GLOBAL_SINCE_DATE"):
            global_since = effective_val

    print("[CFG] Loaded env-like parameters (non-secrets):")
    for k, v in env_config.items():
        print(f"  {k} = {v}")

    env_whitelist: List[str] = []
    env_blacklist: List[str] = []
    per_table_since: Dict[str, str] = {}

    for row in tbl_rows:
        tname = str(row.get("table_name") or "").strip()
        if not tname:
            continue
        allow_val = str(row.get("allow") or "").strip()
        deny_val = str(row.get("deny") or "").strip()
        since_val = "" if (row.get("since_date") is None or row.get("since_date") == "") else str(row.get("since_date")).strip()
        allow = _bool_str(allow_val)
        deny = _bool_str(deny_val)
        if allow and not deny:
            env_whitelist.append(tname)
        if deny:
            env_blacklist.append(tname)
        if since_val:
            per_table_since[tname] = since_val

    env_whitelist = sorted(set(env_whitelist))
    env_blacklist = sorted(set(env_blacklist))

    print(f"[CFG] whitelist={env_whitelist}")
    print(f"[CFG] blacklist={env_blacklist}")
    print(f"[CFG] per_table_since={per_table_since}")

    return env_whitelist, env_blacklist, global_since, per_table_since, env_config

# ----------------- HTTP with retries (hardened) -----------------
def _http_request(method: str, url: str, headers: dict, data: Optional[bytes] = None) -> Tuple[int, bytes]:
    MAX_RETRIES = _int_env("MAX_RETRIES_HTTP", "5")
    BACKOFF = _int_env("RETRY_BACKOFF_SEC", "5")
    ctx = ssl.create_default_context()
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = request.Request(url, method=method, headers=headers, data=data)
            with request.urlopen(req, context=ctx) as resp:
                status = resp.getcode()
                content = resp.read() or b""
                key = method.lower()
                API_STATS.setdefault(key, {"count": 0, "bytes": 0})
                API_STATS[key]["count"] += 1
                API_STATS[key]["bytes"] += len(content)
                if 200 <= status < 300:
                    return status, content
                last_error = RuntimeError(f"Status {status}")
        except Exception as e:
            last_error = e
        if attempt < MAX_RETRIES:
            time.sleep(BACKOFF * attempt)
    raise RuntimeError(f"HTTP {method} failed after {MAX_RETRIES} retries: {last_error}")

# ----------------- P6 helpers -----------------
def _get_p6_auth_from_vault_or_env() -> Tuple[str, str]:
    use_vault = _bool_env("ENABLE_P6_AUTH_VAULT", "true")
    if use_vault and VAULT_ID:
        try:
            user = get_secret_from_vault(VAULT_ID, SECRET_NAME_USER).strip()
            pwd = get_secret_from_vault(VAULT_ID, SECRET_NAME_PASS).strip()
            print(f"[AUTH] P6 creds from Vault: user={_mask(user)}")
            return user, pwd
        except Exception as e:
            print(f"[AUTH][WARN] Vault auth failed: {e}")
    user = os.getenv("P6_USERNAME", "").strip()
    pwd = os.getenv("P6_PASSWORD", "").strip()
    if not user or not pwd:
        raise RuntimeError("P6 credentials not found in Vault or env (P6_USERNAME/P6_PASSWORD)")
    print(f"[AUTH] P6 creds from env: user={_mask(user)}")
    return user, pwd

def _p6_headers_basic_auth(user: str, pwd: str) -> dict:
    token = b64encode(f"{user}:{pwd}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}", "Content-Type": "application/json"}

def get_p6_urls() -> Tuple[str, str]:
    base = os.getenv("PDS_BASE", "").rstrip("/")
    cfg_code = os.getenv("PDS_CONFIG_CODE", "ds_p6adminuser")
    if not base:
        raise RuntimeError("PDS_BASE not set")
    url_cols = f"{base}/p6ws/restapi/pds/v1/configuration/{cfg_code}/metadata/columns"
    url_runq = f"{base}/p6ws/restapi/pds/v1/configuration/{cfg_code}/runQuery"
    return url_cols, url_runq

def get_p6_tables_url() -> str:
    base = os.getenv("PDS_BASE", "").rstrip("/")
    cfg_code = os.getenv("PDS_CONFIG_CODE", "ds_p6adminuser")
    if not base:
        raise RuntimeError("PDS_BASE not set")
    return f"{base}/p6ws/restapi/pds/v1/configuration/{cfg_code}/metadata/tables"

def fetch_all_p6_tables(user: str, pwd: str) -> List[str]:
    url = get_p6_tables_url()
    hdrs = _p6_headers_basic_auth(user, pwd)
    status, content = _http_request("GET", url, hdrs)
    if status != 200:
        raise RuntimeError(f"Fetching tables list failed: HTTP {status}")
    data = json.loads(content.decode("utf-8"))
    tables: List[str] = []
    if isinstance(data, dict):
        if "tables" in data and isinstance(data["tables"], list):
            for t in data["tables"]:
                if isinstance(t, str): tables.append(t)
                elif isinstance(t, dict) and "name" in t: tables.append(str(t["name"]))
        elif "items" in data and isinstance(data["items"], list):
            for item in data["items"]:
                if isinstance(item, str): tables.append(item)
                elif isinstance(item, dict) and "name" in item: tables.append(str(item["name"]))
    elif isinstance(data, list):
        for t in data:
            if isinstance(t, str): tables.append(t)
            elif isinstance(t, dict) and "name" in t: tables.append(str(t["name"]))
    return sorted(set(tables))

def fetch_p6_columns(table: str, since_date: str, user: str, pwd: str) -> dict:
    url_cols, _ = get_p6_urls()
    hdrs = _p6_headers_basic_auth(user, pwd)
    payload = {"table": table}
    if since_date:
        payload["since"] = since_date
    status, content = _http_request("POST", url_cols, hdrs, data=json.dumps(payload).encode("utf-8"))
    if status != 200:
        raise RuntimeError(f"Metadata fetch failed for {table}: HTTP {status}")
    return json.loads(content.decode("utf-8"))

def fetch_p6_runquery_page(table: str, columns: List[str], user: str, pwd: str, page: int, page_size: int, since_date: str = "") -> dict:
    _, url_runq = get_p6_urls()
    hdrs = _p6_headers_basic_auth(user, pwd)
    payload = {"name": f"{table}_extract_page_{page}", "page": page, "pageSize": page_size, "tables": [{"tableName": table, "columns": columns}]}
    if since_date:
        payload["sinceDate"] = since_date
    status, content = _http_request("POST", url_runq, hdrs, data=json.dumps(payload).encode("utf-8"))
    if status != 200:
        raise RuntimeError(f"runQuery failed for {table} page={page}: HTTP {status}")
    return json.loads(content.decode("utf-8"))

def post_runquery_all_pages(table: str, columns: List[str], user: str, pwd: str, since_date: str = "") -> dict:
    batch_ts = _now_ts()
    page_size = _int_env("PAGE_SIZE", "5000")
    max_pages = _int_env("MAX_PAGES_PER_TABLE", "1000")
    responses = []
    total_bytes = 0
    total_rows = 0
    pages = 0
    t0 = time.time()
    for page in range(1, max_pages + 1):
        resp = fetch_p6_runquery_page(table, columns, user, pwd, page, page_size, since_date)
        responses.append(resp)
        payload_bytes = len(json.dumps(resp).encode("utf-8"))
        total_bytes += payload_bytes
        rows = resp.get("result", {}).get("rows") or resp.get("rows") or []
        row_count = len(rows)
        total_rows += row_count
        pages += 1
        if row_count < page_size:
            break
    t1 = time.time()
    _perf("RUNQUERY", table, batch_ts, t1 - t0, pages, total_bytes, total_rows, path="")
    return {"table": table, "batch_ts": batch_ts, "responses": responses, "pages": pages, "bytes": total_bytes, "rows": total_rows}

# ----------------- FS/DF helpers -----------------
def _fs_put_text(spark: SparkSession, uri: str, text: str):
    sc = spark.sparkContext
    rdd = sc.parallelize([text])
    rdd.saveAsTextFile(uri)

def _safe_df(spark: SparkSession, rows: List[dict]):
    if not rows:
        return spark.createDataFrame([], StructType([StructField("status", StringType(), True)]))
    try:
        return spark.createDataFrame(rows)
    except Exception as e:
        print(f"[SAFE_DF][WARN] Creating DF from dict rows failed: {e}")
        flat = [(json.dumps(r, ensure_ascii=False),) for r in rows]
        return spark.createDataFrame(flat, ["raw_json"])

def _extract_rows_from_result(result: Any) -> List[dict]:
    if isinstance(result, dict) and "rows" in result:
        return result["rows"]
    if isinstance(result, list):
        return result
    return []

# ----------------- Audit & Perf storage -----------------
AUDIT_ROWS: List[Tuple[str, str, str, str, str, str, str, str, str, str]] = []
PERF_ROWS: List[Tuple[str, str, str, str, str, str, str, str, str, str]] = []
API_STATS = {"get": {"count": 0, "bytes": 0}, "post": {"count": 0, "bytes": 0}}

def _audit(category, table, url, http_status, error_type, retry_count, batch_ts, details, extra1="", extra2=""):
    AUDIT_ROWS.append((category, table, url, http_status, error_type, str(retry_count), batch_ts, details, extra1, extra2))

def _perf(phase, table, batch_ts, duration_sec, pages, bytes_total, rows_total, path, note="", extra=""):
    PERF_ROWS.append((phase, table, batch_ts, f"{duration_sec:.3f}", str(pages), str(bytes_total), str(rows_total), path, note, extra))

# ----------------- Main -----------------
def main():
    spark = setup_spark()
    OS_NAMESPACE = os.getenv("OS_NAMESPACE")
    OS_BUCKET = os.getenv("OS_BUCKET")
    OS_PREFIX_META = os.getenv("OS_PREFIX_META")
    OS_PREFIX_DATA = os.getenv("OS_PREFIX_DATA")
    OS_PREFIX_INDEX = os.getenv("OS_PREFIX_INDEX")
    WRITE_MODE = os.getenv("WRITE_MODE", "overwrite")

    env_csv_oci = os.getenv("P6_ENV_CSV_OCI_URI", "").strip()
    tbl_csv_oci = os.getenv("P6_TABLE_CSV_OCI_URI", "").strip()
    env_csv_fallback = os.getenv("P6_ENV_CSV_OCI_URI_FALLBACK", "").strip() or None
    tbl_csv_fallback = os.getenv("P6_TABLE_CSV_OCI_URI_FALLBACK", "").strip() or None

    if not env_csv_oci or not tbl_csv_oci:
        raise RuntimeError("Both P6_ENV_CSV_OCI_URI and P6_TABLE_CSV_OCI_URI must be set to OCI URIs (primary). You may also provide fallback URIs via P6_ENV_CSV_OCI_URI_FALLBACK and P6_TABLE_CSV_OCI_URI_FALLBACK.")

    try:
        env_whitelist, env_blacklist, global_since, per_table_since, env_config = load_config_from_oci_csvs_with_fallback(
            env_csv_oci, tbl_csv_oci, fallback_env_oci=env_csv_fallback, fallback_tbl_oci=tbl_csv_fallback
        )
    except Exception as e:
        print("[CFG][FATAL] Failed to load CSV configs:", e)
        raise

    batch_ts = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
    ENABLE_VERSIONED_PATHS = _bool_env("ENABLE_VERSIONED_PATHS", "true")
    if ENABLE_VERSIONED_PATHS:
        versioned_meta_prefix = f"{OS_PREFIX_META}/{batch_ts}"
        versioned_data_prefix = f"{OS_PREFIX_DATA}/{batch_ts}"
        versioned_index_prefix = f"{OS_PREFIX_INDEX}/{batch_ts}"
    else:
        versioned_meta_prefix = OS_PREFIX_META
        versioned_data_prefix = OS_PREFIX_DATA
        versioned_index_prefix = OS_PREFIX_INDEX

    meta_root = build_uri(OS_BUCKET, OS_NAMESPACE, versioned_meta_prefix)
    data_root = build_uri(OS_BUCKET, OS_NAMESPACE, versioned_data_prefix)
    index_root = build_uri(OS_BUCKET, OS_NAMESPACE, versioned_index_prefix)

    try:
        user, pwd = _get_p6_auth_from_vault_or_env()
        PROCESS_ALL_TABLES = _bool_env("PROCESS_ALL_TABLES", "false")

        if PROCESS_ALL_TABLES:
            all_tables = []
            try:
                all_tables = fetch_all_p6_tables(user, pwd)
            except Exception as e:
                print("[TABLES][WARN] fetch_all_p6_tables failed, falling back:", e)
            if all_tables:
                selected = [t for t in all_tables if t not in env_blacklist]
            else:
                default_tables = ["TASK", "PROJECT", "WBS"]
                base_tables = env_whitelist or default_tables
                selected = [t for t in base_tables if (t in env_whitelist if env_whitelist else t not in env_blacklist)]
        else:
            default_tables = ["TASK", "PROJECT", "WBS"]
            base_tables = env_whitelist or default_tables
            selected = [t for t in base_tables if (t in env_whitelist if env_whitelist else t not in env_blacklist)]

        print(f"[TABLES] Selected: {selected}")

        signer = get_oci_signer()
        os_client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)

        columns_map: Dict[str, List[str]] = {}
        if _bool_env("ENABLE_META_COLUMNS", "true"):
            for t in selected:
                tsince = per_table_since.get(t, global_since)
                try:
                    meta = fetch_p6_columns(t, tsince, user, pwd)
                    cols = [c["name"] for c in meta.get("columns", []) if not c.get("isLob")]
                    columns_map[t] = cols
                    fname = f"{t.lower()}_columns_{batch_ts}.json"
                    upload_single_text_to_oci(os_client, OS_NAMESPACE, OS_BUCKET, f"{versioned_meta_prefix}/columns/{fname}", json.dumps(meta, ensure_ascii=False))
                    print(f"[META][OK] {t} columns={len(cols)} since={tsince or 'None'} uri={meta_root}/columns/{fname}")
                except Exception as e:
                    print(f"[META][WARN] {t}: {e}")
                    _audit("META_FAIL", t, "", "", "EXCEPTION", 0, batch_ts, str(e))

        results = []
        if _bool_env("ENABLE_RUNQUERY", "true"):
            for t in selected:
                cols = columns_map.get(t, [])
                table_since = per_table_since.get(t, global_since)
                if not cols:
                    _audit("EMPTY_COLUMNS", t, "", "0", "NO_COLS", 0, batch_ts, "No columns post exclusion")
                    results.append({"table": t, "archive": None, "data_file_uri": "", "rows_total": 0, "pages": 0, "bytes": 0, "error": "No columns"})
                    continue
                try:
                    arc = post_runquery_all_pages(t, cols, user, pwd, table_since)
                    data_fname = f"{t.lower()}_data_{batch_ts}.json"
                    arc_uri = f"{versioned_data_prefix}/runquery/{data_fname}"
                    upload_single_text_to_oci(os_client, OS_NAMESPACE, OS_BUCKET, arc_uri, json.dumps(arc, ensure_ascii=False))
                    all_rows = []
                    for resp in arc.get("responses", []):
                        res = resp.get("result") or resp
                        all_rows.extend(_extract_rows_from_result(res))
                    rows_df = _safe_df(spark, all_rows)
                    output_prefix = f"common/api/p6/output_formats/{batch_ts}"
                    rows_csv_uri = f"oci://{OS_BUCKET}@{OS_NAMESPACE}/{output_prefix}/data/csv_rows/{t.lower()}.csv"
                    rows_parquet_uri = f"oci://{OS_BUCKET}@{OS_NAMESPACE}/{output_prefix}/data/parquet_rows/{t.lower()}.parquet"
                    if _bool_env("ENABLE_NORMALIZED_ROWS", "true") and rows_df is not None and len(rows_df.columns) > 0:
                        try:
                            rows_df.write.mode("overwrite").option("header", True).csv(rows_csv_uri)
                        except Exception as e:
                            print(f"[ROWS][WARN] Rows CSV write failed for {t}: {e}")
                        try:
                            rows_df.write.mode("overwrite").parquet(rows_parquet_uri)
                        except Exception as e:
                            print(f"[ROWS][WARN] Rows Parquet write failed for {t}: {e}")
                    results.append({"table": t, "archive": f"oci://{OS_BUCKET}@{OS_NAMESPACE}/{arc_uri}", "data_file_uri": f"oci://{OS_BUCKET}@{OS_NAMESPACE}/{arc_uri}", "rows_total": arc.get("rows", 0), "pages": arc.get("pages", 0), "bytes": arc.get("bytes", 0), "error": ""})
                except Exception as e:
                    print(f"[RUNQUERY][ERROR] {t}: {e}")
                    _audit("RUNQUERY_FAIL", t, "", "", "EXCEPTION", 0, batch_ts, str(e))
                    results.append({"table": t, "archive": None, "data_file_uri": "", "rows_total": 0, "pages": 0, "bytes": 0, "error": str(e)})

        # Single-file index/audit/perf/config/monitor uploads
        if _bool_env("ENABLE_INDEX_CSV", "true"):
            idx_lines = []
            for r in results:
                idx_lines.append({
                    "table": r["table"],
                    "archive_uri": r.get("archive") or "",
                    "data_file_uri": r.get("data_file_uri") or "",
                    "rows_total": str(r.get("rows_total", "")),
                    "pages": str(r.get("pages", "")),
                    "bytes": str(r.get("bytes", "")),
                    "error": r.get("error", "")
                })
            idx_csv_headers = ["table", "archive_uri", "data_file_uri", "rows_total", "pages", "bytes", "error"]
            idx_text = ",".join(idx_csv_headers) + "\n"
            for row in idx_lines:
                idx_text += ",".join([str(row[h]).replace("\n", " ") for h in idx_csv_headers]) + "\n"
            idx_obj_path = f"{versioned_index_prefix}/index/index.csv"
            upload_single_text_to_oci(os_client, OS_NAMESPACE, OS_BUCKET, idx_obj_path, idx_text)
            print(f"[INDEX] Wrote single-file index -> oci://{OS_BUCKET}@{OS_NAMESPACE}/{idx_obj_path}")

        if AUDIT_ROWS and _bool_env("ENABLE_ERROR_AUDIT", "true"):
            audit_csv_headers = ["category", "table", "url", "http_status", "error_type", "retry_count", "batch_ts", "details", "extra1", "extra2"]
            audit_text = ",".join(audit_csv_headers) + "\n"
            for r in AUDIT_ROWS:
                audit_text += ",".join([str(x).replace("\n", " ") for x in r]) + "\n"
            audit_obj_path = f"{versioned_index_prefix}/errors/audit.csv"
            upload_single_text_to_oci(os_client, OS_NAMESPACE, OS_BUCKET, audit_obj_path, audit_text)
            print(f"[AUDIT] Wrote audit -> oci://{OS_BUCKET}@{OS_NAMESPACE}/{audit_obj_path}")

        if PERF_ROWS and _bool_env("ENABLE_PERF_LOG", "true"):
            perf_headers = ["phase", "table", "batch_ts", "duration_sec", "pages", "bytes_total", "rows_total", "path", "note", "extra"]
            perf_text = ",".join(perf_headers) + "\n"
            for r in PERF_ROWS:
                perf_text += ",".join([str(x).replace("\n", " ") for x in r]) + "\n"
            perf_obj_path = f"{versioned_index_prefix}/performance/perf.csv"
            upload_single_text_to_oci(os_client, OS_NAMESPACE, OS_BUCKET, perf_obj_path, perf_text)
            print(f"[PERF] Wrote perf -> oci://{OS_BUCKET}@{OS_NAMESPACE}/{perf_obj_path}")

        secret_keys = {"P6_PASSWORD", "P6_USERNAME", "SECRET_NAME_USER", "SECRET_NAME_PASS", "VAULT_ID"}
        config_rows = []
        for k, v in env_config.items():
            if k in secret_keys or "PASS" in k.upper() or "SECRET" in k.upper():
                val = _mask(str(v), show=1)
            else:
                val = v or ""
            config_rows.append((k, val))
        config_text = "param_name,param_value\n"
        for k, v in config_rows:
            config_text += f"{k},{v.replace(',', ' ')}\n"
        config_obj_path = f"{versioned_index_prefix}/config_params/config_params.csv"
        upload_single_text_to_oci(os_client, OS_NAMESPACE, OS_BUCKET, config_obj_path, config_text)
        print(f"[CONFIG] Wrote config params -> oci://{OS_BUCKET}@{OS_NAMESPACE}/{config_obj_path}")

        monitor_headers = ["batch_ts", "meta_root", "data_root", "index_root", "index_csv_dir", "errors_csv_dir", "performance_csv_dir"]
        monitor_row = (
            batch_ts,
            build_uri(OS_BUCKET, OS_NAMESPACE, versioned_meta_prefix),
            build_uri(OS_BUCKET, OS_NAMESPACE, versioned_data_prefix),
            index_root,
            f"oci://{OS_BUCKET}@{OS_NAMESPACE}/{versioned_index_prefix}/index/index.csv",
            f"oci://{OS_BUCKET}@{OS_NAMESPACE}/{versioned_index_prefix}/errors/audit.csv",
            f"oci://{OS_BUCKET}@{OS_NAMESPACE}/{versioned_index_prefix}/performance/perf.csv",
        )
        monitor_text = ",".join(monitor_headers) + "\n" + ",".join([str(x).replace(",", " ") for x in monitor_row]) + "\n"
        monitor_obj_path = f"{versioned_index_prefix}/monitor/monitor_logs.csv"
        upload_single_text_to_oci(os_client, OS_NAMESPACE, OS_BUCKET, monitor_obj_path, monitor_text)
        print(f"[MONITOR] Wrote monitor logs -> oci://{OS_BUCKET}@{OS_NAMESPACE}/{monitor_obj_path}")

        print("[DONE] P6 ingestion to OCI complete")
    except Exception as e:
        print("[FATAL] Top-level failure:", e)
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
