# p6_config.py
"""
Static configuration for the P6 -> OCI job.

This replaces:
- env_variables.csv
- table_config_template.csv

You can edit this file in Git instead of uploading CSVs to Object Storage.
"""

# -------------------------
# Environment-style settings
# -------------------------

ENV_VARS = {
    # ---- Object Storage outputs ----
    "OS_BUCKET": "bkt-neom-enowa-des-dev-data-landing",
    "OS_PREFIX_META": "common/API/P6/metadata",
    "OS_PREFIX_DATA": "common/API/P6/data",
    "OS_PREFIX_INDEX": "common/API/P6/logs",

    # ---- P6 connection / behaviour ----
    "PDS_BASE": "https://ksa1.p6.oraclecloud.com/neom/uat",
    "PDS_CONFIG_CODE": "ds_p6adminuser",

    # Global since date (optional). If None, full download by default.
    # Tables can override this via TABLE_CONFIG[table]["since_date"].
    "SINCE_DATE": None,          # e.g. "2018-01-01 00:00"

    # Spark / pagination / behaviour
    "WRITE_MODE": "overwrite",
    "PAGE_SIZE": 5000,
    "MAX_PAGES_PER_TABLE": 1000,
    "FAIL_FAST": True,

    # Feature toggles (you can extend these as you need)
    "ENABLE_INDEX_CSV": True,
    "ENABLE_ERROR_AUDIT": True,
    "ENABLE_PERF_LOG": True,
    "ENABLE_VERSIONED_PATHS": True,
    "ENABLE_META_COLUMNS": True,
    "ENABLE_RUNQUERY": True,
    "ENABLE_NORMALIZED_ROWS": True,
    "ENABLE_CONVERSION": True,

    # If True, ignore TABLE_WHITELIST/TABLE_BLACKLIST and process everything
    "PROCESS_ALL_TABLES": False,

    # Optional explicit whitelist / blacklist (comma-separated table names).
    # If you prefer to control allow/deny purely via TABLE_CONFIG below,
    # leave these as empty strings.
    "TABLE_WHITELIST": "",
    "TABLE_BLACKLIST": "",
}

# -------------------------
# Per-table configuration
# -------------------------
# This mirrors table_config_template.csv:
#   table_name, allow, deny, since_date, comment
#
# allow/deny are booleans, since_date is either a string or None.
# comment is purely for documentation / logging.

TABLE_CONFIG = {
    # Examples from your CSV (you can extend with all real tables):

    "TASK": {
        "allow": True,
        "deny": False,
        "since_date": "2015-01-01 00:00",
        "comment": "Example allowed table with per-table since_date",
    },
    "PROJECT": {
        "allow": True,
        "deny": False,
        "since_date": "2018-01-01 00:00",
        "comment": "Example allowed table with per-table since_date",
    },
    "WBS": {
        "allow": True,
        "deny": False,
        "since_date": None,  # uses global SINCE_DATE if set, else full download
        "comment": "Uses global SINCE_DATE if set, else full download",
    },
    "ADMIN_CONFIG": {
        "allow": False,
        "deny": True,
        "since_date": None,
        "comment": "Example denied table (skipped even in PROCESS_ALL_TABLES mode)",
    },

    # You can now add the rest of your tables here, e.g.:
    #
    # "DLTACCT":   {"allow": True,  "deny": False, "since_date": None, "comment": ""},
    # "DLTACTV":   {"allow": True,  "deny": False, "since_date": None, "comment": ""},
    # "DLTOBS":    {"allow": True,  "deny": False, "since_date": None, "comment": ""},
    # ...
}
