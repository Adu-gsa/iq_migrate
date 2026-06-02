"""
Storage & Environment Configuration — `environment_config`
**Description:** Centralized storage, environment, and catalog configuration for FAS Advantage pipelines. Provides:
- Environment identifiers (`DEV`, `TEST`, `PROD`)
- Unity Catalog mapping (env → catalog name)
- Databricks workspace URLs per environment
"""




class EnvironmentConfig:
    """Centralized environment, catalog, and storage configuration for FAS Advantage pipelines."""

    # ---- Environment Identifiers ----
    DEV  = "dev"
    TEST = "test"
    PROD = "prod"

    VALID_ENVIRONMENTS = {DEV, TEST, PROD}

    # ---- Environment → Unity Catalog Mapping ----
    _CATALOG_MAP = {
        DEV:  "foia_dev",
        TEST: "foia_tst",
        PROD: "foia_prod",
    }

    # ---- Databricks Workspace URLs ----
    DEV_URL  = "https://adb-dev-foia.cloud.databricks.com"
    TEST_URL = "https://adb-test-foia.cloud.databricks.com"
    PROD_URL = "https://adb-prod-foia.cloud.databricks.com"

    _URL_MAP = {
        DEV:  DEV_URL,
        TEST: TEST_URL,
        PROD: PROD_URL,
    }

    # ---- Volume / Storage Paths ----
    VOLUME_BASE = "/Volumes/fas_advantage_np/bronze/fas_advantage_s3_np"
    INBOUND_ROOT  = f"{VOLUME_BASE}/IQ_RAW_FILES"
    OUTBOUND_ROOT = f"{VOLUME_BASE}/IQ_RAW_FILES_OUTBOUND"

    # ---- DSE (Data Sharing Enabled) ----
    _DSE_ENABLED_ENVS = {PROD}

    # ---- Default Schema ----
    DEFAULT_SCHEMA = "bronze"

    @staticmethod
    def get_environment(env_input: str) -> str:
        """Validates and normalizes environment string (case-insensitive)."""
        env = env_input.strip().lower()
        if env not in EnvironmentConfig.VALID_ENVIRONMENTS:
            raise ValueError(
                f"Invalid environment: '{env_input}'. "
                f"Must be one of: {sorted(EnvironmentConfig.VALID_ENVIRONMENTS)}"
            )
        return env

    @staticmethod
    def get_catalog(env: str) -> str:
        """Returns the Unity Catalog name for the given environment."""
        env = EnvironmentConfig.get_environment(env)
        return EnvironmentConfig._CATALOG_MAP[env]

    @staticmethod
    def get_databricks_url(env: str) -> str:
        """Returns the Databricks workspace URL for the given environment."""
        env = EnvironmentConfig.get_environment(env)
        return EnvironmentConfig._URL_MAP[env]

    @staticmethod
    def get_storage_location(env: str, path_type: str, table_name: str) -> str:
        """
        Builds a Volume storage path (case-insensitive table name).

        Inbound:  /Volumes/fas_advantage_np/bronze/fas_advantage_s3_np/IQ_RAW_FILES/<table_name>
        Outbound: /Volumes/fas_advantage_np/bronze/fas_advantage_s3_np/IQ_RAW_FILES_OUTBOUND/<table_name>_outbound

        Args:
            env:        Environment (dev/test/prod)
            path_type:  'input' or 'output'
            table_name: Target table name (case-insensitive)
        """
        env = EnvironmentConfig.get_environment(env)
        tbl = table_name.strip().lower()
        if path_type not in ("input", "output"):
            raise ValueError(f"path_type must be 'input' or 'output', got: '{path_type}'")
        if path_type == "input":
            return f"{EnvironmentConfig.INBOUND_ROOT}/{tbl}"
        else:
            return f"{EnvironmentConfig.OUTBOUND_ROOT}/{tbl}_outbound"

    @staticmethod
    def get_input_path(env: str, table_name: str) -> str:
        """Returns inbound path: .../IQ_RAW_FILES/<table>"""
        return EnvironmentConfig.get_storage_location(env, "input", table_name)

    @staticmethod
    def get_output_path(env: str, table_name: str) -> str:
        """Returns outbound path: .../IQ_RAW_FILES_OUTBOUND/<table>_outbound"""
        return EnvironmentConfig.get_storage_location(env, "output", table_name)

    @staticmethod
    def is_dse_enabled(env: str) -> bool:
        """Returns True if Data Sharing (DSE) writing is enabled for this environment."""
        env = EnvironmentConfig.get_environment(env)
        return env in EnvironmentConfig._DSE_ENABLED_ENVS

    @staticmethod
    def get_full_table_name(env: str, schema: str, table_name: str) -> str:
        """Returns fully qualified table name: `catalog`.`schema`.`table`"""
        catalog = EnvironmentConfig.get_catalog(env)
        return f"`{catalog}`.`{schema}`.`{table_name}`"

print("[INFO] environment_config: EnvironmentConfig class loaded.")
print(f"       Environments  : {sorted(EnvironmentConfig.VALID_ENVIRONMENTS)}")
print(f"       Catalog map   : {EnvironmentConfig._CATALOG_MAP}")
print(f"       Inbound root  : {EnvironmentConfig.INBOUND_ROOT}")
print(f"       Outbound root : {EnvironmentConfig.OUTBOUND_ROOT}")
print(f"       DSE enabled   : {sorted(EnvironmentConfig._DSE_ENABLED_ENVS)}")
