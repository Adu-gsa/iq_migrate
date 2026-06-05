# =============================================================================
# Module: environment_config.py
# Version: 0.1
# Developed by: Adu Erena
# Date: 2025-06-05
# Description: Centralized storage, environment, and catalog configuration for
#              FAS Advantage pipelines. Provides environment identifiers,
#              Unity Catalog mapping, Databricks workspace URLs, and Volume
#              storage path resolution.
# =============================================================================
"""
Storage & Environment Configuration — `environment_config`
**Description:** Centralized storage, environment, and catalog configuration for FAS Advantage pipelines. Provides:
- Environment identifiers (`DEV`, `TEST`, `PROD`)
- Unity Catalog mapping (env → catalog name)
- Databricks workspace URLs per environment
"""




class EnvironmentConfig:
    """Centralized environment, catalog, and storage configuration for FAS Advantage pipelines.

    This class serves as the single source of truth for all environment-specific
    configuration values used across Bronze and Silver ingestion pipelines.
    All methods are @staticmethod — no instantiation needed.
    """

    # ---- Environment Identifiers ----
    # These constants define the valid runtime environments for the pipeline
    DEV  = "dev"
    TEST = "test"
    PROD = "prod"

    # Set of all recognized environments — used for input validation
    VALID_ENVIRONMENTS = {DEV, TEST, PROD}

    # ---- Environment → Unity Catalog Mapping ----
    # Each environment maps to a distinct Unity Catalog to ensure data isolation
    _CATALOG_MAP = {
        DEV:  "foia_dev",
        TEST: "foia_tst",
        PROD: "foia_prod",
    }

    # ---- Databricks Workspace URLs ----
    # DEV and TEST share the non-production workspace; PROD has its own
    DEV_URL  = "https://gsa-fas-advantage-np.cloud.databricks.com"
    TEST_URL = "https://gsa-fas-advantage-np.cloud.databricks.com"
    PROD_URL = "https://gsa-fas-advantage.cloud.databricks.com"

    _URL_MAP = {
        DEV:  DEV_URL,
        TEST: TEST_URL,
        PROD: PROD_URL,
    }

    # ---- Volume / Storage Paths ----
    # Base path to the Unity Catalog Volume where source files are stored
    VOLUME_BASE = "/Volumes/fas_advantage_np/bronze/fas_advantage_s3_np"
    # Inbound: where raw source .txt files land before ingestion
    INBOUND_ROOT  = f"{VOLUME_BASE}/IQ_RAW_FILES"
    # Outbound: where source files are archived after successful ingestion
    OUTBOUND_ROOT = f"{VOLUME_BASE}/IQ_RAW_FILES_OUTBOUND"

    # ---- DSE (Data Sharing Enabled) ----
    # Only PROD environment has Data Sharing enabled for external consumers
    _DSE_ENABLED_ENVS = {PROD}

    # ---- Default Schema ----
    # Bronze is the default landing schema for raw ingested data
    DEFAULT_SCHEMA = "bronze"

    @staticmethod
    def get_environment(env_input: str) -> str:
        """Validates and normalizes environment string (case-insensitive).

        Ensures the provided environment string is one of the recognized values
        (dev, test, prod). Raises ValueError with helpful message if invalid.

        Args:
            env_input: Raw environment string from job parameters or widgets

        Returns:
            Normalized lowercase environment string (e.g., 'test')

        Raises:
            ValueError: If env_input is not a valid environment
        """
        env = env_input.strip().lower()
        if env not in EnvironmentConfig.VALID_ENVIRONMENTS:
            raise ValueError(
                f"Invalid environment: '{env_input}'. "
                f"Must be one of: {sorted(EnvironmentConfig.VALID_ENVIRONMENTS)}"
            )
        return env

    @staticmethod
    def get_catalog(env: str) -> str:
        """Returns the Unity Catalog name for the given environment.

        Maps environment to its corresponding Unity Catalog:
        - dev  → foia_dev
        - test → foia_tst
        - prod → foia_prod

        Args:
            env: Environment string (dev/test/prod)

        Returns:
            Unity Catalog name string
        """
        env = EnvironmentConfig.get_environment(env)
        return EnvironmentConfig._CATALOG_MAP[env]

    @staticmethod
    def get_databricks_url(env: str) -> str:
        """Returns the Databricks workspace URL for the given environment.

        Args:
            env: Environment string (dev/test/prod)

        Returns:
            Full Databricks workspace URL string
        """
        env = EnvironmentConfig.get_environment(env)
        return EnvironmentConfig._URL_MAP[env]

    @staticmethod
    def get_storage_location(env: str, path_type: str, table_name: str) -> str:
        """Builds a Volume storage path (case-insensitive table name).

        Constructs the full path to either the inbound source folder or the
        outbound archive folder for a given table.

        Inbound:  /Volumes/fas_advantage_np/bronze/fas_advantage_s3_np/IQ_RAW_FILES/<table_name>
        Outbound: /Volumes/fas_advantage_np/bronze/fas_advantage_s3_np/IQ_RAW_FILES_OUTBOUND/<table_name>_outbound

        Args:
            env:        Environment (dev/test/prod)
            path_type:  'input' for inbound source path, 'output' for archive path
            table_name: Target table name (case-insensitive)

        Returns:
            Full Volume path string

        Raises:
            ValueError: If path_type is not 'input' or 'output'
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
        """Returns inbound path: .../IQ_RAW_FILES/<table>

        Convenience wrapper around get_storage_location for input paths.

        Args:
            env:        Environment (dev/test/prod)
            table_name: Table name to build the path for

        Returns:
            Full inbound Volume path where source .txt files reside
        """
        return EnvironmentConfig.get_storage_location(env, "input", table_name)

    @staticmethod
    def get_output_path(env: str, table_name: str) -> str:
        """Returns outbound path: .../IQ_RAW_FILES_OUTBOUND/<table>_outbound

        Convenience wrapper around get_storage_location for archive paths.

        Args:
            env:        Environment (dev/test/prod)
            table_name: Table name to build the path for

        Returns:
            Full outbound Volume path where archived files are stored
        """
        return EnvironmentConfig.get_storage_location(env, "output", table_name)

    @staticmethod
    def is_dse_enabled(env: str) -> bool:
        """Returns True if Data Sharing (DSE) writing is enabled for this environment.

        Currently only PROD has Data Sharing enabled for external consumers.

        Args:
            env: Environment (dev/test/prod)

        Returns:
            True if DSE is enabled, False otherwise
        """
        env = EnvironmentConfig.get_environment(env)
        return env in EnvironmentConfig._DSE_ENABLED_ENVS

    @staticmethod
    def get_full_table_name(env: str, schema: str, table_name: str) -> str:
        """Returns fully qualified table name: `catalog`.`schema`.`table`

        Builds a three-part Unity Catalog table reference with backtick quoting
        suitable for use in SQL statements.

        Args:
            env:        Environment (dev/test/prod) — used to resolve catalog
            schema:     Schema/database name (e.g., 'bronze', 'silver')
            table_name: Table name

        Returns:
            Backtick-quoted fully qualified table name string
        """
        catalog = EnvironmentConfig.get_catalog(env)
        return f"`{catalog}`.`{schema}`.`{table_name}`"

# Print configuration summary on module load for debugging visibility
print("[INFO] environment_config: EnvironmentConfig class loaded.")
print(f"       Environments  : {sorted(EnvironmentConfig.VALID_ENVIRONMENTS)}")
print(f"       Catalog map   : {EnvironmentConfig._CATALOG_MAP}")
print(f"       Inbound root  : {EnvironmentConfig.INBOUND_ROOT}")
print(f"       Outbound root : {EnvironmentConfig.OUTBOUND_ROOT}")
print(f"       DSE enabled   : {sorted(EnvironmentConfig._DSE_ENABLED_ENVS)}")
