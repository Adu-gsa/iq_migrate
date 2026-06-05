# =============================================================================
# Module: INGEST_TABLE_DISPATCHER.py
# Version: 0.1
# Developed by: Adu Erena
# Date: 2025-06-05
# Description: Bronze Ingestion Dispatcher — single workflow entry point for all
#              Bronze ingest tables. Resolves environment config, loads table-
#              specific schemas, reads source .txt files, enriches with metadata,
#              writes to Bronze Delta tables, archives source files, and logs
#              audit records. Supports both single-table and all-table run modes.
# =============================================================================
# =============================================================================
# Module: INGEST_TABLE_DISPATCHER.py
# Version: 0.1
# Developed by: Adu Erena
# Date: 2025-06-05
# Description: Bronze Ingestion Dispatcher — single workflow entry point for all
#              Bronze ingest tables. Resolves environment config, loads table-
#              specific schemas, reads source .txt files, enriches with metadata,
#              writes to Bronze Delta tables, archives source files, and logs
#              audit records. Supports both single-table and all-table run modes.
# =============================================================================
"""
Bronze Ingestion Dispatcher
This notebook provides a single workflow entry point for all Bronze ingest tables.
How it works:
1. The workflow passes `table_name` (or runs all tables with `run_mode=all`).
2. The dispatcher resolves the target ingest notebook path internally.
"""




import importlib.util
import os
import time
from datetime import datetime
from typing import Dict, List
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType


def _get_dbutils():
    """Resolve dbutils in both notebook and wheel/serverless task contexts.

    Handles dual execution contexts:
    - Notebook: dbutils is injected as a global by Databricks
    - Wheel task: imported from databricks.sdk.runtime
    """
    try:
        return dbutils  # injected in notebook context
    except NameError:
        from databricks.sdk.runtime import dbutils as _dbutils
        return _dbutils


def _get_spark():
    """Resolve spark in both notebook and wheel/serverless task contexts.

    Handles dual execution contexts:
    - Notebook: spark is injected as a global by Databricks
    - Wheel task: retrieved via SparkSession.getActiveSession()
    """
    try:
        return spark  # injected in notebook context
    except NameError:
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()


def _load_module(module_name: str, file_path: str):
    """Dynamically load a Python module from a workspace file path.

    Used to import environment_config.py and etl_utils.py at runtime
    when running in notebook context (where standard package imports
    may not resolve workspace paths correctly).

    Args:
        module_name: Name to assign to the loaded module
        file_path:   Absolute filesystem path to the .py file

    Returns:
        The loaded module object

    Raises:
        ImportError: If the module spec cannot be created from the path
    """
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module spec for {module_name} from: {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _find_file_upwards(start_dir: str, relative_candidates, max_levels: int = 10) -> str:
    """Search for a file by walking up the directory tree from a starting point.

    Traverses parent directories looking for any of the relative_candidates paths.
    Checks both /Workspace-prefixed and bare paths at each level.

    Args:
        start_dir:           Directory to start searching from
        relative_candidates: List of relative paths to look for (e.g., ['env/etl_utils.py'])
        max_levels:          Maximum number of parent directories to traverse

    Returns:
        Absolute path to the first found file

    Raises:
        FileNotFoundError: If none of the candidates are found within max_levels
    """
    current = start_dir
    for _ in range(max_levels + 1):
        for rel_path in relative_candidates:
            for base in (f"/Workspace{current}", current):
                candidate = f"{base}/{rel_path}".replace("//", "/")
                if os.path.exists(candidate):
                    return candidate

        if current in ("", "/"):
            break
        current = current.rsplit("/", 1)[0] or "/"

    raise FileNotFoundError(
        f"Unable to locate file. start_dir={start_dir}, "
        f"relative_candidates={relative_candidates}, max_levels={max_levels}"
    )


def _install_local_fallbacks():
    """Install self-contained fallback implementations of shared utilities.

    When running as a wheel task (outside notebook context), the shared modules
    (environment_config.py, etl_utils.py) cannot be loaded via filesystem paths.
    This function defines minimal local implementations of EnvironmentConfig,
    ensure_etl_log_table_exists, write_etl_log, and archive_file, then injects
    them into the global namespace so downstream code works identically.
    """
    class EnvironmentConfig:
        DEV = "dev"
        TEST = "test"
        PROD = "prod"
        VALID_ENVIRONMENTS = {DEV, TEST, PROD}
        _CATALOG_MAP = {DEV: "foia_dev", TEST: "foia_tst", PROD: "foia_prod"}
        VOLUME_BASE = "/Volumes/fas_advantage_np/bronze/fas_advantage_s3_np"
        INBOUND_ROOT = f"{VOLUME_BASE}/IQ_RAW_FILES"
        OUTBOUND_ROOT = f"{VOLUME_BASE}/IQ_RAW_FILES_OUTBOUND"

        @staticmethod
        def get_environment(env_input: str) -> str:
            env = (env_input or "").strip().lower()
            if env not in EnvironmentConfig.VALID_ENVIRONMENTS:
                raise ValueError(
                    f"Invalid environment: '{env_input}'. Must be one of: {sorted(EnvironmentConfig.VALID_ENVIRONMENTS)}"
                )
            return env

        @staticmethod
        def get_catalog(env: str) -> str:
            env = EnvironmentConfig.get_environment(env)
            return EnvironmentConfig._CATALOG_MAP[env]

        @staticmethod
        def get_input_path(env: str, table_name: str) -> str:
            EnvironmentConfig.get_environment(env)
            return f"{EnvironmentConfig.INBOUND_ROOT}/{table_name.strip().lower()}"

        @staticmethod
        def get_output_path(env: str, table_name: str) -> str:
            EnvironmentConfig.get_environment(env)
            return f"{EnvironmentConfig.OUTBOUND_ROOT}/{table_name.strip().lower()}_outbound"

    def ensure_etl_log_table_exists(catalog: str, schema: str, env: str):
        full_table_name = f"`{catalog}`.`{schema}`.`etl_log`"
        ddl = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                job_id            STRING,
                job_run_id        STRING,
                task_id           STRING,
                task_run_id       STRING,
                start_time        TIMESTAMP,
                end_time          TIMESTAMP,
                source_file_path  STRING,
                source_table      STRING,
                target_table      STRING,
                success           INT,
                failure_reason    STRING,
                record_count      LONG
            )
            USING DELTA
        """
        _get_spark().sql(ddl)

    def write_etl_log(
        catalog, schema, job_id, job_run_id, task_id, task_run_id,
        start_time, end_time, source_file_path, source_table,
        target_table, success, failure_reason, record_count
    ):
        full_log_table = f"`{catalog}`.`{schema}`.`etl_log`"
        log_data = [(
            str(job_id), str(job_run_id), str(task_id), str(task_run_id),
            start_time, end_time, str(source_file_path),
            str(source_table) if source_table else None,
            str(target_table), int(success),
            str(failure_reason) if failure_reason else None,
            int(record_count)
        )]
        log_schema = StructType([
            StructField("job_id", StringType(), True),
            StructField("job_run_id", StringType(), True),
            StructField("task_id", StringType(), True),
            StructField("task_run_id", StringType(), True),
            StructField("start_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("source_file_path", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("success", IntegerType(), True),
            StructField("failure_reason", StringType(), True),
            StructField("record_count", LongType(), True),
        ])
        _get_spark().createDataFrame(log_data, schema=log_schema).write.format("delta").mode("append").saveAsTable(full_log_table)

    def archive_file(source_file_path: str, archive_base_path: str, env: str):
        file_name = source_file_path.split("/")[-1]
        date_partition = datetime.now().strftime("%Y-%m-%d")
        archive_dest_path = f"{archive_base_path}/{env}/{date_partition}/{file_name}"
        _get_dbutils().fs.mv(source_file_path, archive_dest_path, recurse=False)
        return archive_dest_path

    globals().update({
        "EnvironmentConfig": EnvironmentConfig,
        "ensure_etl_log_table_exists": ensure_etl_log_table_exists,
        "write_etl_log": write_etl_log,
        "archive_file": archive_file,
    })


try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    notebook_dir = notebook_path.rsplit("/", 1)[0]

    environment_config_path = _find_file_upwards(
        notebook_dir,
        [
            "env/environment_config.py",
            "Data_ingestion/env/environment_config.py",
        ],
    )
    etl_utils_path = _find_file_upwards(
        notebook_dir,
        [
            "env/etl_utils.py",
            "Data_ingestion/env/etl_utils.py",
        ],
    )

    environment_config = _load_module("environment_config", environment_config_path)
    etl_utils = _load_module("etl_utils", etl_utils_path)

    # Bind Databricks runtime objects so imported helper functions can call spark.sql and dbutils.fs.
    etl_utils.spark = spark
    etl_utils.dbutils = dbutils

    EnvironmentConfig = environment_config.EnvironmentConfig
    for name in ["ensure_etl_log_table_exists", "archive_file", "write_etl_log"]:
        globals()[name] = getattr(etl_utils, name)

    print(f"[INFO] Loaded environment module: {environment_config_path}")
    print(f"[INFO] Loaded ETL utils module: {etl_utils_path}")
except Exception as exc:
    print(f"[WARN] Shared module load failed; using local fallbacks. Error: {exc}")
    _install_local_fallbacks()
    print("[INFO] Local fallback EnvironmentConfig and ETL utils installed.")


# Define unified workflow widgets for this notebook.
class BronzeIngestion:
    """Encapsulates the end-to-end Bronze ingestion flow for a single table.

    Responsibilities:
    1. Read source .txt file using the table-specific StructType schema
    2. Add metadata columns (ingest_ts, input_file_name)
    3. Write enriched DataFrame to the Bronze Delta table (full overwrite)
    4. Archive the source file to the outbound folder with date partitioning
    5. Write ETL audit log entry on success or failure
    """
    # Executes one table ingestion flow: read, enrich metadata, write Delta, archive, and log.
    SOURCE_FILE_FORMAT = "csv"
    SOURCE_DELIMITER = "^|~"
    SOURCE_HAS_HEADER = "true"
    SOURCE_ENCODING = "UTF-8"
    COL_INGEST_TS = "ingest_ts"
    COL_INPUT_FILE_NAME = "input_file_name"

    def __init__(self, spark, source_schema: StructType, config: Dict[str, str]):
        # Stores runtime values and table-specific configuration for ingestion.
        self.spark = spark
        self.source_schema = source_schema
        self.env = config["env"]
        self.catalog = config["catalog"]
        self.schema = config["schema"]
        self.target_table = config["target_table"]
        self.source_file_path = config["source_file_path"]
        self.input_file_name = config["input_file_name"]
        self.output_folder_path = config["output_folder_path"]
        self.job_id = config["job_id"]
        self.job_run_id = config["job_run_id"]
        self.task_id = config["task_id"]
        self.task_run_id = config["task_run_id"]
        self.full_target_table = f"`{self.catalog}`.`{self.schema}`.`{self.target_table}`"

    def _read_source_file(self):
        # Reads source text file with the passed StructType schema.
        source_path = self.source_file_path[5:] if self.source_file_path.startswith("dbfs:") else self.source_file_path
        return (
            self.spark.read
            .format(self.SOURCE_FILE_FORMAT)
            .schema(self.source_schema)
            .option("sep", self.SOURCE_DELIMITER)
            .option("header", self.SOURCE_HAS_HEADER)
            .option("encoding", self.SOURCE_ENCODING)
            .option("mode", "PERMISSIVE")
            .option("maxCharsPerColumn", -1)
            .load(source_path)
        )

    def _add_metadata_columns(self, df):
        # Appends ingestion timestamp and source file name fields.
        return (
            df
            .withColumn(self.COL_INGEST_TS, F.current_timestamp())
            .withColumn(self.COL_INPUT_FILE_NAME, F.lit(self.input_file_name).cast(StringType()))
        )

    def _write_to_bronze(self, df):
        # Writes transformed dataframe into the Bronze target table.
        # Repartition to spread write workload across all cluster workers.
        (
            df.repartition(32)
            .write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(self.full_target_table)
        )
        # Get row count from the written Delta table instead of scanning the DataFrame twice.
        row_count = self.spark.table(self.full_target_table).count()
        return row_count

    def run(self):
        # Runs end-to-end ingestion and writes ETL log on success or failure.
        start_time = datetime.now()
        success = 0
        failure_reason = None
        record_count = 0

        try:
            ensure_etl_log_table_exists(self.catalog, self.schema, self.env)
            raw_df = self._read_source_file()
            enriched_df = self._add_metadata_columns(raw_df)
            record_count = self._write_to_bronze(enriched_df)
            archive_file(self.source_file_path, self.output_folder_path, self.env)
            success = 1
            print(f"[DONE] table={self.target_table} rows={record_count:,}")
        except Exception as exc:
            failure_reason = str(exc)
            print(f"[FAILURE] table={self.target_table} error={failure_reason}")
            raise
        finally:
            write_etl_log(
                self.catalog,
                self.schema,
                self.job_id,
                self.job_run_id,
                self.task_id,
                self.task_run_id,
                start_time,
                datetime.now(),
                self.source_file_path,
                None,
                self.full_target_table,
                success,
                failure_reason,
                record_count,
            )

def run_bronze_ingestion(table_name: str, source_schema: StructType, source_folder: str = None):
    """Resolve source path and execute one table ingestion using shared Bronze rules.

    Locates the single .txt file in the inbound folder, builds the ingestion config,
    and delegates to BronzeIngestion.run() for the actual processing.

    Args:
        table_name:    Name of the table to ingest
        source_schema: PySpark StructType defining the expected source file columns
        source_folder: Optional override for the source folder name (defaults to table_name)

    Raises:
        FileNotFoundError: If no .txt file exists in the inbound folder
        ValueError:        If multiple .txt files are found (expects exactly one)
    """
    # Resolves source path and executes one table ingestion using shared Bronze rules.
    if source_folder:
        input_folder_path = f"{EnvironmentConfig.INBOUND_ROOT}/{source_folder}"
        output_folder_path = f"{EnvironmentConfig.OUTBOUND_ROOT}/{source_folder}_outbound"
    else:
        input_folder_path = EnvironmentConfig.get_input_path(env, table_name)
        output_folder_path = EnvironmentConfig.get_output_path(env, table_name)

    txt_files = [f.path for f in _get_dbutils().fs.ls(input_folder_path) if f.name.lower().endswith(".txt")]
    if len(txt_files) == 0:
        raise FileNotFoundError(f"No .txt file found in: {input_folder_path}")
    if len(txt_files) > 1:
        raise ValueError(f"Expected 1 .txt file in {input_folder_path}, found {len(txt_files)}: {txt_files}")

    source_file_path = txt_files[0]
    source_file_path = source_file_path[5:] if source_file_path.startswith("dbfs:") else source_file_path

    config = {
        "env": env,
        "catalog": catalog,
        "schema": schema,
        "target_table": table_name,
        "source_file_path": source_file_path,
        "input_file_name": source_file_path.split("/")[-1],
        "output_folder_path": output_folder_path,
        **job_ids,
    }
    BronzeIngestion(_get_spark(), source_schema, config).run()


# =============================================================================
# SCHEMA BUILDER FUNCTIONS
# Each build_schema_<table_name>() function returns a PySpark StructType that
# defines the column names and types for reading the corresponding source .txt
# file. All columns are defined as StringType because Bronze ingestion preserves
# raw values as-is; type casting happens in the Bronze-to-Silver transformation.
# =============================================================================

def build_schema_adv_product():
    return StructType([
        StructField("OID",                       StringType(), True),
        StructField("PROD_ID",                   StringType(), True),
        StructField("STORE_ID",                  StringType(), True),
        StructField("CREATION_TIME",             StringType(), True),
        StructField("STATUS",                    StringType(), True),
        StructField("DELETED",                   StringType(), True),
        StructField("LAST_MOD_TIME",             StringType(), True),
        StructField("NAME",                      StringType(), True),
        StructField("DEPT",                      StringType(), True),
        StructField("PRICE",                     StringType(), True),
        StructField("STOCK",                     StringType(), True),
        StructField("PREVIEW_IMAGE",             StringType(), True),
        StructField("PREVIEW_IMAGE_WIDTH",       StringType(), True),
        StructField("PREVIEW_IMAGE_HEIGHT",      StringType(), True),
        StructField("FULL_IMAGE",                StringType(), True),
        StructField("FULL_IMAGE_WIDTH",          StringType(), True),
        StructField("FULL_IMAGE_HEIGHT",         StringType(), True),
        StructField("AUDIO_FILE",                StringType(), True),
        StructField("VIDEO_FILE",                StringType(), True),
        StructField("SDESC2",                    StringType(), True),
        StructField("LPRICE",                    StringType(), True),
        StructField("LONGDESC",                  StringType(), True),
        StructField("APPDEF1",                   StringType(), True),
        StructField("APPDEF2",                   StringType(), True),
        StructField("APPDEF3",                   StringType(), True),
        StructField("RATING",                    StringType(), True),
        StructField("NO_VOTES",                  StringType(), True),
        StructField("TOTAL_RATING",              StringType(), True),
        StructField("ADV_ASS_NUM",               StringType(), True),
        StructField("ADV_ITEM_NUM",              StringType(), True),
        StructField("ADV_SCHED_NUM",             StringType(), True),
        StructField("ADV_CONTRACT_NUM",          StringType(), True),
        StructField("ADV_CATCODE",               StringType(), True),
        StructField("ADV_DUNS",                  StringType(), True),
        StructField("ADV_VENDOR_NAME",           StringType(), True),
        StructField("ADV_GOVT_NAME",             StringType(), True),
        StructField("ADV_ITEM_TYPE",             StringType(), True),
        StructField("ADV_OPTIONS_FLG",           StringType(), True),
        StructField("ADV_ACCESSORY_FLG",         StringType(), True),
        StructField("ADV_UNIT",                  StringType(), True),
        StructField("ADV_AAC",                   StringType(), True),
        StructField("ADV_ITEM_CODE",             StringType(), True),
        StructField("ADV_DELIVERY_CODE",         StringType(), True),
        StructField("ADV_ALLIED_COMP_FLG",       StringType(), True),
        StructField("ADV_CHLORINE_FREE_FLG",     StringType(), True),
        StructField("ADV_ENERGY_EFFICIENT_FLG",  StringType(), True),
        StructField("ADV_LEAD_FREE_FLG",         StringType(), True),
        StructField("ADV_ENERGY_STAR_FLG",       StringType(), True),
        StructField("ADV_LOW_VOLATILE_FLG",      StringType(), True),
        StructField("ADV_OZONE_SAFE_FLG",        StringType(), True),
        StructField("ADV_NIB_NISH_FLG",          StringType(), True),
        StructField("ADV_RECYCLED_CONTENT_FLG",  StringType(), True),
        StructField("ADV_UNICORE_FLG",           StringType(), True),
        StructField("ADV_WATER_CONSERVING_FLG",  StringType(), True),
        StructField("ADV_NONE_CODE_FLG",         StringType(), True),
        StructField("ADV_OTHER_ENV_FLG",         StringType(), True),
        StructField("ADV_YEAR_2000_FLG",         StringType(), True),
        StructField("ADV_ENVIRONMENTAL_FLG",     StringType(), True),
        StructField("ADV_WILDFIRE_ITEM_FLG",     StringType(), True),
        StructField("ADV_SMALL_BUSINESS_FLG",    StringType(), True),
        StructField("ADV_DISCOUNT_FLG",          StringType(), True),
        StructField("ADV_NSN",                   StringType(), True),
        StructField("ADV_MANUFACTURE_NAME",      StringType(), True),
        StructField("ADV_VISA_FLG",              StringType(), True),
        StructField("ADV_DEL_DAYS_LOW",          StringType(), True),
        StructField("ADV_DEL_DAYS_HIGH",         StringType(), True),
        StructField("ADV_MINORITY_OWNED",        StringType(), True),
        StructField("ADV_WOMAN_OWNED",           StringType(), True),
        StructField("ADV_COLOR_FLG",             StringType(), True),
        StructField("ADV_VENDOR_URL",            StringType(), True),
        StructField("ADV_BUSINESS_SIZE",         StringType(), True),
        StructField("ADV_LSA_CODE",              StringType(), True),
        StructField("ADV_BUS_ATTRIB",            StringType(), True),
        StructField("ADV_DIMENSION",             StringType(), True),
        StructField("ADV_SIN_NUM",               StringType(), True),
        StructField("ADV_SIN_MAX_ORDER",         StringType(), True),
        StructField("ADV_PHOTO_CODE",            StringType(), True),
        StructField("ADV_SDB_PROG",              StringType(), True),
        StructField("ADV_ENVIRON_MESSAGE",       StringType(), True),
        StructField("ADV_CPG",                   StringType(), True),
        StructField("ADV_VET_OWNED_SMALL_BUS",   StringType(), True),
        StructField("ADV_PRODUCT",               StringType(), True),
        StructField("ADV_PCODE",                 StringType(), True),
        StructField("PRICE_INDICATOR",           StringType(), True),
    ])

def build_schema_bpa_header():
    return StructType([
        StructField("ID",                          StringType(), True),
        StructField("BPA_CATEGORY_ID",             StringType(), True),
        StructField("BPA_NUMBER",                  StringType(), True),
        StructField("CONTRACT_NUMBER",             StringType(), True),
        StructField("CREATION_TIME",               StringType(), True),
        StructField("CREATED_BY",                  StringType(), True),
        StructField("DESCRIPTION",                 StringType(), True),
        StructField("STORE_ID",                    StringType(), True),
        StructField("POC_FIRST_NAME",              StringType(), True),
        StructField("POC_LAST_NAME",               StringType(), True),
        StructField("POC_PHONE",                   StringType(), True),
        StructField("START_DATE",                  StringType(), True),
        StructField("END_DATE",                    StringType(), True),
        StructField("URL",                         StringType(), True),
        StructField("LAST_MOD_TIME",               StringType(), True),
        StructField("UPDATED_BY",                  StringType(), True),
        StructField("SCHEDULE_DISCOUNT",           StringType(), True),
        StructField("STATUS",                      StringType(), True),
        StructField("BATCH_NUMBER",                StringType(), True),
        StructField("VENDOR_NAME",                 StringType(), True),
        StructField("SERVICE",                     StringType(), True),
        StructField("POC_EMAIL",                   StringType(), True),
        StructField("BPA_NOTES",                   StringType(), True),
        StructField("QUOTES",                      StringType(), True),
        StructField("EBUY_QUOTES",                 StringType(), True),
        StructField("USERS",                       StringType(), True),
        StructField("ADV_STATUS",                  StringType(), True),
        StructField("BPA_SCHEDULE",                StringType(), True),
        StructField("BPA_SIN",                     StringType(), True),
        StructField("DISP_IN_EBUY",                StringType(), True),
        StructField("BPA_MIN_ORDER",               StringType(), True),
        StructField("STD_DELIVERY_TIME",           StringType(), True),
        StructField("STD_DELIVERY_TIME_2",         StringType(), True),
        StructField("DELIVERY_CODE",               StringType(), True),
        StructField("NEXT_DAY_DELIVERY",           StringType(), True),
        StructField("DESKTOP_DELIVERY",            StringType(), True),
        StructField("SECURE_DESKTOP_DELIVERY",     StringType(), True),
        StructField("CHECK_UPIID_FORMAT",          StringType(), True),
        StructField("CONVENIENCE_FEE",             StringType(), True),
        StructField("NEXT_DAY_DELIVERY_FLAT_RATE", StringType(), True),
        StructField("DESKTOP_DELIVERY_FLAT_RATE",  StringType(), True),
        StructField("SECURE_DELIVERY_FLAT_RATE",   StringType(), True),
    ])

def build_schema_bpa_item():
    return StructType([
        StructField("ID",               StringType(), True),
        StructField("BPA_ID",           StringType(), True),
        StructField("ITEM_NUM",         StringType(), True),
        StructField("ASSNUM",           StringType(), True),
        StructField("LINE_NUMBER",      StringType(), True),
        StructField("STOCK_INDICATOR",  StringType(), True),
        StructField("ITEM_PRICE",       StringType(), True),
        StructField("PRICING_PROGRAM",  StringType(), True),
        StructField("STATUS",           StringType(), True),
        StructField("MFR_NAME",         StringType(), True),
    ])

def build_schema_bpa_item_price():
    return StructType([
        StructField("ID",               StringType(), True),
        StructField("ITEM_ID",          StringType(), True),
        StructField("START_RANGE",      StringType(), True),
        StructField("END_RANGE",        StringType(), True),
        StructField("DISCOUNT_PERCENT", StringType(), True),
        StructField("PRICE",            StringType(), True),
    ])

def build_schema_catalog_832():
    return StructType([
        StructField("VEND_ID",             StringType(), True),
        StructField("CONTRACT_NUM",        StringType(), True),
        StructField("SCHED_NUM",           StringType(), True),
        StructField("CATALOG_NUM",         StringType(), True),
        StructField("AGENCY_NAME",         StringType(), True),
        StructField("BUYER_CODE",          StringType(), True),
        StructField("CON_STDATE",          StringType(), True),
        StructField("CON_ENDDATE",         StringType(), True),
        StructField("CC_DISC",             StringType(), True),
        StructField("DISC_TERM_AMT",       StringType(), True),
        StructField("DISC_TERM_PER",       StringType(), True),
        StructField("DISC_TERM_DAY",       StringType(), True),
        StructField("FOB",                 StringType(), True),
        StructField("MAX_BATTERY",         StringType(), True),
        StructField("MAX_NSP",             StringType(), True),
        StructField("MAX_SHIP",            StringType(), True),
        StructField("MOP",                 StringType(), True),
        StructField("MIN_ORDER",           StringType(), True),
        StructField("MAX_ORDER",           StringType(), True),
        StructField("DELIVERY_DAYS1",      StringType(), True),
        StructField("DELIVERY_DAYS2",      StringType(), True),
        StructField("ZONE_FLAG",           StringType(), True),
        StructField("PROMPT_PAY",          StringType(), True),
        StructField("MOD_DATE",            StringType(), True),
        StructField("DISC_TERM_PER2",      StringType(), True),
        StructField("DISC_TERM_DAY2",      StringType(), True),
        StructField("PPOINT1",             StringType(), True),
        StructField("PPOINT2",             StringType(), True),
        StructField("PR_WAR",              StringType(), True),
        StructField("FOB_AK",              StringType(), True),
        StructField("FOB_HI",              StringType(), True),
        StructField("FOB_PR",              StringType(), True),
        StructField("FOB_US",              StringType(), True),
        StructField("X12_ID",              StringType(), True),
        StructField("APPR_DATE",           StringType(), True),
        StructField("TRANS_DATE",          StringType(), True),
        StructField("SOURCE_EDI",          StringType(), True),
        StructField("L_FILE",              StringType(), True),
        StructField("M_FILE",              StringType(), True),
        StructField("R_FILE",              StringType(), True),
        StructField("W_FILE",              StringType(), True),
        StructField("WARNUMBER",           StringType(), True),
        StructField("WARPERIOD",           StringType(), True),
        StructField("OCONTNUM",            StringType(), True),
        StructField("SPECTERMS",           StringType(), True),
        StructField("EFF_DATE",            StringType(), True),
        StructField("SUSPEND",             StringType(), True),
        StructField("LSA_CODE",            StringType(), True),
        StructField("WO_CODE",             StringType(), True),
        StructField("SIZE_CODE",           StringType(), True),
        StructField("MIN_CODE",            StringType(), True),
        StructField("FOB_CD",              StringType(), True),
        StructField("REF_IND",             StringType(), True),
        StructField("FILL",                StringType(), True),
        StructField("SDB_PROG",            StringType(), True),
        StructField("VET_OWNED_SMALL_BUS", StringType(), True),
        StructField("ATTRIBUTES_FLAG",     StringType(), True),
        StructField("HUBZ_SBC",            StringType(), True),
        StructField("DELIVERY_CODE",       StringType(), True),
        StructField("WOSB_CODE",           StringType(), True),
        StructField("EDWOSB_CODE",         StringType(), True),
        StructField("MMTI",                StringType(), True),
        StructField("BUS_ATTRIB",          StringType(), True),
        StructField("SSP_8A_EXIT_DATE",    StringType(), True),
        StructField("CON_EOL_DATE",        StringType(), True),
    ])

def build_schema_contract_zone():
    return StructType([
        StructField("VEND_ID",       StringType(), True),
        StructField("CONTRACT_NUM",  StringType(), True),
        StructField("STATE",         StringType(), True),
        StructField("SCHED_NUM",     StringType(), True),
        StructField("ZONE",          StringType(), True),
        StructField("MOD_DATE",      StringType(), True),
    ])

def build_schema_contracts():
    return StructType([
        StructField("CONTRACT_IDENTITY",             StringType(), True),
        StructField("CONTRACT_NUMBER",               StringType(), True),
        StructField("CONTRACT_END_DATE",             StringType(), True),
        StructField("BUSINESS_SIZE",                 StringType(), True),
        StructField("WOMAN_OWNED",                   StringType(), True),
        StructField("MINORITY_CODE",                 StringType(), True),
        StructField("DISADVANTAGED_CODE",            StringType(), True),
        StructField("LABOR_SETASIDE",                StringType(), True),
        StructField("COMPETITIVE_SOLICITATION_PROC", StringType(), True),
        StructField("SCHEDULE_NUMBER",               StringType(), True),
        StructField("SPECIAL_ITEM_NUMBER",           StringType(), True),
        StructField("CONTRACTOR_NAME",               StringType(), True),
        StructField("CONTRACTOR_ADDRESS1",           StringType(), True),
        StructField("CONTRACTOR_ADDRESS2",           StringType(), True),
        StructField("CONTRACTOR_ADDRESS3",           StringType(), True),
        StructField("CONTRACTOR_CITY",               StringType(), True),
        StructField("CONTRACTOR_STATE",              StringType(), True),
        StructField("CONTRACTOR_ZIP",                StringType(), True),
        StructField("CONTRACTOR_PHONE",              StringType(), True),
        StructField("CONTRACTOR_EMAIL",              StringType(), True),
        StructField("CONTRACTOR_URL",                StringType(), True),
        StructField("ADVANTAGE_ITEM",                StringType(), True),
        StructField("DATE_UPDATED",                  StringType(), True),
        StructField("MFR_IDENTITY",                  StringType(), True),
        StructField("VOSB",                          StringType(), True),
        StructField("BUYER_NAME",                    StringType(), True),
        StructField("BUYER_PHONE",                   StringType(), True),
        StructField("BUYER_EMAIL",                   StringType(), True),
        StructField("HUBZONE",                       StringType(), True),
        StructField("HUBZONE_SBC",                   StringType(), True),
        StructField("STLOC",                         StringType(), True),
        StructField("REF_TEXT",                      StringType(), True),
        StructField("ESB",                           StringType(), True),
        StructField("IS_GWAC",                       StringType(), True),
        StructField("DBA",                           StringType(), True),
        StructField("CONTRACTOR_COUNTRY",            StringType(), True),
        StructField("DUNS",                          StringType(), True),
        StructField("DISASTER_RECOVERY",             StringType(), True),
        StructField("ARRA",                          StringType(), True),
        StructField("EPLS",                          StringType(), True),
        StructField("NAICS",                         StringType(), True),
        StructField("DISPLAY_IN_ELIB",               StringType(), True),
        StructField("WOSB",                          StringType(), True),
        StructField("EDWOSB",                        StringType(), True),
        StructField("CONTRACT_BEGIN_DATE",           StringType(), True),
        StructField("TRIBALLY_OWNED_FIRM",           StringType(), True),
        StructField("AMERICAN_INDIAN_OWNED",         StringType(), True),
        StructField("NATIVE_ALASKAN_OWNED",          StringType(), True),
        StructField("NATIVE_HAWAIIAN_OWNED",         StringType(), True),
        StructField("IS_8A_SOURCE",                  StringType(), True),
        StructField("EXIT_DATE_8A_SOURCE",           StringType(), True),
        StructField("CONTRACT_CLOSE_DATE",           StringType(), True),
        StructField("IS_8A_JOINT_VENTURE",           StringType(), True),
        StructField("WOMEN_OWNED_JOINT_VENTURE",     StringType(), True),
        StructField("VETERAN_OWNED_JOINT_VENTURE",   StringType(), True),
        StructField("HUBZONE_JOINT_VENTURE",         StringType(), True),
        StructField("SBA_VOSB",                      StringType(), True),
        StructField("SBA_SDVOSB",                    StringType(), True),
    ])

def build_schema_gsin_hide_remove():
    return StructType([
        StructField("GSIN",           StringType(), True),
        StructField("STATUS",         StringType(), True),
        StructField("DATE_REQUESTED", StringType(), True),
        StructField("REASON",         StringType(), True),
        StructField("REQUESTED_BY",   StringType(), True),
    ])

def build_schema_gsin_hide_remove_hist():
    return StructType([
        StructField("GSIN",          StringType(), True),
        StructField("STATUS",        StringType(), True),
        StructField("CONTRACT_NUM",  StringType(), True),
        StructField("ITEM_NUM",      StringType(), True),
        StructField("MFR_NAME",      StringType(), True),
        StructField("VEND_PART",     StringType(), True),
        StructField("ITEM_NAME",     StringType(), True),
        StructField("STATUS_DATE",   StringType(), True),
        StructField("REASON",        StringType(), True),
        StructField("REQUESTED_BY",  StringType(), True),
    ])

def build_schema_item_xref():
    return StructType([
        StructField("ITEM_ID",            StringType(), True),
        StructField("ASS_NUM",            StringType(), True),
        StructField("SCHED_NUM",          StringType(), True),
        StructField("CONTRACT_NUM",       StringType(), True),
        StructField("ITEM_NUM",           StringType(), True),
        StructField("CATCODE",            StringType(), True),
        StructField("PCODE",              StringType(), True),
        StructField("NSN_SEARCH",         StringType(), True),
        StructField("NIIN_SEARCH",        StringType(), True),
        StructField("CONT_SEARCH",        StringType(), True),
        StructField("DUNS",               StringType(), True),
        StructField("VENDOR_NAME",        StringType(), True),
        StructField("MFR_NAME",           StringType(), True),
        StructField("ITEM_NAME",          StringType(), True),
        StructField("GOVT_NAME",          StringType(), True),
        StructField("PHOTOCODE",          StringType(), True),
        StructField("DESCRIPTION",        StringType(), True),
        StructField("ITEM_TYPE",          StringType(), True),
        StructField("ITEM_STATUS",        StringType(), True),
        StructField("OPTIONS_IND",        StringType(), True),
        StructField("ACCESSORY_IND",      StringType(), True),
        StructField("VISA_IND",           StringType(), True),
        StructField("DEL_DAYS1",          StringType(), True),
        StructField("DEL_DAYS2",          StringType(), True),
        StructField("WWW_ADDRESS",        StringType(), True),
        StructField("UOM",                StringType(), True),
        StructField("FOB_AK",             StringType(), True),
        StructField("FOB_HI",             StringType(), True),
        StructField("FOB_PR",             StringType(), True),
        StructField("FOB_US",             StringType(), True),
        StructField("FOB_CODE",           StringType(), True),
        StructField("LSA_CODE",           StringType(), True),
        StructField("MINORITY_OWNED",     StringType(), True),
        StructField("WOMAN_OWNED",        StringType(), True),
        StructField("BUSINESS_SIZE",      StringType(), True),
        StructField("ITEM_COLORS",        StringType(), True),
        StructField("DIMENSION",          StringType(), True),
        StructField("AAC",                StringType(), True),
        StructField("ITEM_CODE",          StringType(), True),
        StructField("DELIVERY_CODE",      StringType(), True),
        StructField("ALLIED_COMP",        StringType(), True),
        StructField("CHLORINE_FREE",      StringType(), True),
        StructField("ENERGY_EFFICIENT",   StringType(), True),
        StructField("ENERGY_STAR",        StringType(), True),
        StructField("LEAD_FREE",          StringType(), True),
        StructField("LOW_VOLATILE",       StringType(), True),
        StructField("NIB_NISH",           StringType(), True),
        StructField("OZONE_SAFE",         StringType(), True),
        StructField("RECYCLED_CONTENT",   StringType(), True),
        StructField("REMANUFACTURED",     StringType(), True),
        StructField("UNICORE",            StringType(), True),
        StructField("WATER_CONSERVING",   StringType(), True),
        StructField("NONE_CODE",          StringType(), True),
        StructField("OTHER_ENV",          StringType(), True),
        StructField("YEAR_2000",          StringType(), True),
        StructField("EMERGENCY_ITEM",     StringType(), True),
        StructField("ENVIRONMENTAL",      StringType(), True),
        StructField("SMALL_BUSINESS",     StringType(), True),
        StructField("WILDFIRE_ITEM",      StringType(), True),
        StructField("ITEM_PRICE",         StringType(), True),
        StructField("DISCOUNT",           StringType(), True),
        StructField("NSN",                StringType(), True),
        StructField("SIN",                StringType(), True),
        StructField("SIN_MAX_ORDER",      StringType(), True),
        StructField("SDB_PROG",           StringType(), True),
        StructField("ENVIRON_MESSAGE",    StringType(), True),
        StructField("CPG",                StringType(), True),
        StructField("VET_OWNED_SMALL_BUS", StringType(), True),
        StructField("BUS_ATTRIB",         StringType(), True),
        StructField("HUBZ_SBC",           StringType(), True),
        StructField("VEND_PART",          StringType(), True),
        StructField("PHOTO_GROUP_ID",     StringType(), True),
        StructField("UPC_ISBN_GTIN",      StringType(), True),
        StructField("GSIN",               StringType(), True),
    ])

def build_schema_item_xref_attributes():
    return StructType([
        StructField("CONTRACT_NUM",    StringType(), True),
        StructField("ITEM_NUM",        StringType(), True),
        StructField("URL_508",         StringType(), True),
        StructField("LAST_UPDATED",    StringType(), True),
        StructField("UPDATED_BY",      StringType(), True),
        StructField("UNID",            StringType(), True),
        StructField("SCAN_CODE1",      StringType(), True),
        StructField("SCAN_CODE2",      StringType(), True),
        StructField("SCAN_CODE3",      StringType(), True),
        StructField("TRUE_MFR_PART",   StringType(), True),
        StructField("PSC_CODE",        StringType(), True),
        StructField("MFR_NAME",        StringType(), True),
        StructField("CAGE_CODE",       StringType(), True),
        StructField("EULA_IND",        StringType(), True),
    ])

def build_schema_mp_product():
    return StructType([
        StructField("MP_TYPE_ID",        StringType(), True),
        StructField("GSIN",              StringType(), True),
        StructField("PRODUCT_NAME",      StringType(), True),
        StructField("DESCRIPTION",       StringType(), True),
        StructField("MFR_NAME",          StringType(), True),
        StructField("MFR_NAME_SEARCH",   StringType(), True),
        StructField("ITEM_NUM",          StringType(), True),
        StructField("UPC_ISBN_GTIN",     StringType(), True),
        StructField("PHOTO_GROUP_ID",    StringType(), True),
        StructField("UNSPSC",            StringType(), True),
        StructField("SOURCE_TYPE_ID",    StringType(), True),
        StructField("SOURCE_PRODUCT_ID", StringType(), True),
        StructField("STATUS",            StringType(), True),
        StructField("DATE_CREATED",      StringType(), True),
        StructField("LAST_MODIFIED",     StringType(), True),
        StructField("UOM",               StringType(), True),
        StructField("PHOTO_URL",         StringType(), True),
        StructField("DEL_DAYS1",         StringType(), True),
        StructField("DEL_DAYS2",         StringType(), True),
        StructField("UNSPSC_SOURCE",     StringType(), True),
    ])

def build_schema_order_status():
    return StructType([
        StructField("UNIQUE_ID",          StringType(), True),
        StructField("AWD_UNIQUE_ID",      StringType(), True),
        StructField("LINE_NUM",           StringType(), True),
        StructField("BV_ORDER_NUM",       StringType(), True),
        StructField("CONTRACT_NUM",       StringType(), True),
        StructField("PO_NUMBER_REQN_NUM", StringType(), True),
        StructField("NSN_MFR_PART",       StringType(), True),
        StructField("LINE_STATUS",        StringType(), True),
        StructField("PROCESS_CODE",       StringType(), True),
        StructField("STATUS_DATE",        StringType(), True),
        StructField("QUANTITY",           StringType(), True),
        StructField("MODE",               StringType(), True),
        StructField("MODE_URL",           StringType(), True),
        StructField("TRACKING_NUM",       StringType(), True),
        StructField("EST_SHIP_DATE",      StringType(), True),
        StructField("TCNGBL",             StringType(), True),
        StructField("DATE_CREATED",       StringType(), True),
        StructField("FSS19_PO_NUMBER",    StringType(), True),
        StructField("DISPLAY_FLAG",       StringType(), True),
    ])

def build_schema_price_discount():
    return StructType([
        StructField("VEND_ID",       StringType(), True),
        StructField("CONTRACT_NUM",  StringType(), True),
        StructField("MFR_PART",      StringType(), True),
        StructField("ZONE",          StringType(), True),
        StructField("DOLLAR_QTY",    StringType(), True),
        StructField("SEQ",           StringType(), True),
        StructField("SALE",          StringType(), True),
        StructField("SCHED_NUM",     StringType(), True),
        StructField("VEND_PART",     StringType(), True),
        StructField("LINE_NUM",      StringType(), True),
        StructField("ASS_NUM",       StringType(), True),
        StructField("MSG",           StringType(), True),
        StructField("QTY1",          StringType(), True),
        StructField("QTY2",          StringType(), True),
        StructField("DISC_PRICE",    StringType(), True),
        StructField("DISC_PCT",      StringType(), True),
        StructField("MOD_DATE",      StringType(), True),
        StructField("MFR_NAME",      StringType(), True),
    ])

def build_schema_product_file():
    return StructType([
        StructField("VEND_ID",                      StringType(), True),
        StructField("CONTRACT_NUM",                 StringType(), True),
        StructField("MFR_PART",                     StringType(), True),
        StructField("SCHED_NUM",                    StringType(), True),
        StructField("MFR_NAME",                     StringType(), True),
        StructField("PROD_NAME",                    StringType(), True),
        StructField("LINE_NUM",                     StringType(), True),
        StructField("VEND_PART",                    StringType(), True),
        StructField("PROD_DESC1",                   StringType(), True),
        StructField("PROD_DESC2",                   StringType(), True),
        StructField("SPEC_ITEM",                    StringType(), True),
        StructField("ASS_NUM",                      StringType(), True),
        StructField("PROD_LENGTH",                  StringType(), True),
        StructField("PROD_WIDTH",                   StringType(), True),
        StructField("PROD_HEIGHT",                  StringType(), True),
        StructField("PROD_DEPTH",                   StringType(), True),
        StructField("PROD_WEIGHT_OLD",              StringType(), True),
        StructField("PROD_DIMENSION",               StringType(), True),
        StructField("PROD_CUBE",                    StringType(), True),
        StructField("PROD_STDPACK",                 StringType(), True),
        StructField("INCREMENT",                    StringType(), True),
        StructField("DISCONTINUE",                  StringType(), True),
        StructField("WAR_TEXT",                     StringType(), True),
        StructField("WARNUMBER",                    StringType(), True),
        StructField("WARPERIOD",                    StringType(), True),
        StructField("BASEPART",                     StringType(), True),
        StructField("P_WWW",                        StringType(), True),
        StructField("MIN_ORDER",                    StringType(), True),
        StructField("MAX_ORDER",                    StringType(), True),
        StructField("DISC_TERM_AMT",                StringType(), True),
        StructField("DISC_TERM_PER",                StringType(), True),
        StructField("DISC_TERM_DAY",                StringType(), True),
        StructField("MAX_SHIP",                     StringType(), True),
        StructField("QTY",                          StringType(), True),
        StructField("QTY_PER_PACK",                 StringType(), True),
        StructField("CONDITIONS",                   StringType(), True),
        StructField("DELIVERY_DAYS1",               StringType(), True),
        StructField("PPOINT1",                      StringType(), True),
        StructField("PPOINT2",                      StringType(), True),
        StructField("ENERGY_STAR",                  StringType(), True),
        StructField("ALLIED_COMP",                  StringType(), True),
        StructField("PROD_ENVCODE",                 StringType(), True),
        StructField("PROD_ENVMSG1",                 StringType(), True),
        StructField("PROD_ENVMSG2",                 StringType(), True),
        StructField("VEND_NAME",                    StringType(), True),
        StructField("UOM",                          StringType(), True),
        StructField("FOB_AK",                       StringType(), True),
        StructField("FOB_HI",                       StringType(), True),
        StructField("FOB_PR",                       StringType(), True),
        StructField("FOB_US",                       StringType(), True),
        StructField("NSN",                          StringType(), True),
        StructField("MOD_DATE",                     StringType(), True),
        StructField("RECYCLED_CONTENT",             StringType(), True),
        StructField("ENERGY_EFFICIENT",             StringType(), True),
        StructField("LEAD_FREE",                    StringType(), True),
        StructField("WATER_CONSERVING",             StringType(), True),
        StructField("REMANUFACTURED",               StringType(), True),
        StructField("CHLORINE_FREE",                StringType(), True),
        StructField("OZONE_SAFE",                   StringType(), True),
        StructField("YEAR_2000",                    StringType(), True),
        StructField("UNICORE",                      StringType(), True),
        StructField("NIB_NISH",                     StringType(), True),
        StructField("NONE",                         StringType(), True),
        StructField("OTHER_ENV",                    StringType(), True),
        StructField("LOW_VOLATILE",                 StringType(), True),
        StructField("OPTIONS_IND",                  StringType(), True),
        StructField("ACCX_IND",                     StringType(), True),
        StructField("MAINT_IND",                    StringType(), True),
        StructField("LEASE_IND",                    StringType(), True),
        StructField("RENTAL_IND",                   StringType(), True),
        StructField("EWARR_IND",                    StringType(), True),
        StructField("PROD_WEIGHT",                  StringType(), True),
        StructField("PROD_CUBE_UOM",                StringType(), True),
        StructField("PROD_WEIGHT_UOM",              StringType(), True),
        StructField("PROD_LENGTH_WIDTH_HEIGHT_UOM", StringType(), True),
        StructField("QTY_UNIT_UOM",                 StringType(), True),
    ])

def build_schema_sin():
    return StructType([
        StructField("SIN_IDENTITY",        StringType(), True),
        StructField("SCHEDULE_NUMBER",     StringType(), True),
        StructField("SPECIAL_ITEM_NUMBER", StringType(), True),
        StructField("SIN_GROUP_TITLE",     StringType(), True),
        StructField("SIN_DESCRIPTION1",    StringType(), True),
        StructField("SIN_DESCRIPTION2",    StringType(), True),
        StructField("SIN_ORDER",           StringType(), True),
        StructField("CO_FNAME",            StringType(), True),
        StructField("CO_LNAME",            StringType(), True),
        StructField("CO_PHONE",            StringType(), True),
        StructField("CO_EMAIL",            StringType(), True),
        StructField("SIN_ANCILLARY",       StringType(), True),
        StructField("SIN_ANCRA",           StringType(), True),
        StructField("SIN_238910",          StringType(), True),
        StructField("SIN_OLM",             StringType(), True),
        StructField("COMPLIMENTARY_SIN",   StringType(), True),
        StructField("HIDE_IN_ELIB",        StringType(), True),
        StructField("HIDE_IN_EBUY",        StringType(), True),
        StructField("HIDE_IN_ELIB_DATE",   StringType(), True),
        StructField("HIDE_IN_EBUY_DATE",   StringType(), True),
    ])

def build_schema_sin_limit():
    return StructType([
        StructField("VEND_ID",       StringType(), True),
        StructField("CONTRACT_NUM",  StringType(), True),
        StructField("SPEC_ITEM",     StringType(), True),
        StructField("SCHED_NUM",     StringType(), True),
        StructField("LINE_NUM",      StringType(), True),
        StructField("MAX_ORDER",     StringType(), True),
        StructField("MIN_ORDER",     StringType(), True),
        StructField("ORDER_TYPE",    StringType(), True),
        StructField("MOD_DATE",      StringType(), True),
        StructField("SIN_DESC",      StringType(), True),
    ])

def build_schema_suspend_contract():
    return StructType([
        StructField("CONTRACT_NUM",  StringType(), True),
        StructField("SUSPEND_TYPE",  StringType(), True),
    ])

def build_schema_zone_price():
    return StructType([
        StructField("VEND_ID",       StringType(), True),
        StructField("CONTRACT_NUM",  StringType(), True),
        StructField("MFR_PART",      StringType(), True),
        StructField("ZONE",          StringType(), True),
        StructField("SCHED_NUM",     StringType(), True),
        StructField("VEND_PART",     StringType(), True),
        StructField("LINE_NUM",      StringType(), True),
        StructField("UNIT_PRICE",    StringType(), True),
        StructField("LIST_PRICE",    StringType(), True),
        StructField("SCHED_PRICE",   StringType(), True),
        StructField("SALE",          StringType(), True),
        StructField("SALE_PRICE",    StringType(), True),
        StructField("SALE_ST_DATE",  StringType(), True),
        StructField("SALE_END_DATE", StringType(), True),
        StructField("PROD_DISC",     StringType(), True),
        StructField("ASS_NUM",       StringType(), True),
        StructField("MOD_DATE",      StringType(), True),
        StructField("PROJ_CODE",     StringType(), True),
        StructField("LOC_CODE",      StringType(), True),
        StructField("PROG_CODE",     StringType(), True),
        StructField("MFR_NAME",      StringType(), True),
    ])

class BronzeIngestionDispatcher:
    # Routes table_name requests to inlined schemas and executes Bronze ingestion.

    def __init__(self):
        self.schema_builders = SCHEMA_BUILDERS

    def list_tables(self) -> List[str]:
        # Returns all supported table names accepted by this notebook.
        return sorted(self.schema_builders.keys())

    def _normalize_table_name(self, table_name: str) -> str:
        # Validates and normalizes the workflow-supplied table name.
        normalized = (table_name or "").strip().lower()
        if not normalized:
            raise ValueError("Widget 'table_name' is required when run_mode=single.")
        if normalized not in self.schema_builders:
            raise ValueError(f"Unsupported table_name='{normalized}'. Supported values: {self.list_tables()}")
        return normalized

    def run_table(self, table_name: str):
        # Executes Bronze ingestion for one table using its inlined schema.
        normalized = self._normalize_table_name(table_name)
        source_schema = self.schema_builders[normalized]()
        source_folder = SOURCE_FOLDER_OVERRIDES.get(normalized)
        print(f"[RUN] table={normalized} source_folder={source_folder or normalized}")
        start_ts = time.perf_counter()
        run_bronze_ingestion(normalized, source_schema, source_folder=source_folder)
        print(f"[DONE] table={normalized} elapsed_s={time.perf_counter() - start_ts:.2f}")

    def run_all(self, fail_fast: bool = True):
        # Executes all table ingestions in sorted order and optionally stops on first failure.
        failed = {}
        for t in self.list_tables():
            try:
                self.run_table(t)
            except Exception as exc:
                failed[t] = str(exc)
                print(f"[FAIL] table={t} error={exc}")
                if fail_fast:
                    raise
        if failed:
            print(f"[WARN] Completed with failures: {failed}")
        print(f"[COMPLETE] run_all finished. failed_count={len(failed)}")



if __name__ == '__main__':
    # ---- Main Entry Point ----
    # This block executes when the module is run directly (as a wheel entry point
    # or notebook). It parses runtime parameters, resolves environment/catalog,
    # and dispatches ingestion for the requested table(s).
    import sys as _sys

    def _get_param(name, default=""):
        # Notebook context: use dbutils.widgets. Wheel task context: fall back to sys.argv (--key=value).
        try:
            return dbutils.widgets.get(name).strip() or default
        except NameError:
            for arg in _sys.argv[1:]:
                if arg.startswith(f"--{name}="):
                    return arg.split("=", 1)[1].strip() or default
            return default

    try:
        dbutils.widgets.removeAll()
        dbutils.widgets.dropdown("env", "test", ["dev", "test", "prod"], "Environment")
        dbutils.widgets.text("schema", "bronze", "Target Schema")
        dbutils.widgets.dropdown("run_mode", "single", ["single", "all"], "Run Mode")
        dbutils.widgets.text("table_name", "", "Table Name (single mode)")
        dbutils.widgets.dropdown("fail_fast", "true", ["true", "false"], "Fail Fast (all mode)")
        dbutils.widgets.text("job_id", "", "Databricks Job ID")
        dbutils.widgets.text("job_run_id", "", "Databricks Job Run ID")
        dbutils.widgets.text("task_id", "", "Databricks Task ID")
        dbutils.widgets.text("task_run_id", "", "Databricks Task Run ID")
    except NameError:
        pass  # Not in notebook context — parameters come from sys.argv (--key=value)

    env = EnvironmentConfig.get_environment(_get_param("env", "test"))
    schema = _get_param("schema", "bronze")
    catalog = EnvironmentConfig.get_catalog(env)
    run_mode = _get_param("run_mode", "single").lower()
    table_name = _get_param("table_name")
    fail_fast = _get_param("fail_fast", "true").lower() == "true"

    job_ids = {
        "job_id": _get_param("job_id"),
        "job_run_id": _get_param("job_run_id"),
        "task_id": _get_param("task_id"),
        "task_run_id": _get_param("task_run_id"),
    }

    print(f"[INFO] env={env} catalog={catalog} schema={schema} run_mode={run_mode}")

    # Schema builders inlined from _generated_schemas.py — no filesystem access required.

    print("[INFO] All 19 schema builders defined.")


    SOURCE_FOLDER_OVERRIDES = {
        "adv_product": "ADV_PRODUCT",
    }

    SCHEMA_BUILDERS = {
        "adv_product": build_schema_adv_product,
        "bpa_header": build_schema_bpa_header,
        "bpa_item": build_schema_bpa_item,
        "bpa_item_price": build_schema_bpa_item_price,
        "catalog_832": build_schema_catalog_832,
        "contract_zone": build_schema_contract_zone,
        "contracts": build_schema_contracts,
        "gsin_hide_remove": build_schema_gsin_hide_remove,
        "gsin_hide_remove_hist": build_schema_gsin_hide_remove_hist,
        "item_xref": build_schema_item_xref,
        "item_xref_attributes": build_schema_item_xref_attributes,
        "mp_product": build_schema_mp_product,
        "order_status": build_schema_order_status,
        "price_discount": build_schema_price_discount,
        "product_file": build_schema_product_file,
        "sin": build_schema_sin,
        "sin_limit": build_schema_sin_limit,
        "suspend_contract": build_schema_suspend_contract,
        "zone_price": build_schema_zone_price,
    }

    dispatcher = BronzeIngestionDispatcher()
    if run_mode == "all":
        dispatcher.run_all(fail_fast=fail_fast)
    else:
        dispatcher.run_table(table_name)

    print("[COMPLETE] Unified Bronze ingestion notebook finished.")
