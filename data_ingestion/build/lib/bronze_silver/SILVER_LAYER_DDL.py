# Databricks notebook source
# MAGIC %md
# MAGIC ## Executive Summary
# MAGIC
# MAGIC This notebook provisions the complete Silver layer schema for the FOIA pipeline in Unity Catalog.
# MAGIC
# MAGIC - Creates 19 Delta tables in the target `foia_tst.silver`.
# MAGIC - Uses typed Silver table definitions with `CREATE OR REPLACE TABLE`.
# MAGIC - Applies Delta table settings for column mapping and writer/reader compatibility.
# MAGIC - Uses `CLUSTER BY` for key access patterns per table.
# MAGIC - Is self-contained and independent from Bronze DDL notebooks/files at runtime.
# MAGIC - Intended to run before Bronze-to-Silver ingestion jobs so target tables are ready.
# MAGIC
# MAGIC # Silver Layer DDL - Table Definitions
# MAGIC
# MAGIC | Field | Value |
# MAGIC |-------|-------|
# MAGIC | **Developed by** | Adu Erena |
# MAGIC | **Date** | 2026-05-27 |
# MAGIC | **Version** | 1.0 |
# MAGIC
# MAGIC **Description:** Creates all Silver layer Delta tables for the FAS Advantage pipeline. Each table is created using `CREATE OR REPLACE TABLE` with Delta format, clustering (`CLUSTER BY`), and column mapping enabled.
# MAGIC
# MAGIC **Tables created:**
# MAGIC
# MAGIC | # | Table | Columns | Cluster Columns |
# MAGIC |---|-------|---------|-----------------------|
# MAGIC | 1 | `bpa_header` | 42 | bpa_number, contract_number, store_id, id |
# MAGIC | 2 | `bpa_item` | 10 | bpa_id, item_num, status |
# MAGIC | 3 | `bpa_item_price` | 6 | item_id, start_range, end_range |
# MAGIC | 4 | `catalog_832` | 63 | vend_id, contract_num |
# MAGIC | 5 | `contract_zone` | 6 | vend_id, contract_num, state |
# MAGIC | 6 | `gsin_hide_remove` | 5 | gsin |
# MAGIC | 7 | `gsin_hide_remove_hist` | 10 | gsin, status_date |
# MAGIC | 8 | `mp_product` | 20 | gsin, unspsc_source |
# MAGIC | 9 | `price_discount` | 18 | vend_id, contract_num, mfr_part, zone |
# MAGIC | 10 | `sin_limit` | 10 | vend_id, contract_num, spec_item |
# MAGIC | 11 | `suspend_contract` | 2 | contract_num |
# MAGIC | 12 | `zone_price` | 21 | vend_id, contract_num, mfr_part, zone |
# MAGIC | 13 | `contracts` | 60 | schedule_number, special_item_number, contractor_name |
# MAGIC | 14 | `sin` | 20 | schedule_number, special_item_number |
# MAGIC | 15 | `adv_product` | 83 | adv_contract_num, prod_id, adv_item_num, deleted |
# MAGIC | 16 | `item_xref` | 74 | contract_num, item_id, item_num, sched_num |
# MAGIC | 17 | `product_file` | 76 | vend_id, contract_num, mfr_part, sched_num |
# MAGIC | 18 | `order_status` | 19 | po_number_reqn_num, bv_order_num, awd_unique_id, date_created |
# MAGIC | 19 | `item_xref_attributes` | 14 | contract_num, item_num, mfr_name |
# MAGIC
# MAGIC **Target objects:** catalog `foia_tst`, schema `silver`.
# MAGIC
# MAGIC > Run this notebook before Silver ingestion to ensure all target tables exist with the expected datatypes.

# COMMAND ----------

# Silver DDL notebook is intentionally self-contained and has no dependency
# on BRONZE_LAYER_DDL notebooks/files for table creation logic.

# COMMAND ----------

import re

# Project-required database objects for Silver.
catalog = "foia_tst"
bronze_schema = "bronze"
silver_schema = "silver"

print(f"[INFO] Silver DDL using catalog={catalog}, source_schema={bronze_schema}, target_schema={silver_schema}")

# COMMAND ----------

# spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{silver_schema}`")

print(f"[INFO] DDL target: {catalog}.{silver_schema}")

# COMMAND ----------

def _exec_ddl(table_name: str, ddl_sql: str):
    # Normalize accidental trailing comma before COMMENT for valid Spark SQL DDL.
    ddl_sql = re.sub(r",\s*\nCOMMENT", "\nCOMMENT", ddl_sql)
    spark.sql(ddl_sql)
    print(f"[OK] Created or replaced {catalog}.{silver_schema}.{table_name}")

# 1) bpa_header
_exec_ddl("bpa_header", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.bpa_header (
    id BIGINT NOT NULL,
    bpa_category_id INT,
    bpa_number STRING NOT NULL,
    contract_number STRING NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    created_by STRING NOT NULL,
    description STRING,
    store_id INT NOT NULL,
    poc_first_name STRING,
    poc_last_name STRING,
    poc_phone STRING,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    url STRING,
    last_mod_time TIMESTAMP,
    updated_by STRING,
    schedule_discount INT,
    status INT NOT NULL,
    batch_number BIGINT,
    vendor_name STRING,
    service BOOLEAN DEFAULT false NOT NULL,
    poc_email STRING,
    bpa_notes STRING,
    quotes INT,
    ebuy_quotes STRING,
    users STRING,
    adv_status BOOLEAN DEFAULT false NOT NULL,
    bpa_schedule STRING,
    bpa_sin STRING,
    disp_in_ebuy BOOLEAN DEFAULT false NOT NULL,
    bpa_min_order DECIMAL(18,2) DEFAULT 0 NOT NULL,
    std_delivery_time INT DEFAULT 0 NOT NULL,
    std_delivery_time_2 INT DEFAULT 0 NOT NULL,
    delivery_code STRING DEFAULT '' NOT NULL,
    next_day_delivery STRING,
    desktop_delivery STRING,
    secure_desktop_delivery STRING,
    check_upiid_format BOOLEAN DEFAULT false NOT NULL,
    convenience_fee STRING,
    next_day_delivery_flat_rate STRING,
    desktop_delivery_flat_rate STRING,
    secure_delivery_flat_rate STRING
)
USING DELTA
CLUSTER BY (bpa_number, contract_number, store_id, id)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for BPA_HEADER'
""")

# 2) bpa_item
_exec_ddl("bpa_item", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.bpa_item (
    id BIGINT NOT NULL,
    bpa_id BIGINT NOT NULL,
    item_num STRING NOT NULL,
    assnum BIGINT,
    line_number STRING,
    stock_indicator INT,
    item_price DECIMAL(18,2) NOT NULL,
    pricing_program BIGINT,
    status INT,
    mfr_name STRING
)
USING DELTA
CLUSTER BY (bpa_id, item_num, status)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for BPA_ITEM'
""")

# 3) bpa_item_price
_exec_ddl("bpa_item_price", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.bpa_item_price (
    id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    start_range INT NOT NULL,
    end_range INT NOT NULL,
    discount_percent INT,
    price DECIMAL(18,2)
)
USING DELTA
CLUSTER BY (item_id, start_range, end_range)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for BPA_ITEM_PRICE'
""")

# 4) catalog_832
_exec_ddl("catalog_832", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.catalog_832 (
    vend_id STRING NOT NULL,
    contract_num STRING NOT NULL,
    sched_num STRING,
    catalog_num STRING,
    agency_name STRING,
    buyer_code STRING,
    con_stdate TIMESTAMP,
    con_enddate TIMESTAMP,
    cc_disc DECIMAL(4,2),
    disc_term_amt INT,
    disc_term_per DECIMAL(4,2),
    disc_term_day STRING,
    fob STRING,
    max_battery DECIMAL(18,2),
    max_nsp DECIMAL(18,2),
    max_ship INT,
    mop STRING,
    min_order DECIMAL(18,2),
    max_order DECIMAL(18,2),
    delivery_days1 INT,
    delivery_days2 INT,
    zone_flag BOOLEAN NOT NULL,
    prompt_pay BOOLEAN NOT NULL,
    mod_date TIMESTAMP,
    disc_term_per2 DECIMAL(4,2),
    disc_term_day2 STRING,
    ppoint1 STRING,
    ppoint2 STRING,
    pr_war STRING,
    fob_ak STRING,
    fob_hi STRING,
    fob_pr STRING,
    fob_us STRING,
    x12_id STRING,
    appr_date TIMESTAMP,
    trans_date TIMESTAMP,
    source_edi STRING,
    l_file STRING,
    m_file STRING,
    r_file STRING,
    w_file STRING,
    warnumber STRING,
    warperiod STRING,
    ocontnum STRING,
    specterms INT,
    eff_date TIMESTAMP,
    suspend INT,
    lsa_code STRING,
    wo_code STRING,
    size_code STRING,
    min_code STRING,
    fob_cd STRING,
    ref_ind STRING,
    fill STRING,
    sdb_prog STRING,
    vet_owned_small_bus STRING,
    attributes_flag TINYINT DEFAULT 0,
    hubz_sbc STRING,
    delivery_code STRING,
    wosb_code STRING,
    edwosb_code STRING,
    mmti STRING,
    bus_attrib STRING,
    ssp_8a_exit_date DATE,
    con_eol_date TIMESTAMP
)
USING DELTA
CLUSTER BY (vend_id, contract_num)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for CATALOG_832'
""")

# 5) contract_zone
_exec_ddl("contract_zone", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.contract_zone (
    vend_id STRING NOT NULL,
    contract_num STRING NOT NULL,
    state STRING NOT NULL,
    sched_num STRING,
    zone DECIMAL(2,0),
    mod_date TIMESTAMP
)
USING DELTA
CLUSTER BY (vend_id, contract_num, state)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for CONTRACT_ZONE'
""")

# 6) gsin_hide_remove
_exec_ddl("gsin_hide_remove", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.gsin_hide_remove (
    gsin DECIMAL(14,0) NOT NULL,
    status STRING NOT NULL,
    date_requested TIMESTAMP,
    reason STRING,
    requested_by STRING
)
USING DELTA
CLUSTER BY (gsin)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for GSIN_HIDE_REMOVE'
""")

# 7) gsin_hide_remove_hist
_exec_ddl("gsin_hide_remove_hist", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.gsin_hide_remove_hist (
    gsin DECIMAL(14,0) NOT NULL,
    status STRING NOT NULL,
    contract_num STRING NOT NULL,
    item_num STRING NOT NULL,
    mfr_name STRING NOT NULL,
    vend_part STRING,
    item_name STRING,
    status_date STRING NOT NULL,
    reason STRING,
    requested_by STRING
)
USING DELTA
CLUSTER BY (gsin, status_date)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for GSIN_HIDE_REMOVE_HIST'
""")

# 8) mp_product
_exec_ddl("mp_product", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.mp_product (
    mp_type_id INT NOT NULL,
    gsin DECIMAL(14,0) NOT NULL,
    product_name STRING,
    description STRING,
    mfr_name STRING,
    mfr_name_search STRING,
    item_num STRING,
    upc_isbn_gtin STRING,
    photo_group_id STRING,
    unspsc STRING,
    source_type_id INT NOT NULL,
    source_product_id INT,
    status INT,
    date_created TIMESTAMP DEFAULT current_timestamp() NOT NULL,
    last_modified TIMESTAMP DEFAULT current_timestamp(),
    uom STRING,
    photo_url STRING,
    del_days1 INT,
    del_days2 INT,
    unspsc_source TINYINT DEFAULT 0 NOT NULL
)
USING DELTA
CLUSTER BY (gsin, unspsc_source)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for MP_PRODUCT'
""")

# 9) price_discount
_exec_ddl("price_discount", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.price_discount (
    vend_id STRING NOT NULL,
    contract_num STRING NOT NULL,
    mfr_part STRING NOT NULL,
    zone DECIMAL(2,0) NOT NULL,
    dollar_qty INT NOT NULL,
    seq INT NOT NULL,
    sale STRING NOT NULL,
    sched_num STRING,
    vend_part STRING,
    line_num STRING,
    ass_num DECIMAL(14,0),
    msg STRING,
    qty1 INT,
    qty2 INT,
    disc_price DECIMAL(18,2),
    disc_pct DECIMAL(4,2),
    mod_date TIMESTAMP,
    mfr_name STRING DEFAULT 'blank' NOT NULL
)
USING DELTA
CLUSTER BY (vend_id, contract_num, mfr_part, zone)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for PRICE_DISCOUNT'
""")

# 10) sin_limit
_exec_ddl("sin_limit", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.sin_limit (
    vend_id STRING NOT NULL,
    contract_num STRING NOT NULL,
    spec_item STRING NOT NULL,
    sched_num STRING,
    line_num STRING,
    max_order DECIMAL(18,2),
    min_order DECIMAL(18,2),
    order_type STRING,
    mod_date TIMESTAMP,
    sin_desc STRING
)
USING DELTA
CLUSTER BY (vend_id, contract_num, spec_item)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for SIN_LIMIT'
""")

# 11) suspend_contract
_exec_ddl("suspend_contract", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.suspend_contract (
    contract_num STRING NOT NULL,
    suspend_type INT DEFAULT 1 NOT NULL
)
USING DELTA
CLUSTER BY (contract_num)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for SUSPEND_CONTRACT'
""")

# 12) zone_price
_exec_ddl("zone_price", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.zone_price (
    vend_id STRING NOT NULL,
    contract_num STRING NOT NULL,
    mfr_part STRING NOT NULL,
    zone DECIMAL(2,0) NOT NULL,
    sched_num STRING,
    vend_part STRING,
    line_num STRING,
    unit_price DECIMAL(18,2),
    list_price DECIMAL(18,2),
    sched_price DECIMAL(18,2),
    sale BOOLEAN NOT NULL,
    sale_price DECIMAL(18,2),
    sale_st_date TIMESTAMP,
    sale_end_date TIMESTAMP,
    prod_disc BOOLEAN NOT NULL,
    ass_num DECIMAL(14,0),
    mod_date TIMESTAMP,
    proj_code STRING,
    loc_code STRING,
    prog_code STRING,
    mfr_name STRING
)
USING DELTA
CLUSTER BY (vend_id, contract_num, mfr_part, zone)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for ZONE_PRICE'
""")

# 13) contracts
_exec_ddl("contracts", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.contracts (
    contract_identity BIGINT NOT NULL,
    contract_number STRING,
    contract_end_date STRING NOT NULL,
    business_size STRING,
    woman_owned STRING,
    minority_code STRING,
    disadvantaged_code STRING,
    labor_setaside STRING,
    competitive_solicitation_proc STRING,
    schedule_number STRING NOT NULL,
    special_item_number STRING NOT NULL,
    contractor_name STRING NOT NULL,
    contractor_address1 STRING,
    contractor_address2 STRING,
    contractor_address3 STRING,
    contractor_city STRING,
    contractor_state STRING,
    contractor_zip STRING,
    contractor_phone STRING,
    contractor_email STRING,
    contractor_url STRING,
    advantage_item STRING NOT NULL,
    date_updated TIMESTAMP NOT NULL,
    mfr_identity BIGINT,
    vosb STRING,
    buyer_name STRING,
    buyer_phone STRING,
    buyer_email STRING,
    hubzone STRING,
    hubzone_sbc STRING,
    stloc STRING,
    ref_text STRING,
    esb STRING,
    is_gwac STRING,
    dba STRING,
    contractor_country STRING,
    duns STRING,
    disaster_recovery STRING DEFAULT 'N' NOT NULL,
    arra STRING DEFAULT 'N' NOT NULL,
    epls STRING DEFAULT 'N' NOT NULL,
    naics STRING,
    display_in_elib STRING DEFAULT 'Y' NOT NULL,
    wosb STRING DEFAULT 'N' NOT NULL,
    edwosb STRING DEFAULT 'N' NOT NULL,
    contract_begin_date STRING DEFAULT '' NOT NULL,
    tribally_owned_firm STRING DEFAULT 'N' NOT NULL,
    american_indian_owned STRING DEFAULT 'N' NOT NULL,
    native_alaskan_owned STRING DEFAULT 'N' NOT NULL,
    native_hawaiian_owned STRING DEFAULT 'N' NOT NULL,
    is_8a_source STRING DEFAULT 'N' NOT NULL,
    exit_date_8a_source STRING DEFAULT '' NOT NULL,
    contract_close_date STRING DEFAULT '' NOT NULL,
    is_8a_joint_venture STRING,
    women_owned_joint_venture STRING,
    veteran_owned_joint_venture STRING,
    hubzone_joint_venture STRING,
    sba_vosb STRING,
    sba_sdvosb STRING
)
USING DELTA
CLUSTER BY (schedule_number, special_item_number, contractor_name)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for CONTRACTS'
""")

# 14) sin
_exec_ddl("sin", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.sin (
    sin_identity BIGINT NOT NULL,
    schedule_number STRING NOT NULL,
    special_item_number STRING NOT NULL,
    sin_group_title STRING,
    sin_description1 STRING,
    sin_description2 STRING,
    sin_order DOUBLE,
    co_fname STRING,
    co_lname STRING,
    co_phone STRING,
    co_email STRING,
    sin_ancillary STRING,
    sin_ancra STRING,
    sin_238910 STRING,
    sin_olm STRING,
    complimentary_sin STRING DEFAULT '0' NOT NULL,
    hide_in_elib STRING DEFAULT 'N' NOT NULL,
    hide_in_ebuy STRING DEFAULT 'N' NOT NULL,
    hide_in_elib_date TIMESTAMP,
    hide_in_ebuy_date TIMESTAMP
)
USING DELTA
CLUSTER BY (schedule_number, special_item_number)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for SIN'
""")

# 15) adv_product
_exec_ddl("adv_product", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.adv_product (
    oid DECIMAL(14,0) NOT NULL,
    prod_id STRING NOT NULL,
    store_id INT NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    status INT NOT NULL,
    deleted INT NOT NULL,
    last_mod_time TIMESTAMP NOT NULL,
    name STRING NOT NULL,
    dept STRING,
    price DECIMAL(18,2) NOT NULL,
    stock INT,
    preview_image STRING,
    preview_image_width INT,
    preview_image_height INT,
    full_image STRING,
    full_image_width INT,
    full_image_height INT,
    audio_file STRING,
    video_file STRING,
    sdesc2 STRING,
    lprice DECIMAL(18,2),
    longdesc STRING,
    appdef1 STRING,
    appdef2 STRING,
    appdef3 STRING,
    rating DOUBLE DEFAULT 0,
    no_votes INT DEFAULT 0,
    total_rating DOUBLE DEFAULT 0,
    adv_ass_num STRING NOT NULL,
    adv_item_num STRING NOT NULL,
    adv_sched_num STRING NOT NULL,
    adv_contract_num STRING NOT NULL,
    adv_catcode STRING NOT NULL,
    adv_duns STRING,
    adv_vendor_name STRING,
    adv_govt_name STRING,
    adv_item_type STRING NOT NULL,
    adv_options_flg INT NOT NULL,
    adv_accessory_flg INT NOT NULL,
    adv_unit STRING NOT NULL,
    adv_aac STRING,
    adv_item_code STRING,
    adv_delivery_code STRING,
    adv_allied_comp_flg INT NOT NULL,
    adv_chlorine_free_flg INT NOT NULL,
    adv_energy_efficient_flg INT NOT NULL,
    adv_lead_free_flg INT NOT NULL,
    adv_energy_star_flg INT NOT NULL,
    adv_low_volatile_flg INT NOT NULL,
    adv_ozone_safe_flg INT NOT NULL,
    adv_nib_nish_flg INT NOT NULL,
    adv_recycled_content_flg INT NOT NULL,
    adv_unicore_flg INT NOT NULL,
    adv_water_conserving_flg INT NOT NULL,
    adv_none_code_flg INT NOT NULL,
    adv_other_env_flg INT NOT NULL,
    adv_year_2000_flg INT,
    adv_environmental_flg INT,
    adv_wildfire_item_flg INT NOT NULL,
    adv_small_business_flg INT NOT NULL,
    adv_discount_flg INT NOT NULL,
    adv_nsn STRING,
    adv_manufacture_name STRING NOT NULL,
    adv_visa_flg INT,
    adv_del_days_low INT,
    adv_del_days_high INT,
    adv_minority_owned STRING,
    adv_woman_owned STRING,
    adv_color_flg INT,
    adv_vendor_url STRING,
    adv_business_size STRING,
    adv_lsa_code STRING,
    adv_bus_attrib STRING,
    adv_dimension STRING,
    adv_sin_num STRING,
    adv_sin_max_order DECIMAL(18,2),
    adv_photo_code STRING,
    adv_sdb_prog STRING,
    adv_environ_message STRING,
    adv_cpg INT,
    adv_vet_owned_small_bus STRING,
    adv_product STRING,
    adv_pcode STRING,
    price_indicator TINYINT
)
USING DELTA
CLUSTER BY (adv_contract_num, prod_id, adv_item_num, deleted)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for ADV_PRODUCT'
""")

# 16) item_xref
_exec_ddl("item_xref", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.item_xref (
    item_id DECIMAL(10,0) NOT NULL,
    ass_num DECIMAL(14,0) NOT NULL,
    sched_num STRING,
    contract_num STRING,
    item_num STRING,
    catcode STRING,
    pcode STRING,
    nsn_search STRING,
    niin_search STRING,
    cont_search STRING,
    duns STRING,
    vendor_name STRING,
    mfr_name STRING,
    item_name STRING,
    govt_name STRING,
    photocode STRING,
    description STRING,
    item_type STRING,
    item_status STRING,
    options_ind INT,
    accessory_ind INT,
    visa_ind INT,
    del_days1 INT,
    del_days2 INT,
    www_address STRING,
    uom STRING,
    fob_ak STRING,
    fob_hi STRING,
    fob_pr STRING,
    fob_us STRING,
    fob_code STRING,
    lsa_code STRING,
    minority_owned STRING,
    woman_owned STRING,
    business_size STRING,
    item_colors STRING,
    dimension STRING,
    aac STRING,
    item_code STRING,
    delivery_code STRING,
    allied_comp INT,
    chlorine_free INT,
    energy_efficient INT,
    energy_star INT,
    lead_free INT,
    low_volatile INT,
    nib_nish INT,
    ozone_safe INT,
    recycled_content INT,
    remanufactured INT,
    unicore INT,
    water_conserving INT,
    none_code INT,
    other_env INT,
    year_2000 INT,
    emergency_item INT,
    environmental INT,
    small_business INT,
    wildfire_item INT,
    item_price DECIMAL(19,4) DEFAULT 0.0,
    discount INT DEFAULT 0,
    nsn STRING,
    sin STRING,
    sin_max_order DECIMAL(19,4),
    sdb_prog STRING,
    environ_message STRING,
    cpg INT,
    vet_owned_small_bus STRING,
    bus_attrib STRING,
    hubz_sbc STRING,
    vend_part STRING,
    photo_group_id STRING,
    upc_isbn_gtin STRING,
    gsin DECIMAL(14,0)
)
USING DELTA
CLUSTER BY (contract_num, item_id, item_num, sched_num)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for ITEM_XREF'
""")

# 17) product_file
_exec_ddl("product_file", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.product_file (
    vend_id STRING,
    contract_num STRING,
    mfr_part STRING,
    sched_num STRING,
    mfr_name STRING,
    prod_name STRING,
    line_num STRING,
    vend_part STRING,
    prod_desc1 STRING,
    prod_desc2 STRING,
    spec_item STRING,
    ass_num DECIMAL(14,0),
    prod_length STRING,
    prod_width STRING,
    prod_height STRING,
    prod_depth STRING,
    prod_weight_old DECIMAL(7,3),
    prod_dimension STRING,
    prod_cube DECIMAL(9,3),
    prod_stdpack STRING,
    increment STRING,
    discontinue STRING,
    war_text STRING,
    warnumber STRING,
    warperiod STRING,
    basepart STRING,
    p_www STRING,
    min_order DECIMAL(10,4),
    max_order DECIMAL(10,4),
    disc_term_amt INT,
    disc_term_per DECIMAL(4,2),
    disc_term_day STRING,
    max_ship INT,
    qty INT,
    qty_per_pack STRING,
    conditions STRING,
    delivery_days1 INT,
    ppoint1 STRING,
    ppoint2 STRING,
    energy_star BOOLEAN,
    allied_comp BOOLEAN,
    prod_envcode STRING,
    prod_envmsg1 STRING,
    prod_envmsg2 STRING,
    vend_name STRING,
    uom STRING,
    fob_ak STRING,
    fob_hi STRING,
    fob_pr STRING,
    fob_us STRING,
    nsn STRING,
    mod_date TIMESTAMP,
    recycled_content BOOLEAN,
    energy_efficient BOOLEAN,
    lead_free BOOLEAN,
    water_conserving BOOLEAN,
    remanufactured BOOLEAN,
    chlorine_free BOOLEAN,
    ozone_safe BOOLEAN,
    year_2000 BOOLEAN,
    unicore BOOLEAN,
    nib_nish BOOLEAN,
    none_flag BOOLEAN,
    other_env BOOLEAN,
    low_volatile BOOLEAN,
    options_ind INT,
    accx_ind INT,
    maint_ind INT,
    lease_ind INT,
    rental_ind INT,
    ewarr_ind INT,
    prod_weight DECIMAL(11,3),
    prod_cube_uom STRING,
    prod_weight_uom STRING,
    prod_length_width_height_uom STRING,
    qty_unit_uom STRING
)
USING DELTA
CLUSTER BY (vend_id, contract_num, mfr_part, sched_num)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
),
COMMENT 'Delta table for PRODUCT_FILE'
""")

# 18) order_status
_exec_ddl("order_status", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.order_status (
    unique_id DECIMAL(10,0) NOT NULL,
    awd_unique_id DECIMAL(9,0),
    line_num INT,
    bv_order_num DECIMAL(10,0) NOT NULL,
    contract_num STRING,
    po_number_reqn_num STRING NOT NULL,
    nsn_mfr_part STRING NOT NULL,
    line_status STRING,
    process_code STRING,
    status_date TIMESTAMP NOT NULL,
    quantity INT NOT NULL,
    mode STRING,
    mode_url STRING,
    tracking_num STRING,
    est_ship_date TIMESTAMP,
    tcngbl STRING,
    date_created TIMESTAMP DEFAULT current_timestamp(),
    fss19_po_number STRING,
    display_flag TINYINT DEFAULT 1 NOT NULL
)
USING DELTA
CLUSTER BY (po_number_reqn_num, bv_order_num, awd_unique_id, date_created)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for ORDER_STATUS'
""")

# 19) item_xref_attributes
_exec_ddl("item_xref_attributes", f"""
CREATE OR REPLACE TABLE {catalog}.{silver_schema}.item_xref_attributes (
    contract_num STRING NOT NULL,
    item_num STRING NOT NULL,
    url_508 STRING NOT NULL,
    last_updated TIMESTAMP DEFAULT current_timestamp(),
    updated_by STRING,
    unid STRING,
    scan_code1 STRING,
    scan_code2 STRING,
    scan_code3 STRING,
    true_mfr_part STRING,
    psc_code STRING,
    mfr_name STRING,
    cage_code STRING,
    eula_ind BOOLEAN DEFAULT false NOT NULL
)
USING DELTA
CLUSTER BY (contract_num, item_num, mfr_name)
TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.feature.allowColumnDefaults' = 'supported'
),
COMMENT 'Delta table for ITEM_XREF_ATTRIBUTES'
""")

print("[DONE] Silver layer DDL completed using embedded, independent SQL definitions.")