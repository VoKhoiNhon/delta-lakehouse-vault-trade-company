

# I. Dev Flow

## General
1. config meta file:
  - Bronze (don't need meta.collumns)
  - Silver (must has meta.collumns)

```yaml
model:
  table_name: h_company
  database_name: database_name
  data_location: s3a://lakehouse-silver/h_company
  unique_key: ["jurisdiction", "dv_hashkey_company"]
  data_format: delta
  partition_by:
  - jurisdiction

  columns:
    - name: dv_hashkey_company
      type: string
      nullable: false
      description: dv hashkey

    - name: dv_recsrc
      type: string
      nullable: false
      description: dv columns

    - name: dv_loaddts
      nullable: false
      type: timestamp
      description: dv columns

    - name: dv_source_version
      nullable: false
      type: string
      description: dv columns

    - name: jurisdiction
      type: string
      nullable: false

    - name: registration_number
      type: string
    - name: name
      type: string
      nullable: false

    - name: pure_name
      type: string
      nullable: false

    - name: has_regnum
      type: integer

input_resources:
- table_name: manual_company
  format: delta
  data_location: s3a://lakehouse-bronze/manual/company
  record_source: bronze.manual_company
  process_job: jobs/silver/h_company/h_company__src_manual_company.py
```
2. coding
jobs/silver/h_company/h_company__src_bangladesh_company.py

3. tesing
tests/jobs/silver/h_company/test_h_company.py


## 1. Bronze - clean, transform

1. config meta file (don't need meta.collumns)
2. coding - clean data
- clean comapny name - must have
- clean jurisdiction - must have - jurisdiction must follow: resources/lookup_jurisdiction_with_continents.csv
```py
## common clean
df = add_load_date_columns(df)
df = clean_column_names(df)
df = clean_string_columns(df)

## clean comapny name - must have
df = add_pure_company_name(df)

## clean jurisdiction - must have
df = .. custom function clean jurisdiction
```

## 2. Silver - data classification


## 3. Gold - business model


## 4. orchestration
Every job that consumes data directly from raw sources must accept a parameter named dv_source_versionEvery job that consumes data directly from raw sources must accept a parameter named **dv_source_version**

Raw data is stored in the lakehouse under the following path: /lakehouse-raw/source/{dv_source_version}/

Example: For dv_source_version = 'source1_ver202412', the data would be located at /lakehouse-raw/source/source1_ver202412/

*Setup AIRFLOW DAG + test*


# II. Deploy runbook
## 1. dv_recsrc: ???
## 2. dv_source_verson: ???
## 3. merge request:
  - repo code: delta-lakehouse-vault
  - repo dag: lakehouse-orchestration-airflow
## 4. Run step:
  - dag run : dag_name ... (or notebook if airflow not done)
## 5. reconcilation & quality check:
**Set rules to check the quality of the tables you will add data to**

```py
from libs.reconcile import ReconcilitionSuite, SqlCountPair
dv_recsrc = ''
dv_source_verson = ''

reconcile_suites = [
    {
        "sources": ["silver.s_company_demographic"],
        "targets": ["silver.h_company"],
        "source_sql": f"select COUNT(DISTINCT dv_hashkey_company) as cnt_distinct from  delta.`s3a://lakehouse-silver/s_company_demographic`",
        "target_sql": "select count(1) as cnt_distinct from delta.`s3a://lakehouse-silver/h_company`"
    },
    {
        "sources": ["silver.h_company"],
        "targets": ["silver.l_person_company_relationship"],
        "source_sql": "select 0 as cnt",
        "target_sql": f"""select count(1) as cnt
        from delta.`s3a://lakehouse-silver/l_person_company_relationship` l
        where not exists (select 1 from  delta.`s3a://lakehouse-silver/h_company` h where l.dv_hashkey_company = h.dv_hashkey_company)
          and dv_recsrc='{dv_recsrc}' and dv_source_verson='{dv_source_verson}'
        """,
    },
    {
        "sources": ["silver.h_company"],
        "targets": ["silver.l_company_company_relationship"],
        "source_sql": "select 0 as cnt_from",
        "target_sql": "f""select count(1) as cnt_from
        from delta.`s3a://lakehouse-silver/l_company_company_relationship` l
        where not exists (select 1 from  delta.`s3a://lakehouse-silver/h_company` h where l.dv_hashkey_from_company = h.dv_hashkey_company)
        and dv_recsrc='{dv_recsrc}' and dv_source_verson='{dv_source_verson}' """,
    },
    {
        "sources": ["silver.h_company"],
        "targets": ["silver.l_company_company_relationship"],
        "source_sql": "select 0 as cnt_to",
        "target_sql": f"""select count(1) as cnt_to
        from delta.`s3a://lakehouse-silver/l_company_company_relationship` l
        where not exists (select 1 from  delta.`s3a://lakehouse-silver/h_company` h where l.dv_hashkey_to_company = h.dv_hashkey_company)
         and dv_recsrc='{dv_recsrc}' and dv_source_verson='{dv_source_verson}'""",
    },
    {
        "sources": ["silver.h_company"],
        "targets": ["silver.l_bol_buyer"],
        "source_sql": "select 0 as cnt, 0 as cnt_distinct",
        "target_sql": f"""select count(distinct buyer_dv_hashkey_company) as cnt_distinct, count(1) as cnt
        from delta.`s3a://lakehouse-silver/l_bol` l
        where not exists (select 1 from  delta.`s3a://lakehouse-silver/h_company` h where l.buyer_dv_hashkey_company = h.dv_hashkey_company)
        and dv_recsrc='{dv_recsrc}' and dv_source_verson='{dv_source_verson}'""",
    },
    {
        "sources": ["silver.h_company"],
        "targets": ["silver.l_bol_supplier"],
        "source_sql": "select 0 as cnt, 0 as cnt_distinct",
        "target_sql": f"""select count(distinct supplier_dv_hashkey_company) as cnt_distinct, count(1) as cnt
        from delta.`s3a://lakehouse-silver/l_bol` l
        where not exists (select 1 from  delta.`s3a://lakehouse-silver/h_company` h where l.supplier_dv_hashkey_company = h.dv_hashkey_company)
        and dv_recsrc='{dv_recsrc}' and dv_source_verson='{dv_source_verson}'""",
    },
    {
        "sources": ["silver.h_company"],
        "targets": ["silver.l_sanction_company"],
        "source_sql": "select 0 as cnt, 0 as cnt_distinct",
        "target_sql": f"""select count(distinct dv_hashkey_company) as cnt_distinct, count(1) as cnt
        from delta.`s3a://lakehouse-silver/l_sanction_company` l
        where not exists (select 1 from  delta.`s3a://lakehouse-silver/h_company` h where l.dv_hashkey_company = h.dv_hashkey_company)
        and dv_recsrc='{dv_recsrc}' and dv_source_verson='{dv_source_verson}'""",
    }
]

rer = ReconcilitionSuite(spark, SqlCountPair, suite=reconcile_suites)
rs_df, summary_df, _ = rer.validate(output_format="dataframe")
rs_df.drop("run_id", "run_time", "source_sql", "target_sql", "jinja_data").show(20, False)
```

## 6. rollback plan:
**Set rules to check the quality of the tables you will update**
- Find the latest version of each Silver Table you will add data to:
- Prepare a restoreToVersion script for each

```py
from delta.tables import DeltaTable

# ------------------------------------- #
# Load Delta table h_company
delta_table = DeltaTable.forPath(spark, "s3a://lakehouse-silver/h_company")
print(delta_table.toDF().count())
delta_table.history().select('version', 'timestamp', 'operation','operationParameters', ).show(20, False)

# restoreToVersion
delta_table.restoreToVersion(5)
print(delta_table.toDF().count())
delta_table.history().select('version', 'timestamp', 'operation','operationParameters', ).show(20, False)

# ------------------------------------- #
# Load Delta table s_company_demographic
delta_table = DeltaTable.forPath(spark, "s3a://lakehouse-silver/h_company")
print(delta_table.toDF().count())
delta_table.history().select('version', 'timestamp', 'operation','operationParameters', ).show(20, False)

# restoreToVersion
delta_table.restoreToVersion(5)
print(delta_table.toDF().count())
delta_table.history().select('version', 'timestamp', 'operation','operationParameters', ).show(20, False)
```


# III. Some bussiness rules:

## Trade data:
- bol records must have at least seller or buyer
- if null seller or buyer keep the import or export jurisdiction, and replace null to Withheld by Government

## Company data:
- companies must have name

## Person:
- person must has name and address