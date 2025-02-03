
## levenshtein
```py
import time
from jobs.silver.l_company_verification.l_company_verification_by_levenshtein import run as run_levenshtein_verify
from jobs.silver.l_company_verification.l_company_verification_by_pure_name import run as run_pure_name_verify
start_time = time.time()
run_levenshtein_verify(
    env="pro",
    params={},
    spark=None,
    n_group=12,
    to_verify_filter="dv_recsrc = 'dataforseo;https://downloads.dataforseo.com/databasesV3/google_business/f74b708b-0c8f-40b1-a4e3-69d6da446d9a/list.txt'"
)
end_time = time.time()
execution_time = (end_time - start_time) / 60
print(f"Function  took {execution_time:.2f} minutes to execute")
```

## optimize
```py
from delta.tables import DeltaTable
vault_silver_tbls = [
    # "s3a://lakehouse-silver/h_company",
    # "s3a://lakehouse-silver/s_company_address",
    # "s3a://lakehouse-silver/s_company_demographic",
    # "s3a://lakehouse-silver/h_person",
    # "s3a://lakehouse-silver/s_person_address",
    # "s3a://lakehouse-silver/s_person_demographic",
    # "s3a://lakehouse-silver/l_bol",
    # "s3a://lakehouse-silver/s_bol",
    # "s3a://lakehouse-silver/l_company_company_relationship",
    # "s3a://lakehouse-silver/l_person_company_relationship",
    # "s3a://lakehouse-silver/l_person_person_relationship",
    # "s3a://lakehouse-silver/h_industry",

    # "s3a://lakehouse-raw/1tm_2412/company_industry",
    # "s3a://lakehouse-bronze/1tm_2412/company_unique_id"


    # "s3a://lakehouse-bronze/1tm_2412/industry"
    "s3a://lakehouse-silver/tmp/company_verified_key_bridge",
    "s3a://lakehouse-silver/tmp/company_demomgraphic_verified_bridge",
    # "s3a://lakehouse-silver/l_company_verification"
]
for tbl_path in vault_silver_tbls:
    deltaTable = DeltaTable.forPath(spark, tbl_path)
    deltaTable.optimize().executeCompaction()
    deltaTable.optimize().executeZOrderBy("jurisdiction", "dv_recsrc")
    # if "dv_recsrc" in deltaTable.toDF().columns:
    #     deltaTable.optimize().executeZOrderBy("dv_recsrc")
    # spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled=false")
    deltaTable.vacuum(7)
```


## Split run
```py
from libs.utils.commons import get_n_group_jurisdiction
import time
from pyspark.sql.functions import col,explode, split, lit, collect_list, struct
import pyspark.sql.functions as F
def func_s_person_address(df):
    start_time = time.time()
    df.groupBy("jurisdiction", "dv_hashkey_person").agg(
        collect_list(
            struct(
                col("type"),
                lit(None).cast("timestamp").alias("start_date"),
                lit(None).cast("timestamp").alias("end_date"),
                col("full_address"),
                col("street"),
                col("city"),
                col("region"),
                col("state"),
                col("province"),
                col("country_code"),
                col("postal_code"),
                col("latitude"),
                col("longitude"))
        ).alias("addresses")
    ).write.format("delta").mode("append").save("s3a://lakehouse-gold/tmp/s_person_address")
    end_time = time.time()
    execution_time = (end_time - start_time) / 60
    print(f"Function took {execution_time:.2f} minutes to execute")

def run_with_juris_splited(df, n_group, func):
    groups = get_n_group_jurisdiction(df, n_group)
    for group in groups:
        print(
            "group_number:", group["group_number"], "total_cnt:", group["total_cnt"]
        )
        jurisdictions = group["jurisdictions"]
        if len(jurisdictions) < 10:
            print("jurisdictions filter with:", jurisdictions)
            juris_df = df.where(
                F.col("jurisdiction").isin(jurisdictions)
            )
        else:
            print("jurisdictions with dataframe:")
            from pyspark.sql.types import StringType, StructType, StructField
            schema = StructType([StructField("jurisdiction", StringType(), True)])
            spark = df.sparkSession
            jurisdiction_df = spark.createDataFrame(
                [[j] for j in jurisdictions], schema=schema
            )
            juris_df = df.join(jurisdiction_df, "jurisdiction", "inner")

        func(juris_df)

s_company_address = spark.read.format("delta").load("s3a://lakehouse-silver/s_person_address")
run_with_juris_splited(s_company_address, 10, func_s_person_address)
```


Here’s your report translated into English for your boss:

Overview:

Here is the Overview:
- Our old data has 108 million records, and Dataforseo has 206 million. After merging, we have 265 million records, with 156 million from Dataforseo.

- We cleaned our old data, reducing over 3000 jurisdictions to 300+, and removed irrelevant ports from trade data, leaving over 3000 standard ports.

- Dat's data adds 220M+ records to the existing 60M.



To complete the data push, we’ve divided the process into 3 steps:

1. Data Processing: Clean the data, categorize it, and add necessary logic. This step requires a solid understanding of the data and technical skills for processing.

2. Mapping Processed Data: Map the processed data to the format that can be used by the backend. This step isn't difficult, but if the data is as large as Dataforseo’s, it requires substantial resources and advanced techniques (I am currently monitoring and writing code to improve this).

1. Pushing the Data: The data is pushed into two databases: Postgres and Elasticsearch. This is the most time consuming step.


Current Status:

All old data and Dataforseo data are in step 3. At the current speed, it will be completed by the end of tomorrow at the latest. I’m still looking for ways to speed up the process, and if all goes well, it may be finished by tomorrow afternoon.

The trade data from Mr. Dat is being processed in step 2 by Phong. With the amount of data, it will take about 8-10 hours to complete the data push. If Phong finishes step 2 tomorrow, everything will be completed before January 3rd.

The Colombia company data is being processed by Nhon in step 2. If Nhon finishes step 2 tomorrow, everything will be completed before January 3rd.

