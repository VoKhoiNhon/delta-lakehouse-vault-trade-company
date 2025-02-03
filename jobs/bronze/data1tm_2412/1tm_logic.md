# Company
```py

entity_df = spark.read.format("delta").load("s3a://warehouse/data/all/entity").where("is_person = false")
entity_address_df = spark.read.format("delta").load("s3a://warehouse/data/all/entity_address")
entity_status_df = spark.read.format("delta").load("s3a://warehouse/data/all/entity_status")
status_df = spark.read.format("delta").load("s3a://warehouse/data/all/status")
address_df = spark.read.format("delta").load("s3a://warehouse/data/all/address").alias("address")
person_df = spark.read.format("delta").load("s3a://warehouse/data/all/person").alias("person")
company_detail_df = spark.read.format("delta").load("s3a://warehouse/data/all/company_detail")
company_industry_df = spark.read.format("delta").load("s3a://warehouse/data/all/company_industry")
industry_df = spark.read.format("delta").load("s3a://warehouse/data/all/industry")
metadata_df = spark.read.format("delta").load("s3a://warehouse/data/all/metadata")
entity_gov = spark.read.format("delta").load('s3a://lakehouse-raw/1tm_2412/entity_gov')

all_company_joined = (
    entity_df.alias('e')
    .join(company_detail_df.alias('detail'), 'id', 'left' )
    .join(entity_address_df.alias('address'), [F.col('e.id') == F.col('address.entity_id')], 'left' )
    .join(address_df.alias('address_detail'), [F.col('address.address_id') == F.col('address_detail.id')], 'left' )
    .join(company_industry_df.alias('industry'), [F.col('e.id') == F.col('industry.company_id')], 'left')
    .join(industry_df.alias('industry_detail'), [F.col('industry.industry_id') == F.col('industry_detail.id')], 'left')
    .join(entity_status_df.alias('status'), [F.col('e.id') == F.col('status.entity_id')], 'left')
    .join(status_df.alias('status_detail'), [F.col('status.status_id') == F.col('status_detail.id')], 'left')
    .join(metadata_df.alias('metadata'), [F.col('e.id') == F.col('metadata.entity_id')], 'left')
    .join(entity_gov.where("is_person = false").select("id").alias('gov'), [F.col('e.id') == F.col('gov.id')], 'left')
)

df = all_company_joined.selectExpr(
    'e.id',
    'jurisdiction', 'name', 'registration_number',
    'full_address', 'street',
    'city', 'region', 'state', 'province',
    'e.country_code', 'country_name', 'postal_code',
    'latitude', 'longitude', 'address.type',
    'lei_code', 'description', 'date_incorporated',
    'date_struck_off', 'legal_form', 'category', 'phone_numbers', 'emails',
    'websites', 'linkedin_url', 'twitter_url', 'facebook_url', 'fax_numbers',
    'other_names', 'no_of_employees', 'image_url', 'authorised_capital',
    'paid_up_capital', 'currency_code', 'status', 'status_code',
    'status_desc', 'is_branch', 'is_sanctioned', 'is_trade',
    'industry_code', 'desc', 'industry_detail.country_code as industry_country_code',
    'metadata.data_source as data_source', 'metadata.link as data_source_link',
    'case when gov.id is not null then 1 else 0 end as is_gov_source'
)
df = clean_jurisdiciton_1tm(spark, df)

lookup_jurisdiction = self.spark.createDataFrame(
            pd.read_csv("resources/lookup_jurisdiction.csv", na_filter=False)
        )
df = (
    df.alias("c")
    .join(F.broadcast(lookup_jurisdiction).alias("l"), "jurisdiction", "left")
    .selectExpr(
        "c.*",
        "l.country_code as lookup_country_code",
        "l.country_name as lookup_country_name",
    )
    .withColumn("country_code", F.col("lookup_country_code"))
    .withColumn("country_name", F.col("lookup_country_name"))
    .withColumn(
        "jurisdiction",
        F.expr(
            """
    case when jurisdiction in ('Global', 'European Union')  then 'unspecified'
    when jurisdiction is null then 'unspecified'
    else jurisdiction end
"""
        ),
    )
    .drop("lookup_country_code", "lookup_country_name")
)

table_path = 's3a://lakehouse-raw/1tm_2412/company_full'
df.write.format('delta').mode('overwrite').save(table_path)
```
# Person

```py
from pyspark.sql import functions as F
metadata_df = spark.read.format("delta").load("s3a://warehouse/data/all/metadata")
person_raw = spark.read.format("delta").load("s3a://lakehouse-raw/person")
bronze_person = spark.read.format("delta").load("s3a://lakehouse-bronze/1tm_2412/person")

joined_df = bronze_person.alias('e').join(
    metadata_df.alias('metadata'),
    F.col('e.id') == F.col('metadata.entity_id'),
   'left'
).selectExpr(
    'e.*', 'metadata.data_source as data_source2', 'metadata.link as data_source_link2'
).withColumn(
    "data_source",
    F.expr("data_source2")
).withColumn(
    "data_source_link",
    F.expr("data_source_link2")
).withColumn(
    "jurisdiction",
    F.expr("""
    case when jurisdiction = 'Georgia' and country_code = 'US' then 'US.Georgia'
         when jurisdiction in ('Global', 'European Union')  then 'Unknown'
         when jurisdiction is null then 'Unknown'
    else jurisdiction end""")
).drop("data_source2", "data_source_link2")d.createOrReplaceTempView('tmp1')

lookup_jurisdiction = spark.read.format('csv').load(
    'resources/lookup_jurisdiction.csv',
     header=True
).createOrReplaceTempView('lookup')
(
    joined_df.write.mode('overwrite')
    .format('delta')
    .option("delta.autoOptimize.optimizeWrite", "true")
    .save("s3a://lakehouse-raw/1tm_2412/person")
)

```py
entity_df = spark.read.format("delta").load("s3a://warehouse/data/all/entity")
entity_address_df = spark.read.format("delta").load("s3a://warehouse/data/all/entity_address")
all_address_joined = (
    entity_address_df.alias('address')
    .join(entity_df.alias('e'), [F.col('e.id') == F.col('address.entity_id')], 'inner' )
)
all_company_joined.selectExpr(
    "address.entity_id as id",
    "address.start_date",
    "address.end_date",
    "e.is_person"
).withColumn(
    "row_number",
    F.expr(
        f"""
        ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY
            CASE WHEN start_date IS NULL THEN 1 ELSE 0 END,
            CASE WHEN end_date IS NULL THEN 1 ELSE 0 END
    )"""
    ),
).where("row_number = 1 and start_date is not null or end_date is not null").drop("row_number")\
    .write.format('delta').mode('overwrite').option("delta.autoOptimize.optimizeWrite", "true") \
    .save("s3a://lakehouse-raw/1tm_2412/entity_address")
```

from pyspark.sql.functions import when, col
from delta.tables import DeltaTable
# Initialize DeltaTable
deltaTable = DeltaTable.forPath(spark, "s3a://lakehouse-bronze/1tm_2412/person")

# Perform the update operation
deltaTable.update(
    condition = None,  # Update all rows
    set = {
        "jurisdiction": when((col("jurisdiction") == "Georgia") & (col("country_code") == "US"), "US.Georgia")
                       .when(col("jurisdiction").isin("Global", "European Union"), "Unknown")
                       .when(col("jurisdiction").isNull(), "Unknown")
                       .otherwise(col("jurisdiction"))
    }
)
```

# Entity_link
1. person_unique_id, company_unique_id
2. entity_link_with_flag ->
    - entity_link_compvscomp
    - entity_link_personvscomp
    - entity_link_personvsperson
+ filter hashkey exists h_company + h_person


```py
sql_query = """
WITH ranked_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY
                CASE WHEN jurisdiction IS NULL THEN 1 ELSE 0 END,
                CASE WHEN registration_number IS NULL THEN 1 ELSE 0 END,
                CASE WHEN name IS NULL THEN 1 ELSE 0 END,
                CASE WHEN pure_name IS NULL THEN 1 ELSE 0 END
        ) as row_num
    FROM delta.`s3a://lakehouse-bronze/1tm_2412/company`
)
SELECT
    id,
    jurisdiction,
    registration_number,
    name,
    pure_name,
    md5(concat_ws(';', jurisdiction, coalesce(registration_number, null), pure_name)) as DV_HASHKEY_COMPANY
FROM ranked_data
WHERE row_num = 1
"""

spark.sql(sql_query).drop("row_num").write.format('delta') \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .mode('overwrite').save("s3a://lakehouse-bronze/1tm_2412/company_unique_id")




# Ưu tiên các row không null (False sẽ được sắp xếp trước True)
sql_query = """
WITH ranked_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY
                CASE WHEN jurisdiction IS NULL THEN 1 ELSE 0 END,
                CASE WHEN name IS NULL THEN 1 ELSE 0 END,
                CASE WHEN full_address IS NULL THEN 1 ELSE 0 END
        ) as row_num
    FROM delta.`s3a://lakehouse-bronze/1tm_2412/person`
)
SELECT
    id,
    jurisdiction,
    name,
    full_address,
    md5(concat_ws(';', jurisdiction, name, coalesce(full_address, null))) as DV_HASHKEY_PERSON


FROM ranked_data
WHERE row_num = 1
"""
spark.sql(sql_query).drop("row_num").write.format('delta') \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .mode('overwrite').save("s3a://lakehouse-bronze/1tm_2412/person_unique_id")


spark.sql("""
select el.id, el.from_entity_id, el.to_entity_id, el.related_type, el.position, el.position_code, el.start_date, el.end_date, el.created_at, el.updated_at,

cf.jurisdiction from_jurisdiction, cf.registration_number from_registration_number, cf.name from_name, cf.pure_name from_pure_name,
ct.jurisdiction to_jurisdiction, ct.registration_number to_registration_number, ct.name to_name, ct.pure_name to_pure_name,
cf.DV_HASHKEY_COMPANY as from_hashkey_company,
ct.DV_HASHKEY_COMPANY as to_hashkey_company,
case when cf.id is null then 0 else 1 end as from_is_comp,
case when ct.id is null then 0 else 1 end as to_is_comp
from delta.`s3a://warehouse/data/all/entity_link` el
left join delta.`s3a://lakehouse-bronze/1tm_2412/company_unique_id` cf on el.from_entity_id = cf.id
left join delta.`s3a://lakehouse-bronze/1tm_2412/company_unique_id` ct on el.to_entity_id = ct.id
""").write.format('delta') \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .partitionBy("from_is_comp","to_is_comp") \
    .mode('overwrite').save("s3a://lakehouse-bronze/1tm_2412/entity_link_with_flag")


spark.sql("""
select *
from delta.`s3a://lakehouse-bronze/1tm_2412/entity_link_with_flag`
where from_is_comp = 1 and to_is_comp = 1
-- where cf.name is not null and ct.name is not null
""").write.format('delta') \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .mode('overwrite').save("s3a://lakehouse-bronze/1tm_2412/entity_link_compvscomp")

spark.sql("""
select el.id, el.related_type, el.position, el.position_code, el.start_date, el.end_date, el.created_at, el.updated_at,
cf.jurisdiction from_jurisdiction, cf.name from_name, cf.full_address from_full_address,
el.to_jurisdiction, el.to_registration_number, el.to_name, el.to_pure_name,
cf.DV_HASHKEY_PERSON as from_hashkey_person,
el.to_hashkey_company

from delta.`s3a://lakehouse-bronze/1tm_2412/entity_link_with_flag` el
inner join delta.`s3a://lakehouse-bronze/1tm_2412/person_unique_id` cf on el.from_entity_id = cf.id
where from_is_comp = 0 and to_is_comp = 1
""").write.format('delta') \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .mode('overwrite').save("s3a://lakehouse-bronze/1tm_2412/entity_link_personvscomp")

spark.sql("""
select el.id, el.related_type, el.position, el.position_code, el.start_date, el.end_date, el.created_at, el.updated_at,
cf.jurisdiction from_jurisdiction, cf.name from_name, cf.full_address from_full_address,
ct.jurisdiction to_jurisdiction, ct.name to_name, ct.full_address to_full_address,
md5(concat_ws(';', cf.jurisdiction, cf.name, coalesce(cf.full_address, null))) as from_hashkey_person,
md5(concat_ws(';', ct.jurisdiction, ct.name, coalesce(ct.full_address, null))) as to_hashkey_person

from delta.`s3a://lakehouse-bronze/1tm_2412/entity_link_with_flag` el
inner join delta.`s3a://lakehouse-bronze/1tm_2412/person_unique_id` cf on el.from_entity_id = cf.id
inner join delta.`s3a://lakehouse-bronze/1tm_2412/person_unique_id` ct on el.to_entity_id = ct.id
where from_is_comp = 0 and to_is_comp = 0
""").write.format('delta') \
    .option("delta.autoOptimize.optimizeWrite", "true") \
    .mode('overwrite').save("s3a://lakehouse-bronze/1tm_2412/entity_link_personvsperson")
```