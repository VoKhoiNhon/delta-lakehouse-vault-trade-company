

1. apply pure_name
-> compare has_regis vs has_regis -> insert (row number vs tên dài nhất và không có ký tự đặc biệt)
- phải cùng  jurisdiction, registration_number mới được merge

compare no_regis vs no_regis -> insert (row number vs dài nhất)



Ưu tiên các row không null (False sẽ được sắp xếp trước True)
sql_query = """
WITH ranked_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY jurisdiction, pure_name
            ORDER BY
                CASE WHEN registration_number IS NULL THEN 1 ELSE 0 END
        ) as row_num
    FROM delta.`s3a://lakehouse-bronze/1tm_2412/person`
)


pure_name_no_regis_vs_no_regis
pure_name_has_regis_vs_has_regis
pure_name_no_regis_vs_has_regis
pure_name_cross_no_regis_vs_has_regis


thực hiện việc verify/match các companies dựa trên 3 patterns:

1. verify_regex_name_has_regis_vs_has_regis:
Match các companies có registration number
Dùng jurisdiction + registration_number + regexed_name để match
Ưu tiên theo: có reg number > ít ký tự đặc biệt > tên dài hơn

2. verify_regex_name_no_regis_vs_no_regis:
Match các companies không có registration number
Dùng jurisdiction + regexed_name để match
Ưu tiên: ít ký tự đặc biệt > tên dài hơn

3. verify_regex_name_no_regis_vs_has_regis:
Cross-match giữa companies có và không có registration
Match dựa trên jurisdiction + regexed_name



# regexed_name_no_regis_vs_no_regis vs levenshtein_pure_name_no_regis_vs_has_regis
-  bắc cầu verified_key của no_regis_vs_no_regis sang levenshtein_pure_name

```py
from libs.utils.delta_utils import merge_df_with_schema, delta_insert_for_hubnlink


l_company_verification_path = "s3a://lakehouse-silver/l_company_verification"
regex_path = "s3a://lakehouse-silver/company_verification/regex"
levenshtein_path = "s3a://lakehouse-silver/company_verification/levenshtein"


# step 1 - regex insert

regex_df = spark.sql(f"""
select *
from delta.`{regex_path}`
""")

delta_insert_for_hubnlink(
    spark,
    df=regex_df,
    data_location = l_company_verification_path,
    base_cols = ['jurisdiction', 'dv_hashkey_company'],
    struct_type = delta_table.toDF().schema,
)

# step 2 - levenshtein update regex
update_df = spark.sql(f"""
select t.jurisdiction, t.dv_hashkey_company, t.dv_recsrc, t.dv_source_version, t.name, t.pure_name,
    v.verified_dv_hashkey_company, v.verified_registration_number, v.verified_pure_name, t.verified_regexed_name, v.verified_name
from delta.`{levenshtein_path}` v
inner join delta.`{regex_path}` t
    on v.dv_hashkey_company = t.verified_dv_hashkey_company
where t.verify_method = 'regexed_name_no_regis_vs_no_regis'
""")


from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, l_company_verification_path)

delta_table.alias("base").merge(
    update_df.alias("updates"),
    "base.dv_hashkey_company = updates.dv_hashkey_company and base.verify_method = 'regexed_name_no_regis_vs_no_regis'",
).whenMatchedUpdate(
    set={
        "verify_method": F.lit("cross_levenshtein_regexed_name"),
        "verified_dv_hashkey_company": "updates.verified_dv_hashkey_company",
        "verified_registration_number": "updates.verified_registration_number",
        "verified_pure_name": "updates.verified_pure_name",
        "verified_regexed_name": "updates.verified_regexed_name",
    }
).execute()

# step 3 - levenshtein insert
insert_df = spark.sql(f"""
select le.*
from delta.`{levenshtein_path}` le
where not exists (select 1 from delta.`{regex_path}` l where le.dv_hashkey_company = l.verified_dv_hashkey_company)
""")

delta_insert_for_hubnlink(
    spark,
    df=insert_df,
    data_location = l_company_verification_path,
    base_cols = ['jurisdiction', 'dv_hashkey_company'],
    struct_type = delta_table.toDF().schema,
)
```