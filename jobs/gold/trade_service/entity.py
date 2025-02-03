from pyspark.sql import functions as F

from libs.executers.gold_executer import GoldTradeExecuter
from libs.meta import TableMeta

# from libs.utils.commons import get_n_group_jurisdiction
# from libs.utils.delta_utils import delta_upsert_for_hubnlink
from libs.utils.boto3 import delete_s3_folder_with_spark_sesion


class EntityExecuter(GoldTradeExecuter):
    # def transform(self, jurisdictions=[]):
    def transform(self):
        bridge_company_key = self.input_dataframe_dict[
            "silver.bridge_company_key"
        ].dataframe
        sat_company = self.input_dataframe_dict[
            "silver.bridge_company_demomgraphic"
        ].dataframe
        # if len(jurisdictions) > 0 and len(jurisdictions) < 10:
        #     print("jurisdictions filter with:", jurisdictions)
        #     sat_company = sat_company.where(F.col("jurisdiction").isin(jurisdictions))
        #     bridge_company_key = bridge_company_key.where(
        #         F.col("jurisdiction").isin(jurisdictions)
        #     )
        # else:
        #     print("jurisdictions with dataframe:")
        #     from pyspark.sql.types import StringType, StructType, StructField

        #     schema = StructType([StructField("jurisdiction", StringType(), True)])
        #     jurisdiction_df = self.spark.createDataFrame(
        #         [[j] for j in jurisdictions], schema=schema
        #     )
        #     sat_company = sat_company.join(jurisdiction_df, "jurisdiction", "inner")
        #     bridge_company_key = bridge_company_key.join(
        #         jurisdiction_df, "jurisdiction", "inner"
        #     )
        l_sanction_company = self.input_dataframe_dict[
            "silver.l_sanction_company"
        ].dataframe
        l_bol = self.input_dataframe_dict["silver.l_bol"].dataframe

        l_bol.createOrReplaceTempView("l_bol")
        l_sanction_company.createOrReplaceTempView("l_sanction_company")
        bridge_company_key.createOrReplaceTempView("bridge_company_key")
        sat_company.createOrReplaceTempView("sat_company")
        source_flags = self.spark.sql(
            """
            SELECT DISTINCT
                company_key,
                type
            FROM (
                SELECT buyer_dv_hashkey_company as company_key, 'buyer' as type
                FROM l_bol
                WHERE buyer_dv_hashkey_company IS NOT NULL

                UNION ALL

                SELECT supplier_dv_hashkey_company as company_key, 'supplier'
                FROM l_bol
                WHERE supplier_dv_hashkey_company IS NOT NULL

                UNION ALL

                SELECT dv_hashkey_company as company_key, 'sanction'
                FROM l_sanction_company
            )
        """
        )
        delete_s3_folder_with_spark_sesion(
            self.spark, "s3a://lakehouse-silver/tmp/source_flags"
        )

        source_flags.write.format("delta").option(
            "delta.autoOptimize.optimizeWrite", "true"
        ).save("s3a://lakehouse-silver/tmp/source_flags")
        source_flags = self.spark.read.format("delta").load(
            "s3a://lakehouse-silver/tmp/source_flags"
        )
        source_flags.createOrReplaceTempView("source_flags")
        aggregated_flags = self.spark.sql(
            """
WITH expanded_keys AS (
    SELECT jurisdiction, to_key, from_key as check_key FROM bridge_company_key
    UNION ALL
    SELECT jurisdiction, to_key, to_key as check_key FROM bridge_company_key
),
joined_flags AS (
    SELECT
        e.jurisdiction,
        e.to_key,
        src.type
    FROM expanded_keys e
    LEFT JOIN source_flags src ON src.company_key = e.check_key
)
SELECT
    jurisdiction,
    to_key,
    MAX(type = 'buyer') as is_trade_buyer,
    MAX(type = 'supplier') as is_trade_supplier,
    MAX(type = 'sanction') as is_sanction
FROM joined_flags
GROUP BY jurisdiction, to_key
        """
        )
        delete_s3_folder_with_spark_sesion(
            self.spark, "s3a://lakehouse-silver/tmp/aggregated_flags"
        )
        aggregated_flags.write.format("delta").option(
            "delta.autoOptimize.optimizeWrite", "true"
        ).save("s3a://lakehouse-silver/tmp/aggregated_flags")
        aggregated_flags = self.spark.read.format("delta").load(
            "s3a://lakehouse-silver/tmp/aggregated_flags"
        )
        aggregated_flags.createOrReplaceTempView("aggregated_flags")
        import pandas as pd

        lookup_jurisdiction = self.spark.createDataFrame(
            pd.read_csv("resources/lookup_jurisdiction.csv", na_filter=False)
        )
        lookup_jurisdiction.createOrReplaceTempView("lookup_jurisdiction")
        result = self.spark.sql(
            f"""
            SELECT /*+ BROADCASTJOIN(l) */
                c.dv_source_version,
                c.dv_recsrc,
                c.dv_hashkey_company as id,
                FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts,
                c.name,
                c.description,
                c.lei_code,
                c.date_incorporated,
                c.date_struck_off,
                CASE
                    WHEN l.is_state = true THEN split(c.jurisdiction, '\\.')[1]
                    WHEN c.jurisdiction is null THEN 'unspecified'
                    ELSE c.jurisdiction
                END as jurisdiction,
                c.legal_form,
                c.category,
                c.phone_numbers,
                c.emails,
                c.websites,
                c.linkedin_url,
                c.twitter_url,
                c.facebook_url,
                c.fax_numbers,
                c.other_names,
                c.registration_number,
                CASE
                    WHEN l.country_name is null THEN 'unspecified'
                    ELSE l.country_name
                END as country_name,
                CASE
                    WHEN l.alpha2_code is null THEN 'unspecified'
                    ELSE l.alpha2_code
                END as country_code,
                FALSE as is_person,
                COALESCE(f.is_trade_buyer OR f.is_trade_supplier, FALSE) as is_trade,
                COALESCE(f.is_sanction, FALSE) as is_sanction,
                FROM_UTC_TIMESTAMP(NOW(), 'UTC') as created_at,
                FROM_UTC_TIMESTAMP(NOW(), 'UTC') as updated_at
            FROM sat_company c
            LEFT JOIN aggregated_flags f
                ON c.jurisdiction = f.jurisdiction
                AND c.dv_hashkey_company = f.to_key
            LEFT JOIN lookup_jurisdiction AS l
                ON c.jurisdiction = l.jurisdiction
        """
        )
        return result

    # def execute(self, n_group=8):
    #     sat_company = self.input_dataframe_dict[
    #         "silver.bridge_company_demomgraphic"
    #     ].dataframe
    #     groups = get_n_group_jurisdiction(sat_company, n_group)
    #     for group in groups:
    #         df = self.transform(group["jurisdictions"])
    #         print(
    #             "group_number:", group["group_number"], "total_cnt:", group["total_cnt"]
    #         )
    #         if df.count() > 0:
    #             print(
    #                 f"Write to {self.meta_table_model.table_name} - {self.meta_table_model.data_location}"
    #             )
    #         delta_insert_for_hubnlink(
    #             spark=self.spark,
    #             df=df,
    #             data_location=self.meta_table_model.data_location,
    #             base_cols=self.meta_table_model.unique_key,
    #             struct_type=self.meta_table_model.struct_type,
    #             partition_by=self.meta_table_model.partition_by,
    #         )


def run(env="pro", payload={}, spark=None):
    import sys

    table_meta_hub = TableMeta(
        from_files=["metas/gold/trade_service/entity.yaml"], payload=payload
    )
    executer_hub = EntityExecuter(
        sys.argv[0],
        table_meta_hub.model,
        table_meta_hub.input_resources,
        spark=spark,
    )
    executer_hub.execute()


if __name__ == "__main__":
    run()
