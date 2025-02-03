from pyspark.sql import functions as F
import pandas as pd

from libs.executers.gold_executer import GoldCompanyExecuter
from libs.meta import TableMeta
from libs.utils.base_transforms import update_hashkey_company
from libs.utils.delta_utils import delta_insert, delta_insert_for_hubnlink
from libs.utils.commons import run_with_juris_splited


class EntityExecuter(GoldCompanyExecuter):
    def execute_each_batch(self, h_company):
        # h_company = self.input_dataframe_dict["silver.h_company"].dataframe
        bridge_company_demomgraphic = self.input_dataframe_dict[
            "silver.bridge_company_demomgraphic"
        ].dataframe
        s_company_address = self.input_dataframe_dict[
            "silver.s_company_address"
        ].dataframe
        l_sanction_company = self.input_dataframe_dict[
            "silver.l_sanction_company"
        ].dataframe
        h_industry = self.input_dataframe_dict["silver.h_industry"].dataframe
        l_company_industry = self.input_dataframe_dict[
            "silver.l_company_industry"
        ].dataframe

        lookup_jurisdiction = self.spark.createDataFrame(
            pd.read_csv(
                "resources/lookup_jurisdiction_with_continents.csv", na_filter=False
            )
        )

        mapping_df = (
            self.input_dataframe_dict["silver.bv_bridge_company_key"]
            .dataframe.filter(F.col("from_key").isNotNull())
            .select("from_key", "to_key")
        )

        h_company = h_company.join(
            mapping_df,
            on=(h_company.dv_hashkey_company == mapping_df.from_key),
            how="left_anti",
        )

        s_company_address = update_hashkey_company(
            s_company_address, mapping_df, id_col="dv_hashkey_company"
        )
        l_sanction_company = update_hashkey_company(
            l_sanction_company, mapping_df, id_col="dv_hashkey_company"
        )
        l_company_industry = update_hashkey_company(
            l_company_industry, mapping_df, id_col="dv_hashkey_company"
        )

        company_address_df = s_company_address.groupBy(
            "dv_hashkey_company", "jurisdiction"
        ).agg(
            F.collect_list(
                F.struct(
                    F.col("type"),
                    F.lit(None).cast("timestamp").alias("start_date"),
                    F.lit(None).cast("timestamp").alias("end_date"),
                    F.col("full_address"),
                    F.col("street"),
                    F.col("city"),
                    F.col("region"),
                    F.col("state"),
                    F.col("province"),
                    F.col("country_code"),
                    F.col("postal_code"),
                    F.col("latitude"),
                    F.col("longitude"),
                )
            ).alias("addresses")
        )

        company_detail_df = bridge_company_demomgraphic.groupBy(
            "dv_hashkey_company", "jurisdiction"
        ).agg(
            F.struct(
                F.first("image_url").alias("image_url"),
                F.first("no_of_employees").alias("no_of_employees"),
                F.first("authorised_capital").cast("float").alias("authorised_capital"),
                F.first("paid_up_capital").cast("float").alias("paid_up_capital"),
                F.first("currency_code").alias("currency_code"),
                F.first("is_branch").alias("is_branch"),
            ).alias("company")
        )

        company_status_df = bridge_company_demomgraphic.groupBy(
            "dv_hashkey_company", "jurisdiction"
        ).agg(
            F.collect_list(
                F.struct(F.col("status"), F.col("status_code"), F.col("status_desc"))
            ).alias("statuses")
        )

        industry_df = h_industry.join(
            l_company_industry, ["dv_hashkey_industry"], "left"
        )

        industry_df = industry_df.groupBy("dv_hashkey_company", "jurisdiction").agg(
            F.collect_list(
                F.struct(
                    F.col("type"),
                    F.col("industry_code"),
                    F.col("desc"),
                    F.col("country_code"),
                    F.col("standard_type"),
                )
            ).alias("industries")
        )

        is_sanctioned_df = l_sanction_company.withColumn(
            "is_sanctioned", F.lit(True)
        ).select("dv_hashkey_company", "is_sanctioned")

        entity_company = (
            h_company.alias("h")
            .join(
                bridge_company_demomgraphic.alias("s"),
                ["jurisdiction", "dv_hashkey_company"],
                "left",
            )
            .join(
                F.broadcast(lookup_jurisdiction).alias("lookup"),
                ["jurisdiction"],
                "left",
            )
            .join(company_address_df, ["dv_hashkey_company", "jurisdiction"], "left")
            .join(company_status_df, ["dv_hashkey_company", "jurisdiction"], "left")
            .join(company_detail_df, ["dv_hashkey_company", "jurisdiction"], "left")
            .join(industry_df, ["dv_hashkey_company", "jurisdiction"], "left")
            .join(is_sanctioned_df, ["dv_hashkey_company"], "left")
        )

        entity_company = entity_company.select(
            F.col("dv_hashkey_company").alias("id"),
            "h.dv_recsrc",
            "h.dv_loaddts",
            "h.dv_source_version",
            F.lit(False).alias("is_person"),
            "is_sanctioned",
            "h.name",
            "description",
            "lei_code",
            "lookup.country_code",
            "lookup.country_name",
            "lookup.continent",
            "h.registration_number",
            "date_incorporated",
            "date_struck_off",
            "h.jurisdiction",
            "legal_form",
            "category",
            "phone_numbers",
            "emails",
            "websites",
            "linkedin_url",
            "twitter_url",
            "facebook_url",
            "fax_numbers",
            "other_names",
            "addresses",
            "statuses",
            "industries",
            "company",
        ).dropDuplicates(["id"])

        delta_insert_for_hubnlink(
            spark=self.spark,
            df=entity_company,
            data_location=self.meta_table_model.data_location,
            base_cols=self.meta_table_model.unique_key,
            struct_type=entity_company.schema,
            partition_by=self.meta_table_model.partition_by,
        )

    def execute(self, n_group):
        h_company = self.input_dataframe_dict["silver.h_company"].dataframe
        bridge_company_demomgraphic = self.input_dataframe_dict[
            "silver.bridge_company_demomgraphic"
        ].dataframe
        if self.query:
            h_company = h_company.where(self.query)
            print(
                f"filter {self.query} \n"
                f"h_company.count(): {h_company.count():,} | bridge_company.count(): {bridge_company_demomgraphic.count():,}"
            )

        if n_group > 1:
            run_with_juris_splited(h_company, n_group, self.execute_each_batch)
        else:
            self.execute_each_batch(h_company)


def run(spark=None, n_group=2, payload={}):
    import sys

    table_meta = TableMeta(
        from_files=["metas/gold/company_service/entity_company.yaml"],
        payload=payload,
    )
    executer_entity = EntityExecuter(
        app_name=sys.argv[0],
        meta_table_model=table_meta.model,
        meta_input_resource=table_meta.input_resources,
        params=payload,
        spark=spark,
    )
    executer_entity.execute(n_group)


if __name__ == "__main__":
    run()
