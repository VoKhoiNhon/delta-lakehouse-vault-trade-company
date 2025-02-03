from pyspark.sql import functions as F
import pandas as pd
from libs.executers.gold_executer import GoldCompanyExecuter
from libs.meta import TableMeta
from libs.utils.delta_utils import delta_insert
from libs.utils.commons import run_with_juris_splited


class Executer(GoldCompanyExecuter):
    def transform(self):
        h_person = self.input_dataframe_dict["silver.h_person"].dataframe
        s_person_demographic = self.input_dataframe_dict[
            "silver.s_person_demographic"
        ].dataframe
        s_person_address = self.input_dataframe_dict[
            "silver.s_person_address"
        ].dataframe
        l_sanction_person = self.input_dataframe_dict[
            "silver.l_sanction_person"
        ].dataframe

        lookup_jurisdiction = self.spark.createDataFrame(
            pd.read_csv(
                "resources/lookup_jurisdiction_with_continents.csv", na_filter=False
            )
        )

        is_sanctioned_df = l_sanction_person.withColumn(
            "is_sanctioned", F.lit(True)
        ).select("dv_hashkey_person", "is_sanctioned")

        person_address_df = s_person_address.groupBy(
            "dv_hashkey_person", "jurisdiction"
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

        person_detail_df = s_person_demographic.groupBy(
            "dv_hashkey_person", "jurisdiction"
        ).agg(
            F.struct(
                F.first("first_name").alias("first_name"),
                F.first("last_name").alias("last_name"),
                F.first("middle_name").alias("middle_name"),
                F.first(F.col("name")).alias("full_name"),
                F.first("image_url").alias("image_url"),
                F.first("dob").alias("dob"),
                F.first("birthplace").alias("birthplace"),
                F.first("nationality").alias("nationality"),
                F.first("country_of_residence").alias("country_of_residence"),
                F.first("accuracy_level").alias("accuracy_level"),
                F.first("gender").alias("gender"),
                F.first("skills").alias("skills"),
                F.first("job_summary").alias("job_summary"),
                F.first("salary").alias("salary"),
                F.first("yoe").alias("yoe"),
                F.first("industry").alias("industry"),
            ).alias("person")
        )

        entity_person = (
            h_person.alias("h")
            .join(
                s_person_demographic.alias("s"),
                ["jurisdiction", "dv_hashkey_person"],
                "left",
            )
            .join(lookup_jurisdiction.alias("lookup"), ["jurisdiction"], "left")
            .join(person_address_df, ["dv_hashkey_person", "jurisdiction"], "left")
            .join(person_detail_df, ["dv_hashkey_person", "jurisdiction"], "left")
            .join(is_sanctioned_df, ["dv_hashkey_person"], "left")
        )

        entity_person = entity_person.select(
            F.col("dv_hashkey_person").alias("id"),
            "h.dv_recsrc",
            "h.dv_loaddts",
            "h.dv_source_version",
            F.lit(True).alias("is_person"),
            "is_sanctioned",
            "h.name",
            "lookup.country_code",
            "lookup.country_name",
            "lookup.continent",
            "h.jurisdiction",
            "phone_numbers",
            "emails",
            "linkedin_url",
            "twitter_url",
            "facebook_url",
            "addresses",
            "person",
        ).dropDuplicates(["id"])

        return entity_person


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(
        from_files=["metas/gold/company_service/entity_person.yaml"], env=env
    )
    executer_entity = Executer(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer_entity.execute()


if __name__ == "__main__":
    run()
