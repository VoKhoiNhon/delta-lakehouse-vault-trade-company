from libs.executers.raw_vault_executer import SatelliteVaultExecuter
from libs.meta import TableMeta
import pyspark.sql.functions as F
from libs.utils.vault_hashfuncs import DV_HASHKEY_PERSON


class SPersonKentuckyExecuterDemographic(SatelliteVaultExecuter):
    def transform(self):
        # person = self.input_dataframe_dict["1tm_2412.person"].dataframe.filter(
        #     F.col("name").isNotNull()
        # )
        # entity_gov = self.input_dataframe_dict["1tm_2412.entity_gov"]
        # df = person.alias("e").join(
        #     entity_gov.where("is_person = true").select("id").alias("gov"),
        #     [F.col("e.id") == F.col("gov.id")],
        #     "left",
        # )
        df = self.input_dataframe_dict["1tm_2412.person"].dataframe.filter(
            F.col("name").isNotNull()
        )
        df = df.selectExpr(
            f"{DV_HASHKEY_PERSON} as dv_hashkey_person",
            "concat_ws(';', data_source, data_source_link) as dv_recsrc",
            "FROM_UTC_TIMESTAMP(NOW(), 'UTC') as dv_loaddts",
            "'1tm' as dv_source_version",
            "jurisdiction",
            "name",
            "first_name",
            "last_name",
            "middle_name",
            "image_url",
            "dob",
            "birthplace",
            "nationality",
            "country_of_residence",
            "accuracy_level",
            "gender",
            "skills",
            "job_summary",
            "salary",
            "yoe",
            "industry",
            "phone_numbers",
            "emails",
            "linkedin_url",
            "twitter_url",
            "facebook_url",
        )
        return df


def run(env="pro", params={}, spark=None):
    import sys

    table_meta = TableMeta(from_files="metas/silver/s_person_demographic.yaml", env=env)
    executer = SPersonKentuckyExecuterDemographic(
        sys.argv[0],
        table_meta.model,
        table_meta.input_resources,
        params,
        spark,
    )
    executer.execute()


if __name__ == "__main__":

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("pytest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    run(env="test", params={"dv_source_version": "init"}, spark=spark)
